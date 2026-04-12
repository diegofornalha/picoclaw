//go:build whatsapp_native

// PicoClaw - Ultra-lightweight personal AI agent
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package whatsapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const (
	sqliteDriver   = "sqlite"
	whatsappDBName = "store.db"

	reconnectInitial    = 5 * time.Second
	reconnectMax        = 5 * time.Minute
	reconnectMultiplier = 2.0
)

// WhatsAppNativeChannel implements the WhatsApp channel using whatsmeow (in-process, no external bridge).
type WhatsAppNativeChannel struct {
	*channels.BaseChannel
	config       config.WhatsAppConfig
	storePath    string
	client       *whatsmeow.Client
	container    *sqlstore.Container
	mu           sync.Mutex
	runCtx       context.Context
	runCancel    context.CancelFunc
	reconnectMu  sync.Mutex
	reconnecting bool
	stopping     atomic.Bool    // set once Stop begins; prevents new wg.Add calls
	loggedOut    atomic.Bool    // set when session is permanently invalid (LoggedOut, StreamReplaced, etc.)
	wg           sync.WaitGroup // tracks background goroutines (QR handler, reconnect)

	// Presence tracking
	presenceMu   sync.RWMutex
	presenceMap  map[string]*PresenceInfo // JID string → presence info

	// QRCallback is an optional hook called when QR login events occur.
	// Used by the WhatsApp pool to intercept QR codes for the web UI.
	// Parameters: event ("code", "timeout", etc.) and code (QR data, only for "code" event).
	QRCallback func(event, code string)

	// OnDisconnect is an optional hook called when the WhatsApp connection drops.
	// Used by the WhatsApp pool to re-queue the slot for pairing.
	OnDisconnect func()

	// IgnoreNumbers is a set of phone numbers (without @s.whatsapp.net) to ignore.
	// Used by the pool to prevent bot numbers from talking to each other.
	IgnoreNumbers map[string]bool
}

// NewWhatsAppNativeChannel creates a WhatsApp channel that uses whatsmeow for connection.
// name is the channel identifier (e.g. "whatsapp_native" or "whatsapp_1" for pool mode).
// storePath is the directory for the SQLite session store (e.g. workspace/whatsapp).
func NewWhatsAppNativeChannel(
	name string,
	cfg config.WhatsAppConfig,
	bus *bus.MessageBus,
	storePath string,
) (channels.Channel, error) {
	base := channels.NewBaseChannel(name, cfg, bus, cfg.AllowFrom, channels.WithMaxMessageLength(65536))
	if storePath == "" {
		storePath = "whatsapp"
	}
	c := &WhatsAppNativeChannel{
		BaseChannel: base,
		config:      cfg,
		storePath:   storePath,
	}
	return c, nil
}

func (c *WhatsAppNativeChannel) Start(ctx context.Context) error {
	logger.InfoCF("whatsapp", "Starting WhatsApp native channel (whatsmeow)", map[string]any{"store": c.storePath})

	// Reset lifecycle state from any previous Stop() so a restarted channel
	// behaves correctly.  Use reconnectMu to be consistent with eventHandler
	// and Stop() which coordinate under the same lock.
	c.reconnectMu.Lock()
	c.stopping.Store(false)
	c.loggedOut.Store(false)
	c.reconnecting = false
	c.reconnectMu.Unlock()

	if err := os.MkdirAll(c.storePath, 0o700); err != nil {
		return fmt.Errorf("create session store dir: %w", err)
	}

	dbPath := filepath.Join(c.storePath, whatsappDBName)
	connStr := "file:" + dbPath + "?_foreign_keys=on"

	db, err := sql.Open(sqliteDriver, connStr)
	if err != nil {
		return fmt.Errorf("open whatsapp store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err = db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		return fmt.Errorf("enable foreign keys: %w", err)
	}

	waLogger := waLog.Stdout("WhatsApp", "WARN", true)
	container := sqlstore.NewWithDB(db, sqliteDriver, waLogger)
	if err = container.Upgrade(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("open whatsapp store: %w", err)
	}

	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		_ = container.Close()
		return fmt.Errorf("get device store: %w", err)
	}

	client := whatsmeow.NewClient(deviceStore, waLogger)

	// Create runCtx/runCancel BEFORE registering event handler and starting
	// goroutines so that Stop() can cancel them at any time, including during
	// the QR-login flow.
	c.runCtx, c.runCancel = context.WithCancel(ctx)

	client.AddEventHandler(c.eventHandler)

	c.mu.Lock()
	c.container = container
	c.client = client
	c.mu.Unlock()

	// cleanupOnError clears struct references and releases resources when
	// Start() fails after fields are already assigned.  This prevents
	// Stop() from operating on stale references (double-close, disconnect
	// of a partially-initialized client, or stray event handler callbacks).
	startOK := false
	defer func() {
		if startOK {
			return
		}
		c.runCancel()
		client.Disconnect()
		c.mu.Lock()
		c.client = nil
		c.container = nil
		c.mu.Unlock()
		_ = container.Close()
	}()

	if client.Store.ID == nil {
		qrChan, err := client.GetQRChannel(c.runCtx)
		if err != nil {
			return fmt.Errorf("get QR channel: %w", err)
		}
		if err := client.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		// Handle QR events in a background goroutine so Start() returns
		// promptly.  The goroutine is tracked via c.wg and respects
		// c.runCtx for cancellation.
		// Guard wg.Add with reconnectMu + stopping check (same protocol
		// as eventHandler) so a concurrent Stop() cannot enter wg.Wait()
		// while we call wg.Add(1).
		c.reconnectMu.Lock()
		if c.stopping.Load() {
			c.reconnectMu.Unlock()
			return fmt.Errorf("channel stopped during QR setup")
		}
		c.wg.Add(1)
		c.reconnectMu.Unlock()
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-c.runCtx.Done():
					return
				case evt, ok := <-qrChan:
					if !ok {
						return
					}
					if evt.Event == "code" {
						if c.QRCallback != nil {
							c.QRCallback("code", evt.Code)
						}
						logger.InfoCF("whatsapp", "Scan this QR code with WhatsApp (Linked Devices):", nil)
						qrterminal.GenerateWithConfig(evt.Code, qrterminal.Config{
							Level:      qrterminal.L,
							Writer:     os.Stdout,
							HalfBlocks: true,
						})
					} else {
						if c.QRCallback != nil {
							c.QRCallback(evt.Event, "")
						}
						logger.InfoCF("whatsapp", "WhatsApp login event", map[string]any{"event": evt.Event})
					}
				}
			}
		}()
	} else {
		if err := client.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
	}

	startOK = true
	c.SetRunning(true)
	logger.InfoC("whatsapp", "WhatsApp native channel connected")
	return nil
}

func (c *WhatsAppNativeChannel) Stop(ctx context.Context) error {
	logger.InfoC("whatsapp", "Stopping WhatsApp native channel")

	// Mark as stopping under reconnectMu so the flag is visible to
	// eventHandler atomically with respect to its wg.Add(1) call.
	// This closes the TOCTOU window where eventHandler could check
	// stopping (false), then Stop sets it true + enters wg.Wait,
	// then eventHandler calls wg.Add(1) — causing a panic.
	c.reconnectMu.Lock()
	c.stopping.Store(true)
	c.reconnectMu.Unlock()

	if c.runCancel != nil {
		c.runCancel()
	}

	// Disconnect the client first so any blocking Connect()/reconnect loops
	// can be interrupted before we wait on the goroutines.
	c.mu.Lock()
	client := c.client
	container := c.container
	c.mu.Unlock()

	if client != nil {
		client.Disconnect()
	}

	// Wait for background goroutines (QR handler, reconnect) to finish in a
	// context-aware way so Stop can be bounded by ctx.
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines have finished.
	case <-ctx.Done():
		// Context canceled or timed out; log and proceed with best-effort cleanup.
		logger.WarnC("whatsapp", fmt.Sprintf("Stop context canceled before all goroutines finished: %v", ctx.Err()))
	}

	// Now it is safe to clear and close resources.
	c.mu.Lock()
	c.client = nil
	c.container = nil
	c.mu.Unlock()

	if container != nil {
		_ = container.Close()
	}
	c.SetRunning(false)
	return nil
}

func (c *WhatsAppNativeChannel) eventHandler(evt any) {
	switch v := evt.(type) {
	case *events.Message:
		c.handleIncoming(v)
	case *events.CallOffer:
		c.mu.Lock()
		client := c.client
		c.mu.Unlock()
		if client != nil {
			_ = client.RejectCall(context.Background(), v.BasicCallMeta.From, v.BasicCallMeta.CallID)
			logger.InfoCF("whatsapp", "Call rejected automatically", map[string]any{"from": v.BasicCallMeta.From.String()})
		}
	case *events.LoggedOut:
		c.loggedOut.Store(true)
		logger.ErrorCF("whatsapp", "WhatsApp session logged out — re-pairing required", map[string]any{
			"on_connect": v.OnConnect,
			"reason":     v.Reason.String(),
		})
	case *events.StreamReplaced:
		c.loggedOut.Store(true)
		logger.ErrorCF("whatsapp", "WhatsApp stream replaced by another session — re-pairing required", nil)
	case *events.KeepAliveTimeout:
		logger.WarnCF("whatsapp", "WhatsApp keep-alive timeout", map[string]any{
			"error_count":  v.ErrorCount,
			"last_success": v.LastSuccess.Format(time.RFC3339),
		})
	case *events.ConnectFailure:
		logger.ErrorCF("whatsapp", "WhatsApp connect failure", map[string]any{
			"reason":  v.Reason.String(),
			"message": v.Message,
		})
	case *events.TemporaryBan:
		c.loggedOut.Store(true)
		logger.ErrorCF("whatsapp", "WhatsApp temporary ban — stopping reconnect", map[string]any{
			"code":   v.Code.String(),
			"expire": v.Expire.String(),
		})
	case *events.ClientOutdated:
		c.loggedOut.Store(true)
		logger.ErrorCF("whatsapp", "WhatsApp client outdated — update whatsmeow required", nil)
	case *events.Presence:
		c.handlePresenceEvent(v)
	case *events.Disconnected:
		if c.OnDisconnect != nil {
			c.OnDisconnect()
		}
		logger.InfoCF("whatsapp", "WhatsApp disconnected, will attempt reconnection", nil)
		c.reconnectMu.Lock()
		if c.reconnecting {
			c.reconnectMu.Unlock()
			return
		}
		// Check stopping while holding the lock so the check and wg.Add
		// are atomic with respect to Stop() setting the flag + calling
		// wg.Wait(). This prevents the TOCTOU race.
		if c.stopping.Load() {
			c.reconnectMu.Unlock()
			return
		}
		c.reconnecting = true
		c.wg.Add(1)
		c.reconnectMu.Unlock()
		go func() {
			defer c.wg.Done()
			c.reconnectWithBackoff()
		}()
	}
}

func (c *WhatsAppNativeChannel) reconnectWithBackoff() {
	defer func() {
		c.reconnectMu.Lock()
		c.reconnecting = false
		c.reconnectMu.Unlock()
	}()

	backoff := reconnectInitial
	for {
		if c.loggedOut.Load() {
			logger.ErrorCF("whatsapp", "Aborting reconnect — session invalid, re-pairing required", nil)
			return
		}

		select {
		case <-c.runCtx.Done():
			return
		default:
		}

		c.mu.Lock()
		client := c.client
		c.mu.Unlock()
		if client == nil {
			return
		}

		logger.InfoCF("whatsapp", "WhatsApp reconnecting", map[string]any{"backoff": backoff.String()})
		err := client.Connect()
		if err == nil {
			logger.InfoC("whatsapp", "WhatsApp reconnected")
			return
		}

		logger.WarnCF("whatsapp", "WhatsApp reconnect failed", map[string]any{"error": err.Error()})

		select {
		case <-c.runCtx.Done():
			return
		case <-time.After(backoff):
			if backoff < reconnectMax {
				next := time.Duration(float64(backoff) * reconnectMultiplier)
				if next > reconnectMax {
					next = reconnectMax
				}
				backoff = next
			}
		}
	}
}

func (c *WhatsAppNativeChannel) handleIncoming(evt *events.Message) {
	if evt.Message == nil {
		return
	}

	// Ignore messages from self (own device)
	if evt.Info.IsFromMe {
		return
	}

	senderID := evt.Info.Sender.String()
	chatID := evt.Info.Chat.String()

	// Ignore group messages entirely — bot should be silent in groups
	if evt.Info.Chat.Server == types.GroupServer {
		return
	}

	// Ignore messages from other bot numbers in the pool
	if len(c.IgnoreNumbers) > 0 {
		senderPhone := evt.Info.Sender.User
		if c.IgnoreNumbers[senderPhone] {
			return
		}
	}
	content := evt.Message.GetConversation()
	if content == "" && evt.Message.ExtendedTextMessage != nil {
		content = evt.Message.ExtendedTextMessage.GetText()
	}
	content = utils.SanitizeMessageContent(content)

	// Detect media messages that have no text content
	if content == "" {
		switch {
		case evt.Message.StickerMessage != nil:
			// Try to download and save the sticker
			c.mu.Lock()
			client := c.client
			c.mu.Unlock()
			if client != nil {
				if data, err := client.Download(c.runCtx, evt.Message.StickerMessage); err == nil {
					stickerDir := filepath.Join(filepath.Dir(c.storePath), "media", "stickers")
					_ = os.MkdirAll(stickerDir, 0755)
					stickerPath := filepath.Join(stickerDir, evt.Info.ID+".webp")
					if err := os.WriteFile(stickerPath, data, 0644); err == nil {
						content = "[figurinha salva: " + stickerPath + "] Perguntar ao usuario qual nome dar para a figurinha."
					} else {
						content = "[sticker recebido, erro ao salvar]"
					}
				} else {
					content = "[sticker recebido, erro ao baixar]"
				}
			} else {
				content = "[sticker recebido]"
			}
		case evt.Message.ImageMessage != nil:
			caption := evt.Message.ImageMessage.GetCaption()
			c.mu.Lock()
			cl := c.client
			c.mu.Unlock()
			if cl != nil {
				if data, err := cl.Download(c.runCtx, evt.Message.ImageMessage); err == nil {
					mediaDir := filepath.Join(filepath.Dir(c.storePath), "media", "images")
					_ = os.MkdirAll(mediaDir, 0755)
					ext := ".jpg"
					if mime := evt.Message.ImageMessage.GetMimetype(); strings.Contains(mime, "png") {
						ext = ".png"
					}
					mediaPath := filepath.Join(mediaDir, evt.Info.ID+ext)
					if err := os.WriteFile(mediaPath, data, 0644); err == nil {
						if caption != "" {
							content = "[imagem salva: " + mediaPath + "] " + caption
						} else {
							content = "[imagem salva: " + mediaPath + "]"
						}
					}
				}
			}
			if content == "" {
				if caption != "" {
					content = "[imagem] " + caption
				} else {
					content = "[imagem recebida]"
				}
			}
		case evt.Message.AudioMessage != nil:
			c.mu.Lock()
			cl := c.client
			c.mu.Unlock()
			if cl != nil {
				if data, err := cl.Download(c.runCtx, evt.Message.AudioMessage); err == nil {
					mediaDir := filepath.Join(filepath.Dir(c.storePath), "media", "audio")
					_ = os.MkdirAll(mediaDir, 0755)
					ext := ".ogg"
					if evt.Message.AudioMessage.GetPTT() {
						ext = ".ogg"
					}
					mediaPath := filepath.Join(mediaDir, evt.Info.ID+ext)
					if err := os.WriteFile(mediaPath, data, 0644); err == nil {
						if evt.Message.AudioMessage.GetPTT() {
							content = "[audio de voz salvo: " + mediaPath + "]"
						} else {
							content = "[audio salvo: " + mediaPath + "]"
						}
					}
				}
			}
			if content == "" {
				if evt.Message.AudioMessage.GetPTT() {
					content = "[audio de voz recebido]"
				} else {
					content = "[audio recebido]"
				}
			}
		case evt.Message.VideoMessage != nil:
			caption := evt.Message.VideoMessage.GetCaption()
			c.mu.Lock()
			cl := c.client
			c.mu.Unlock()
			if cl != nil {
				if data, err := cl.Download(c.runCtx, evt.Message.VideoMessage); err == nil {
					mediaDir := filepath.Join(filepath.Dir(c.storePath), "media", "videos")
					_ = os.MkdirAll(mediaDir, 0755)
					mediaPath := filepath.Join(mediaDir, evt.Info.ID+".mp4")
					if err := os.WriteFile(mediaPath, data, 0644); err == nil {
						if caption != "" {
							content = "[video salvo: " + mediaPath + "] " + caption
						} else {
							content = "[video salvo: " + mediaPath + "]"
						}
					}
				}
			}
			if content == "" {
				if caption != "" {
					content = "[video] " + caption
				} else {
					content = "[video recebido]"
				}
			}
		case evt.Message.DocumentMessage != nil:
			filename := evt.Message.DocumentMessage.GetFileName()
			c.mu.Lock()
			cl := c.client
			c.mu.Unlock()
			if cl != nil {
				if data, err := cl.Download(c.runCtx, evt.Message.DocumentMessage); err == nil {
					mediaDir := filepath.Join(filepath.Dir(c.storePath), "media", "docs")
					_ = os.MkdirAll(mediaDir, 0755)
					saveName := evt.Info.ID
					if filename != "" {
						saveName = filename
					}
					mediaPath := filepath.Join(mediaDir, saveName)
					if err := os.WriteFile(mediaPath, data, 0644); err == nil {
						content = "[documento salvo: " + mediaPath + "]"
					}
				}
			}
			if content == "" {
				if filename != "" {
					content = "[documento] " + filename
				} else {
					content = "[documento recebido]"
				}
			}
		case evt.Message.ContactMessage != nil:
			name := evt.Message.ContactMessage.GetDisplayName()
			content = "[contato] " + name
		case evt.Message.LocationMessage != nil:
			content = "[localizacao recebida]"
		case evt.Message.ReactionMessage != nil:
			content = "[reacao] " + evt.Message.ReactionMessage.GetText()
		}
	}

	if content == "" {
		return
	}

	var mediaPaths []string

	metadata := make(map[string]string)
	metadata["message_id"] = evt.Info.ID
	if evt.Info.PushName != "" {
		metadata["user_name"] = evt.Info.PushName
	}

	// Resolve LID to phone number if available
	if evt.Info.Sender.Server == types.HiddenUserServer {
		metadata["lid"] = evt.Info.Sender.User
		c.mu.Lock()
		client := c.client
		c.mu.Unlock()
		if client != nil && client.Store.LIDs != nil {
			if pnJID, err := client.Store.LIDs.GetPNForLID(c.runCtx, evt.Info.Sender); err == nil && !pnJID.IsEmpty() {
				metadata["phone_number"] = "+" + pnJID.User
				senderID = pnJID.String()
				logger.DebugCF("whatsapp", "LID resolved", map[string]any{"lid": evt.Info.Sender.User, "phone": "+" + pnJID.User})
			} else {
				logger.WarnCF("whatsapp", "LID not found in DB — unknown sender", map[string]any{"lid": evt.Info.Sender.User})
			}
		}
	}
	if evt.Info.Chat.Server == types.GroupServer {
		// Skip group messages when mention_only is enabled — prevents
		// data leaks (magic links, subscriber info) in public groups.
		if c.config.GroupTrigger.MentionOnly {
			logger.DebugCF("whatsapp", "ignoring group message (mention_only)", map[string]any{"chat": chatID})
			return
		}
		metadata["peer_kind"] = "group"
		metadata["peer_id"] = chatID
	} else {
		metadata["peer_kind"] = "direct"
		metadata["peer_id"] = senderID
	}

	peerKind := "direct"
	if evt.Info.Chat.Server == types.GroupServer {
		peerKind = "group"
	}
	peer := bus.Peer{Kind: peerKind, ID: chatID}
	messageID := evt.Info.ID
	sender := bus.SenderInfo{
		Platform:    "whatsapp",
		PlatformID:  senderID,
		CanonicalID: identity.BuildCanonicalID("whatsapp", senderID),
		DisplayName: evt.Info.PushName,
	}

	if !c.IsAllowedSender(sender) {
		return
	}

	logger.DebugCF(
		"whatsapp",
		"WhatsApp message received",
		map[string]any{"sender_id": senderID, "content_preview": utils.Truncate(content, 50)},
	)

	// Send read receipt (blue ticks)
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client != nil {
		senderJID := evt.Info.Sender
		if evt.Info.Chat.Server != types.GroupServer {
			senderJID = types.EmptyJID
		}
		_ = client.MarkRead(c.runCtx, []types.MessageID{evt.Info.ID}, time.Now(), evt.Info.Chat, senderJID)
	}

	c.HandleMessage(c.runCtx, peer, messageID, senderID, chatID, content, mediaPaths, metadata, sender)
}

func (c *WhatsAppNativeChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return nil, fmt.Errorf("whatsapp connection not established: %w", channels.ErrTemporary)
	}

	// Detect unpaired state: the client is connected (to WhatsApp servers)
	// but has not completed QR-login yet, so sending would fail.
	if client.Store.ID == nil {
		return nil, fmt.Errorf("whatsapp not yet paired (QR login pending): %w", channels.ErrTemporary)
	}

	to, err := parseJID(msg.ChatID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat id %q: %w", msg.ChatID, err)
	}

	var waMsg *waE2E.Message
	if url := extractURL(msg.Content); url != "" {
		waMsg = &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text:        proto.String(msg.Content),
				MatchedText: proto.String(url),
			},
		}
	} else {
		waMsg = &waE2E.Message{Conversation: proto.String(msg.Content)}
	}

	// Quote original message if reply_to is set
	if msg.ReplyToMessageID != "" {
		ctxInfo := &waE2E.ContextInfo{
			StanzaID:    proto.String(msg.ReplyToMessageID),
			Participant: proto.String(msg.ChatID),
		}
		if waMsg.ExtendedTextMessage != nil {
			waMsg.ExtendedTextMessage.ContextInfo = ctxInfo
		} else {
			// Convert Conversation to ExtendedTextMessage to support ContextInfo
			waMsg = &waE2E.Message{
				ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text:        proto.String(msg.Content),
					ContextInfo: ctxInfo,
				},
			}
		}
	}

	if _, err = client.SendMessage(ctx, to, waMsg); err != nil {
		return nil, fmt.Errorf("whatsapp send: %w", channels.ErrTemporary)
	}
	return nil, nil
}

// SendStatus sends a text message to the WhatsApp Status broadcast (status@broadcast).
func (c *WhatsAppNativeChannel) SendStatus(ctx context.Context, text string) error {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil || !client.IsConnected() {
		return fmt.Errorf("whatsapp not connected")
	}
	if client.Store.ID == nil {
		return fmt.Errorf("whatsapp not paired")
	}
	black := uint32(0xFF000000)
	white := uint32(0xFFFFFFFF)
	font := waE2E.ExtendedTextMessage_SYSTEM_BOLD
	msg := &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:           proto.String(text),
			BackgroundArgb: &black,
			TextArgb:       &white,
			Font:           &font,
		},
	}
	_, err := client.SendMessage(ctx, types.StatusBroadcastJID, msg)
	return err
}

// SendMedia implements the channels.MediaSender interface.
func (c *WhatsAppNativeChannel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return nil, fmt.Errorf("whatsapp connection not established: %w", channels.ErrTemporary)
	}
	if client.Store.ID == nil {
		return nil, fmt.Errorf("whatsapp not yet paired (QR login pending): %w", channels.ErrTemporary)
	}

	to, err := parseJID(msg.ChatID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat id %q: %w", msg.ChatID, channels.ErrSendFailed)
	}

	store := c.GetMediaStore()
	if store == nil {
		return nil, fmt.Errorf("no media store available: %w", channels.ErrSendFailed)
	}

	for _, part := range msg.Parts {
		if err := c.sendMediaPart(ctx, client, to, store, part); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// sendMediaPart uploads and sends a single media part via whatsmeow.
func (c *WhatsAppNativeChannel) sendMediaPart(
	ctx context.Context,
	client *whatsmeow.Client,
	to types.JID,
	store media.MediaStore,
	part bus.MediaPart,
) error {
	localPath, err := store.Resolve(part.Ref)
	if err != nil {
		logger.ErrorCF("whatsapp", "Failed to resolve media ref", map[string]any{
			"ref":   part.Ref,
			"error": err.Error(),
		})
		return fmt.Errorf("whatsapp resolve media ref %q: %w", part.Ref, channels.ErrSendFailed)
	}

	data, err := os.ReadFile(localPath)
	if err != nil {
		logger.ErrorCF("whatsapp", "Failed to read media file", map[string]any{
			"path":  localPath,
			"error": err.Error(),
		})
		return fmt.Errorf("whatsapp read media: %w", channels.ErrSendFailed)
	}

	mimeType := part.ContentType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	var waMsg *waE2E.Message

	switch part.Type {
	case "image":
		resp, err := client.Upload(ctx, data, whatsmeow.MediaImage)
		if err != nil {
			return fmt.Errorf("whatsapp upload image: %w", channels.ErrTemporary)
		}
		waMsg = &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(part.Caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}

	case "video":
		resp, err := client.Upload(ctx, data, whatsmeow.MediaVideo)
		if err != nil {
			return fmt.Errorf("whatsapp upload video: %w", channels.ErrTemporary)
		}
		waMsg = &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				Caption:       proto.String(part.Caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}

	case "audio":
		resp, err := client.Upload(ctx, data, whatsmeow.MediaAudio)
		if err != nil {
			return fmt.Errorf("whatsapp upload audio: %w", channels.ErrTemporary)
		}
		// Detect voice messages (PTT) by filename convention
		fn := strings.ToLower(part.Filename)
		isPTT := strings.Contains(fn, "voice") || strings.Contains(fn, "ptt")
		waMsg = &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype:      proto.String(mimeType),
				PTT:           proto.Bool(isPTT),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}

	default: // "file" or unknown → send as document
		resp, err := client.Upload(ctx, data, whatsmeow.MediaDocument)
		if err != nil {
			return fmt.Errorf("whatsapp upload document: %w", channels.ErrTemporary)
		}
		filename := part.Filename
		if filename == "" {
			filename = filepath.Base(localPath)
		}
		waMsg = &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				Caption:       proto.String(part.Caption),
				Title:         proto.String(filename),
				FileName:      proto.String(filename),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}
	}

	if _, err := client.SendMessage(ctx, to, waMsg); err != nil {
		logger.ErrorCF("whatsapp", "Failed to send media", map[string]any{
			"type":  part.Type,
			"error": err.Error(),
		})
		return fmt.Errorf("whatsapp send media: %w", channels.ErrTemporary)
	}

	logger.DebugCF("whatsapp", "Media sent", map[string]any{
		"type": part.Type,
		"to":   to.String(),
	})
	return nil
}

// extractURL returns the first URL found in text, or empty string.
var urlPattern = regexp.MustCompile(`https?://\S+`)

func extractURL(text string) string {
	return urlPattern.FindString(text)
}

// StartTyping sends a "composing" chat presence to the given chat.
// It repeats every 4 seconds (WhatsApp indicator expires after ~5s).
// The returned stop function sends a "paused" presence and is idempotent.
func (c *WhatsAppNativeChannel) StartTyping(ctx context.Context, chatID string) (func(), error) {
	jid, err := parseJID(chatID)
	if err != nil {
		return func() {}, fmt.Errorf("start typing: %w", err)
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return func() {}, nil
	}

	_ = client.SendChatPresence(ctx, jid, types.ChatPresenceComposing, types.ChatPresenceMediaText)

	typingCtx, typingCancel := context.WithCancel(ctx)
	var once sync.Once

	go func() {
		ticker := time.NewTicker(4 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-typingCtx.Done():
				return
			case <-ticker.C:
				c.mu.Lock()
				cl := c.client
				c.mu.Unlock()
				if cl != nil {
					_ = cl.SendChatPresence(typingCtx, jid, types.ChatPresenceComposing, types.ChatPresenceMediaText)
				}
			}
		}
	}()

	stop := func() {
		once.Do(func() {
			typingCancel()
			c.mu.Lock()
			cl := c.client
			c.mu.Unlock()
			if cl != nil {
				_ = cl.SendChatPresence(context.Background(), jid, types.ChatPresencePaused, "")
			}
		})
	}
	return stop, nil
}

// EditMessage edits a previously sent message.
func (c *WhatsAppNativeChannel) EditMessage(ctx context.Context, chatID, messageID, content string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return fmt.Errorf("edit message: %w", err)
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}

	edited := client.BuildEdit(jid, types.MessageID(messageID), &waE2E.Message{
		Conversation: proto.String(content),
	})
	_, err = client.SendMessage(ctx, jid, edited)
	return err
}

// DeleteMessage revokes (deletes for everyone) a previously sent message.
func (c *WhatsAppNativeChannel) DeleteMessage(ctx context.Context, chatID, messageID string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}

	revoke := client.BuildRevoke(jid, types.EmptyJID, types.MessageID(messageID))
	_, err = client.SendMessage(ctx, jid, revoke)
	return err
}

// ReactToMessage adds a ⏳ reaction to an inbound message.
// The returned undo function removes the reaction (idempotent).
func (c *WhatsAppNativeChannel) ReactToMessage(ctx context.Context, chatID, messageID string) (func(), error) {
	jid, err := parseJID(chatID)
	if err != nil {
		return func() {}, fmt.Errorf("react: %w", err)
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return func() {}, nil
	}

	reaction := client.BuildReaction(jid, types.EmptyJID, types.MessageID(messageID), "⏳")
	_, _ = client.SendMessage(ctx, jid, reaction)

	var once sync.Once
	undo := func() {
		once.Do(func() {
			c.mu.Lock()
			cl := c.client
			c.mu.Unlock()
			if cl != nil {
				unreact := cl.BuildReaction(jid, types.EmptyJID, types.MessageID(messageID), "")
				_, _ = cl.SendMessage(context.Background(), jid, unreact)
			}
		})
	}
	return undo, nil
}


// IsOnWhatsApp checks if a phone number is registered on WhatsApp.
func (c *WhatsAppNativeChannel) IsOnWhatsApp(ctx context.Context, phone string) (bool, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return false, fmt.Errorf("whatsapp not connected")
	}

	cleaned := "+" + strings.TrimLeft(phone, "+")
	resp, err := client.IsOnWhatsApp(ctx, []string{cleaned})
	if err != nil {
		return false, err
	}
	if len(resp) == 0 {
		return false, nil
	}
	return resp[0].IsIn, nil
}

// CreateNewsletter creates a WhatsApp Channel (newsletter) and returns its JID.
func (c *WhatsAppNativeChannel) CreateNewsletter(ctx context.Context, name, description string) (string, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return "", fmt.Errorf("whatsapp not connected")
	}

	meta, err := client.CreateNewsletter(ctx, whatsmeow.CreateNewsletterParams{
		Name:        name,
		Description: description,
	})
	if err != nil {
		return "", err
	}
	return meta.ID.String(), nil
}

// SendToNewsletter sends a message to a WhatsApp Channel (newsletter).
func (c *WhatsAppNativeChannel) SendToNewsletter(ctx context.Context, newsletterJID, content string) error {
	jid, err := types.ParseJID(newsletterJID)
	if err != nil {
		return fmt.Errorf("invalid newsletter JID: %w", err)
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}

	msg := &waE2E.Message{Conversation: proto.String(content)}
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// GroupInfo holds basic info about a WhatsApp group.
type GroupInfo struct {
	JID          string `json:"jid"`
	Name         string `json:"name"`
	Topic        string `json:"topic,omitempty"`
	Participants int    `json:"participants"`
}

// GetJoinedGroups returns all groups the bot is a member of.
func (c *WhatsAppNativeChannel) GetJoinedGroups(ctx context.Context) ([]GroupInfo, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("whatsapp not connected")
	}
	groups, err := client.GetJoinedGroups(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]GroupInfo, len(groups))
	for i, g := range groups {
		result[i] = GroupInfo{
			JID:          g.JID.String(),
			Name:         g.Name,
			Topic:        g.Topic,
			Participants: len(g.Participants),
		}
	}
	return result, nil
}

// SendSticker sends a WebP sticker to a contact or group.
func (c *WhatsAppNativeChannel) SendSticker(ctx context.Context, chatID string, webpData []byte) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	resp, err := client.Upload(ctx, webpData, whatsmeow.MediaImage)
	if err != nil {
		return fmt.Errorf("upload sticker: %w", err)
	}
	msg := &waE2E.Message{
		StickerMessage: &waE2E.StickerMessage{
			URL:           proto.String(resp.URL),
			DirectPath:    proto.String(resp.DirectPath),
			MediaKey:      resp.MediaKey,
			Mimetype:      proto.String("image/webp"),
			FileEncSHA256: resp.FileEncSHA256,
			FileSHA256:    resp.FileSHA256,
			FileLength:    proto.Uint64(resp.FileLength),
		},
	}
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// DownloadSticker downloads a received sticker and returns the WebP bytes.
func (c *WhatsAppNativeChannel) DownloadSticker(ctx context.Context, evt *events.Message) ([]byte, error) {
	if evt.Message.StickerMessage == nil {
		return nil, fmt.Errorf("not a sticker message")
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("whatsapp not connected")
	}
	return client.Download(ctx, evt.Message.StickerMessage)
}

// BlockContact blocks a contact on WhatsApp.
func (c *WhatsAppNativeChannel) BlockContact(ctx context.Context, chatID string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	_, err = client.UpdateBlocklist(ctx, jid, events.BlocklistChangeActionBlock)
	return err
}

// UnblockContact unblocks a contact on WhatsApp.
func (c *WhatsAppNativeChannel) UnblockContact(ctx context.Context, chatID string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	_, err = client.UpdateBlocklist(ctx, jid, events.BlocklistChangeActionUnblock)
	return err
}

// GetBlocklist returns the list of blocked contacts.
func (c *WhatsAppNativeChannel) GetBlocklist(ctx context.Context) ([]string, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("whatsapp not connected")
	}
	list, err := client.GetBlocklist(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(list.JIDs))
	for i, jid := range list.JIDs {
		result[i] = jid.String()
	}
	return result, nil
}

// SetBio updates the "about" status message of the bot's WhatsApp profile.
func (c *WhatsAppNativeChannel) SetBio(ctx context.Context, msg string) error {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	return client.SetStatusMessage(ctx, msg)
}

// GetSubGroups returns all subgroups of a community.
func (c *WhatsAppNativeChannel) GetSubGroups(ctx context.Context, communityJID string) ([]GroupInfo, error) {
	jid, err := parseJID(communityJID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("whatsapp not connected")
	}
	subs, err := client.GetSubGroups(ctx, jid)
	if err != nil {
		return nil, err
	}
	result := make([]GroupInfo, len(subs))
	for i, s := range subs {
		result[i] = GroupInfo{
			JID:  s.JID.String(),
			Name: s.GroupName.Name,
		}
	}
	return result, nil
}

// SendInteractive sends an interactive message with native flow buttons.
func (c *WhatsAppNativeChannel) SendInteractive(ctx context.Context, chatID string, title string, body string, footer string, buttons []map[string]string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	nfButtons := make([]*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton, len(buttons))
	for i, b := range buttons {
		nfButtons[i] = &waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
			Name:             proto.String(b["name"]),
			ButtonParamsJSON: proto.String(b["params"]),
		}
	}
	msg := &waE2E.Message{
		InteractiveMessage: &waE2E.InteractiveMessage{
			Header: &waE2E.InteractiveMessage_Header{
				Title:              proto.String(title),
				HasMediaAttachment: proto.Bool(false),
			},
			Body: &waE2E.InteractiveMessage_Body{
				Text: proto.String(body),
			},
			Footer: &waE2E.InteractiveMessage_Footer{
				Text: proto.String(footer),
			},
			InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
				NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
					Buttons:        nfButtons,
					MessageVersion: proto.Int32(1),
				},
			},
		},
	}
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// SendButtons sends a message with clickable buttons.
func (c *WhatsAppNativeChannel) SendButtons(ctx context.Context, chatID string, text string, footer string, buttons []map[string]string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	btnList := make([]*waE2E.ButtonsMessage_Button, len(buttons))
	for i, b := range buttons {
		btnList[i] = &waE2E.ButtonsMessage_Button{
			ButtonID: proto.String(b["id"]),
			ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{
				DisplayText: proto.String(b["text"]),
			},
			Type: waE2E.ButtonsMessage_Button_RESPONSE.Enum(),
		}
	}
	msg := &waE2E.Message{
		ButtonsMessage: &waE2E.ButtonsMessage{
			ContentText: proto.String(text),
			FooterText:  proto.String(footer),
			Buttons:     btnList,
			HeaderType:  waE2E.ButtonsMessage_EMPTY.Enum(),
		},
	}
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// SetDisappearingTimer sets the disappearing messages timer for a chat.
// Valid values: "off", "24h", "7d", "90d"
func (c *WhatsAppNativeChannel) SetDisappearingTimer(ctx context.Context, chatID string, timer string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	duration, ok := whatsmeow.ParseDisappearingTimerString(timer)
	if !ok {
		return fmt.Errorf("timer invalido: %s (use off, 24h, 7d ou 90d)", timer)
	}
	return client.SetDisappearingTimer(ctx, jid, duration, time.Now())
}

// SendImage sends an image to a contact or group. Supports view-once.
func (c *WhatsAppNativeChannel) SendImage(ctx context.Context, chatID string, imageData []byte, mimetype string, caption string, viewOnce bool) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	resp, err := client.Upload(ctx, imageData, whatsmeow.MediaImage)
	if err != nil {
		return fmt.Errorf("upload image: %w", err)
	}
	if mimetype == "" {
		mimetype = "image/jpeg"
	}
	imageMsg := &waE2E.ImageMessage{
		URL:           proto.String(resp.URL),
		DirectPath:    proto.String(resp.DirectPath),
		MediaKey:      resp.MediaKey,
		Mimetype:      proto.String(mimetype),
		FileEncSHA256: resp.FileEncSHA256,
		FileSHA256:    resp.FileSHA256,
		FileLength:    proto.Uint64(resp.FileLength),
	}
	if caption != "" {
		imageMsg.Caption = proto.String(caption)
	}
	if viewOnce {
		imageMsg.ViewOnce = proto.Bool(true)
	}
	_, err = client.SendMessage(ctx, jid, &waE2E.Message{ImageMessage: imageMsg})
	return err
}

// SendVideo sends a video file to a contact or group. Supports view-once.
func (c *WhatsAppNativeChannel) SendVideo(ctx context.Context, chatID string, videoData []byte, caption string, viewOnce bool) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	resp, err := client.Upload(ctx, videoData, whatsmeow.MediaVideo)
	if err != nil {
		return fmt.Errorf("upload video: %w", err)
	}
	videoMsg := &waE2E.VideoMessage{
		URL:           proto.String(resp.URL),
		DirectPath:    proto.String(resp.DirectPath),
		MediaKey:      resp.MediaKey,
		Mimetype:      proto.String("video/mp4"),
		FileEncSHA256: resp.FileEncSHA256,
		FileSHA256:    resp.FileSHA256,
		FileLength:    proto.Uint64(resp.FileLength),
	}
	if caption != "" {
		videoMsg.Caption = proto.String(caption)
	}
	if viewOnce {
		videoMsg.ViewOnce = proto.Bool(true)
	}
	msg := &waE2E.Message{VideoMessage: videoMsg}
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// SendDocument sends a document file to a contact or group.
func (c *WhatsAppNativeChannel) SendDocument(ctx context.Context, chatID string, docData []byte, mimetype string, filename string, caption string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	resp, err := client.Upload(ctx, docData, whatsmeow.MediaDocument)
	if err != nil {
		return fmt.Errorf("upload document: %w", err)
	}
	if mimetype == "" {
		mimetype = "application/octet-stream"
	}
	docMsg := &waE2E.DocumentMessage{
		URL:           proto.String(resp.URL),
		DirectPath:    proto.String(resp.DirectPath),
		MediaKey:      resp.MediaKey,
		Mimetype:      proto.String(mimetype),
		FileEncSHA256: resp.FileEncSHA256,
		FileSHA256:    resp.FileSHA256,
		FileLength:    proto.Uint64(resp.FileLength),
		FileName:      proto.String(filename),
		Title:         proto.String(filename),
	}
	if caption != "" {
		docMsg.Caption = proto.String(caption)
	}
	_, err = client.SendMessage(ctx, jid, &waE2E.Message{DocumentMessage: docMsg})
	return err
}

// SendContact sends a vCard contact message.
func (c *WhatsAppNativeChannel) SendContact(ctx context.Context, chatID string, name string, phone string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	vcard := fmt.Sprintf("BEGIN:VCARD\nVERSION:3.0\nFN:%s\nTEL;type=CELL;type=VOICE;waid=%s:%s\nEND:VCARD", name, phone, "+"+phone)
	msg := &waE2E.Message{
		ContactMessage: &waE2E.ContactMessage{
			DisplayName: proto.String(name),
			Vcard:       proto.String(vcard),
		},
	}
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// SendPoll sends a poll message to a contact or group.
// selectableCount=1 for single choice, 0 or len(options) for multiple choice.
func (c *WhatsAppNativeChannel) SendPoll(ctx context.Context, chatID string, question string, options []string, selectableCount int) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	msg := client.BuildPollCreation(question, options, selectableCount)
	_, err = client.SendMessage(ctx, jid, msg)
	return err
}

// PresenceInfo holds the last known presence state of a contact.
type PresenceInfo struct {
	Online   bool      `json:"online"`
	LastSeen time.Time `json:"last_seen,omitempty"`
	Updated  time.Time `json:"updated"`
}

// SubscribePresence subscribes to presence updates for a contact.
// Requires the bot to be marked as online first.
func (c *WhatsAppNativeChannel) SubscribePresence(ctx context.Context, chatID string) error {
	jid, err := parseJID(chatID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fmt.Errorf("whatsapp not connected")
	}
	// Mark ourselves as online so we receive presence updates
	_ = client.SendPresence(ctx, types.PresenceAvailable)
	return client.SubscribePresence(ctx, jid)
}

// GetPresence returns the last known presence for a contact.
func (c *WhatsAppNativeChannel) GetPresence(chatID string) *PresenceInfo {
	c.presenceMu.RLock()
	defer c.presenceMu.RUnlock()
	if c.presenceMap == nil {
		return nil
	}
	return c.presenceMap[chatID]
}

// handlePresenceEvent stores presence updates from subscribed contacts.
func (c *WhatsAppNativeChannel) handlePresenceEvent(evt *events.Presence) {
	jid := evt.From.ToNonAD().String()
	info := &PresenceInfo{
		Online:  !evt.Unavailable,
		Updated: time.Now(),
	}
	if !evt.LastSeen.IsZero() {
		info.LastSeen = evt.LastSeen
	}
	c.presenceMu.Lock()
	if c.presenceMap == nil {
		c.presenceMap = make(map[string]*PresenceInfo)
	}
	c.presenceMap[jid] = info
	c.presenceMu.Unlock()
}

// parseJID converts a chat ID (phone number or JID string) to types.JID.
func parseJID(s string) (types.JID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return types.JID{}, fmt.Errorf("empty chat id")
	}
	if strings.Contains(s, "@") {
		return types.ParseJID(s)
	}
	return types.NewJID(s, types.DefaultUserServer), nil
}
