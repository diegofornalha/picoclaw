//go:build whatsapp_native

package whatsapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

// readDevicePhone reads the phone number from a whatsmeow store.db.
// Returns the phone number (e.g. "5521923670743") or empty string.
func readDevicePhone(dbPath string) string {
	if _, err := os.Stat(dbPath); err != nil {
		return ""
	}
	db, err := sql.Open(sqliteDriver, "file:"+dbPath+"?_foreign_keys=on")
	if err != nil {
		return ""
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var jid string
	err = db.QueryRow("SELECT jid FROM whatsmeow_device LIMIT 1").Scan(&jid)
	if err != nil || jid == "" {
		return ""
	}
	// jid format: "5521923670743:60@s.whatsapp.net" → extract phone
	parts := strings.SplitN(jid, ":", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

// SlotStatus represents the connection state of a pool slot.
type SlotStatus int

const (
	SlotDisconnected SlotStatus = iota
	SlotPairing
	SlotConnected
)

func (s SlotStatus) String() string {
	switch s {
	case SlotDisconnected:
		return "disconnected"
	case SlotPairing:
		return "pairing"
	case SlotConnected:
		return "connected"
	default:
		return "unknown"
	}
}

// PoolQREvent is emitted when a slot produces a QR code or changes pairing state.
type PoolQREvent struct {
	SlotNumber int    `json:"slot_number"`
	SlotName   string `json:"slot_name"`
	Event      string `json:"event"` // "code", "success", "timeout", "error"
	Code       string `json:"code"`  // QR data (only for "code" event)
}

// SlotStatusInfo is the public view of a slot's state.
type SlotStatusInfo struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// PoolSlot tracks a single WhatsApp instance within the pool.
type PoolSlot struct {
	Number  int
	Name    string // "whatsapp_1", "whatsapp_2", ...
	Channel *WhatsAppNativeChannel
	Status  SlotStatus
}

// WhatsAppPool manages multiple WhatsApp slots with sequential pairing.
type WhatsAppPool struct {
	mu           sync.RWMutex
	slots        map[int]*PoolSlot
	pairingQueue []int // slot numbers needing pairing, in FIFO order
	maxSlots     int
	basePath     string // e.g. workspace/whatsapp
	config       config.WhatsAppConfig
	bus          *bus.MessageBus
	qrEvents     chan PoolQREvent
	runCtx       context.Context
	runCancel    context.CancelFunc
}

// NewWhatsAppPool creates a pool manager for multiple WhatsApp slots.
func NewWhatsAppPool(cfg config.WhatsAppConfig, msgBus *bus.MessageBus, basePath string, maxSlots int) *WhatsAppPool {
	if maxSlots <= 0 {
		maxSlots = 10
	}
	return &WhatsAppPool{
		slots:    make(map[int]*PoolSlot),
		maxSlots: maxSlots,
		basePath: basePath,
		config:   cfg,
		bus:      msgBus,
		qrEvents: make(chan PoolQREvent, 32),
	}
}

// slotName returns the channel name for a given slot number.
func slotName(n int) string {
	return fmt.Sprintf("whatsapp_%d", n)
}

// slotStorePath returns the store directory for a given slot number.
func (p *WhatsAppPool) slotStorePath(n int) string {
	return filepath.Join(p.basePath, strconv.Itoa(n))
}

// QREvents returns a read-only channel of QR events for the web UI.
func (p *WhatsAppPool) QREvents() <-chan PoolQREvent {
	return p.qrEvents
}

// GetSlotStatuses returns the current status of all slots.
func (p *WhatsAppPool) GetSlotStatuses() []SlotStatusInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]SlotStatusInfo, 0, len(p.slots))
	for _, slot := range p.slots {
		result = append(result, SlotStatusInfo{
			Number: slot.Number,
			Name:   slot.Name,
			Status: slot.Status.String(),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Number < result[j].Number })
	return result
}

// migrateOldLayout moves a legacy workspace/whatsapp/store.db to workspace/whatsapp/1/store.db.
func (p *WhatsAppPool) migrateOldLayout() error {
	oldDB := filepath.Join(p.basePath, whatsappDBName)
	if _, err := os.Stat(oldDB); err != nil {
		return nil // no old layout, nothing to migrate
	}

	newDir := p.slotStorePath(1)
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		return fmt.Errorf("pool migrate: create slot 1 dir: %w", err)
	}

	newDB := filepath.Join(newDir, whatsappDBName)
	if _, err := os.Stat(newDB); err == nil {
		return nil // slot 1 already has a store, skip migration
	}

	if err := os.Rename(oldDB, newDB); err != nil {
		return fmt.Errorf("pool migrate: move store.db to slot 1: %w", err)
	}

	logger.InfoCF("whatsapp-pool", "Migrated legacy store.db to slot 1", map[string]any{
		"from": oldDB,
		"to":   newDB,
	})
	return nil
}

// discoverExistingSlots scans the basePath for existing numbered subdirectories with store.db.
func (p *WhatsAppPool) discoverExistingSlots() []int {
	entries, err := os.ReadDir(p.basePath)
	if err != nil {
		return nil
	}

	var slots []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		n, err := strconv.Atoi(e.Name())
		if err != nil || n <= 0 {
			continue
		}
		dbPath := filepath.Join(p.basePath, e.Name(), whatsappDBName)
		if _, err := os.Stat(dbPath); err == nil {
			slots = append(slots, n)
		}
	}
	sort.Ints(slots)
	return slots
}

// createSlot creates a WhatsAppNativeChannel for the given slot number and wires hooks.
func (p *WhatsAppPool) createSlot(n int) (*PoolSlot, error) {
	name := slotName(n)
	storePath := p.slotStorePath(n)

	ch, err := NewWhatsAppNativeChannel(name, p.config, p.bus, storePath)
	if err != nil {
		return nil, fmt.Errorf("pool: create slot %d: %w", n, err)
	}

	nativeCh := ch.(*WhatsAppNativeChannel)

	slot := &PoolSlot{
		Number:  n,
		Name:    name,
		Channel: nativeCh,
		Status:  SlotDisconnected,
	}

	// Wire QR callback
	nativeCh.QRCallback = func(event, code string) {
		p.mu.Lock()
		if event == "code" {
			slot.Status = SlotPairing
		}
		p.mu.Unlock()

		select {
		case p.qrEvents <- PoolQREvent{
			SlotNumber: n,
			SlotName:   name,
			Event:      event,
			Code:       code,
		}:
		default:
			// drop if channel is full
		}
	}

	// Note: we intentionally do NOT wire OnDisconnect here.
	// The whatsmeow client handles reconnection automatically via reconnectWithBackoff.
	// Setting OnDisconnect would interfere with the reconnect logic and cause instability.

	return slot, nil
}

// Start initializes the pool: migrates old layout, discovers existing slots, starts them,
// and creates the next slot for pairing.
func (p *WhatsAppPool) Start(ctx context.Context) (map[string]channels.Channel, error) {
	p.runCtx, p.runCancel = context.WithCancel(ctx)

	if err := os.MkdirAll(p.basePath, 0o700); err != nil {
		return nil, fmt.Errorf("pool: create base dir: %w", err)
	}

	// Migrate legacy single-slot layout
	if err := p.migrateOldLayout(); err != nil {
		logger.WarnCF("whatsapp-pool", "Migration warning", map[string]any{"error": err.Error()})
	}

	// Discover existing slots
	existing := p.discoverExistingSlots()
	channelMap := make(map[string]channels.Channel)

	p.mu.Lock()

	// Create and start existing slots
	for _, n := range existing {
		slot, err := p.createSlot(n)
		if err != nil {
			logger.ErrorCF("whatsapp-pool", "Failed to create slot", map[string]any{
				"slot":  slotName(n),
				"error": err.Error(),
			})
			continue
		}
		p.slots[n] = slot
		channelMap[slot.Name] = slot.Channel
	}

	// Note: new slots are created on-demand via the QR web app.
	// The pool only starts slots that already have a store.db (previously paired).

	// Collect all bot phone numbers from stores, then set IgnoreNumbers on each slot
	// so they ignore messages from each other.
	botNumbers := make(map[string]bool)
	for _, slot := range p.slots {
		dbPath := filepath.Join(slot.Channel.storePath, whatsappDBName)
		if phone := readDevicePhone(dbPath); phone != "" {
			botNumbers[phone] = true
		}
	}
	if len(botNumbers) > 1 {
		for _, slot := range p.slots {
			slot.Channel.IgnoreNumbers = botNumbers
		}
		logger.InfoCF("whatsapp-pool", "Bot numbers cross-ignore configured", map[string]any{
			"count": len(botNumbers),
		})
	}

	p.mu.Unlock()

	logger.InfoCF("whatsapp-pool", "Pool started", map[string]any{
		"existing_slots": len(existing),
		"total_slots":    len(channelMap),
		"max_slots":      p.maxSlots,
	})

	// Do NOT call Start() here — the manager's StartAll() will start all channels.
	// Calling Start() here would cause double-connect (two whatsmeow clients
	// fighting over the same store.db), which causes WebSocket instability.

	go p.watchSlotConnections()

	return channelMap, nil
}

// removePairingSlot removes a slot number from the pairing queue. Caller must hold p.mu.
func (p *WhatsAppPool) removePairingSlot(n int) {
	for i, num := range p.pairingQueue {
		if num == n {
			p.pairingQueue = append(p.pairingQueue[:i], p.pairingQueue[i+1:]...)
			return
		}
	}
}

// watchSlotConnections monitors QR events and advances the pool when a slot connects.
func (p *WhatsAppPool) watchSlotConnections() {
	for {
		select {
		case <-p.runCtx.Done():
			return
		case evt := <-p.qrEvents:
			// Re-broadcast to external listeners (web UI reads from QREvents())
			// The event was already sent to qrEvents by the callback, so external
			// consumers reading QREvents() will see it.

			if evt.Event == "success" || evt.Event == "pair-success" {
				p.handleSlotConnected(evt.SlotNumber)
			}
		}
	}
}

// handleSlotConnected marks a slot as connected and creates the next slot for pairing.
func (p *WhatsAppPool) handleSlotConnected(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	slot, ok := p.slots[n]
	if !ok {
		return
	}
	slot.Status = SlotConnected
	p.removePairingSlot(n)

	logger.InfoCF("whatsapp-pool", "Slot connected", map[string]any{
		"slot": slot.Name,
	})

	// Check if we should create the next slot
	maxExisting := 0
	for num := range p.slots {
		if num > maxExisting {
			maxExisting = num
		}
	}

	nextNum := maxExisting + 1
	if nextNum > p.maxSlots {
		logger.InfoCF("whatsapp-pool", "Max slots reached", map[string]any{
			"max": p.maxSlots,
		})
		return
	}

	// Only create next if all existing slots are connected
	allConnected := true
	for _, s := range p.slots {
		if s.Status != SlotConnected {
			allConnected = false
			break
		}
	}

	if !allConnected {
		return
	}

	// Create and start next slot
	newSlot, err := p.createSlot(nextNum)
	if err != nil {
		logger.ErrorCF("whatsapp-pool", "Failed to create next slot", map[string]any{
			"slot":  slotName(nextNum),
			"error": err.Error(),
		})
		return
	}

	p.slots[nextNum] = newSlot
	p.pairingQueue = append(p.pairingQueue, nextNum)

	// Start it outside the lock
	go func() {
		if err := newSlot.Channel.Start(p.runCtx); err != nil {
			logger.ErrorCF("whatsapp-pool", "Failed to start new slot", map[string]any{
				"slot":  newSlot.Name,
				"error": err.Error(),
			})
		}
	}()
}

// Stop gracefully stops all pool slots.
func (p *WhatsAppPool) Stop(ctx context.Context) error {
	if p.runCancel != nil {
		p.runCancel()
	}

	p.mu.RLock()
	slots := make([]*PoolSlot, 0, len(p.slots))
	for _, s := range p.slots {
		slots = append(slots, s)
	}
	p.mu.RUnlock()

	for _, slot := range slots {
		if err := slot.Channel.Stop(ctx); err != nil {
			logger.ErrorCF("whatsapp-pool", "Error stopping slot", map[string]any{
				"slot":  slot.Name,
				"error": err.Error(),
			})
		}
	}

	close(p.qrEvents)
	return nil
}

// GetSlot returns a specific pool slot by number.
func (p *WhatsAppPool) GetSlot(n int) (*PoolSlot, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	slot, ok := p.slots[n]
	return slot, ok
}

// ConnectedSlots returns all currently connected slots.
func (p *WhatsAppPool) ConnectedSlots() []*PoolSlot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []*PoolSlot
	for _, s := range p.slots {
		if s.Status == SlotConnected {
			result = append(result, s)
		}
	}
	return result
}

// RegisterNewSlot dynamically registers a new slot's channel with the manager.
// This is called by the pool when advancing to the next slot.
func (p *WhatsAppPool) RegisterNewSlot(manager interface {
	RegisterChannel(name string, channel channels.Channel)
}, slot *PoolSlot) {
	manager.RegisterChannel(slot.Name, slot.Channel)
}
