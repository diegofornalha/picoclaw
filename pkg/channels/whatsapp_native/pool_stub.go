//go:build !whatsapp_native

package whatsapp

import (
	"context"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

// SlotStatus represents the connection state of a pool slot.
type SlotStatus int

const (
	SlotDisconnected SlotStatus = iota
	SlotPairing
	SlotConnected
)

// PoolQREvent is emitted when a slot produces a QR code or changes pairing state.
type PoolQREvent struct {
	SlotNumber int    `json:"slot_number"`
	SlotName   string `json:"slot_name"`
	Event      string `json:"event"`
	Code       string `json:"code"`
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
	Name    string
	Channel *WhatsAppNativeChannel
	Status  SlotStatus
}

// WhatsAppPool is a stub when whatsapp_native build tag is not set.
type WhatsAppPool struct{}

// NewWhatsAppPool returns an error when the binary was not built with -tags whatsapp_native.
func NewWhatsAppPool(_ config.WhatsAppConfig, _ *bus.MessageBus, _ string, _ int) *WhatsAppPool {
	return nil
}

func (p *WhatsAppPool) Start(_ context.Context) (map[string]channels.Channel, error) {
	return nil, fmt.Errorf("whatsapp pool not compiled in; build with -tags whatsapp_native")
}

func (p *WhatsAppPool) Stop(_ context.Context) error { return nil }

func (p *WhatsAppPool) GetSlotStatuses() []SlotStatusInfo { return nil }

func (p *WhatsAppPool) QREvents() <-chan PoolQREvent { return nil }

func (p *WhatsAppPool) ConnectedSlots() []*PoolSlot { return nil }

func (p *WhatsAppPool) GetSlot(_ int) (*PoolSlot, bool) { return nil, false }
