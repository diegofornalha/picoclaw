//go:build !whatsapp_native

package whatsapp

import (
	"context"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

// WhatsAppNativeChannel is a stub type when whatsapp_native build tag is not set.
type WhatsAppNativeChannel struct{}

// SendStatus is a stub.
func (c *WhatsAppNativeChannel) SendStatus(_ context.Context, _ string) error {
	return fmt.Errorf("whatsapp native not compiled in; build with -tags whatsapp_native")
}

// NewWhatsAppNativeChannel returns an error when the binary was not built with -tags whatsapp_native.
// Build with: go build -tags whatsapp_native ./cmd/...
func NewWhatsAppNativeChannel(
	name string,
	cfg config.WhatsAppConfig,
	bus *bus.MessageBus,
	storePath string,
) (channels.Channel, error) {
	return nil, fmt.Errorf("whatsapp native not compiled in; build with -tags whatsapp_native")
}
