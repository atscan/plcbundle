package plcbundle

import (
	"context"
	"io"

	"tangled.org/atscan.net/plcbundle/bundle"
)

// Manager is the main entry point for plcbundle operations
type Manager struct {
	internal *bundle.Manager
}

// New creates a new plcbundle manager
func New(opts ...Option) (*Manager, error) {
	config := defaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	mgr, err := bundle.NewManager(config.bundleConfig, config.plcClient)
	if err != nil {
		return nil, err
	}

	return &Manager{internal: mgr}, nil
}

// LoadBundle loads a bundle by number
func (m *Manager) LoadBundle(ctx context.Context, bundleNumber int) (*Bundle, error) {
	b, err := m.internal.LoadBundle(ctx, bundleNumber)
	if err != nil {
		return nil, err
	}
	return toBundlePublic(b), nil
}

// StreamBundleRaw streams raw compressed bundle data
func (m *Manager) StreamBundleRaw(ctx context.Context, bundleNumber int) (io.ReadCloser, error) {
	return m.internal.StreamBundleRaw(ctx, bundleNumber)
}

// Close closes the manager and releases resources
func (m *Manager) Close() error {
	m.internal.Close()
	return nil
}

// FetchNextBundle fetches the next bundle from PLC directory
func (m *Manager) FetchNextBundle(ctx context.Context) (*Bundle, error) {
	b, err := m.internal.FetchNextBundle(ctx, false)
	if err != nil {
		return nil, err
	}
	return toBundlePublic(b), nil
}
