package plcbundle

import (
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

type config struct {
	bundleConfig *bundle.Config
	plcClient    *plcclient.Client
}

func defaultConfig() *config {
	return &config{
		bundleConfig: bundle.DefaultConfig("./plc_bundles"),
	}
}

// Option configures the Manager
type Option func(*config)

// WithDirectory sets the bundle storage directory
func WithDirectory(dir string) Option {
	return func(c *config) {
		c.bundleConfig.BundleDir = dir
	}
}

// WithPLCDirectory sets the PLC directory URL
func WithPLCDirectory(url string) Option {
	return func(c *config) {
		c.plcClient = plcclient.NewClient(url)
	}
}

// WithVerifyOnLoad enables/disables hash verification when loading bundles
func WithVerifyOnLoad(verify bool) Option {
	return func(c *config) {
		c.bundleConfig.VerifyOnLoad = verify
	}
}

// WithLogger sets a custom logger
func WithLogger(logger Logger) Option {
	return func(c *config) {
		c.bundleConfig.Logger = logger
	}
}

// Logger interface
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
