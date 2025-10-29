// detector/registry.go
package detector

import (
	"fmt"
	"sync"
)

// Registry manages available detectors
type Registry struct {
	detectors map[string]Detector
	mu        sync.RWMutex
}

// NewRegistry creates a new detector registry
func NewRegistry() *Registry {
	return &Registry{
		detectors: make(map[string]Detector),
	}
}

// Register adds a detector to the registry
func (r *Registry) Register(d Detector) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := d.Name()
	if _, exists := r.detectors[name]; exists {
		return fmt.Errorf("detector %q already registered", name)
	}

	r.detectors[name] = d
	return nil
}

// Get retrieves a detector by name
func (r *Registry) Get(name string) (Detector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d, ok := r.detectors[name]
	if !ok {
		return nil, fmt.Errorf("detector %q not found", name)
	}

	return d, nil
}

// List returns all registered detectors
func (r *Registry) List() []Detector {
	r.mu.RLock()
	defer r.mu.RUnlock()

	detectors := make([]Detector, 0, len(r.detectors))
	for _, d := range r.detectors {
		detectors = append(detectors, d)
	}

	return detectors
}

// Names returns all detector names
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.detectors))
	for name := range r.detectors {
		names = append(names, name)
	}

	return names
}

// DefaultRegistry returns a registry with built-in detectors
func DefaultRegistry() *Registry {
	r := NewRegistry()

	// Register real spam detectors
	r.Register(NewInvalidHandleDetector())
	r.Register(NewAlsoKnownAsSpamDetector())
	r.Register(NewCompositeSpamDetector())
	r.Register(NewSpamPDSDetector())
	r.Register(NewServiceAbuseDetector())

	return r
}
