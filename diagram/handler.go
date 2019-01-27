package diagram

import "net/http"

// Reporter represents an a health reporter.
type Stater interface {
	// IsHealthy emits error if application is not healthy.
	GetStats() (string, error)
}

// Handler is an http health handler.
type Handler struct {
	stats   []Stater
	showErr bool
}

// NewHandler creates a new Handler instance.
func NewHandler() *Handler {
	return &Handler{}
}

// With adds reports to the handler.
func (h *Handler) With(stats ...Stater) *Handler {
	h.stats = append(h.stats, stats...)
	return h
}

// ServeHTTP serves an http request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, stat := range h.stats {
		stats, err := stat.GetStats()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}
		_, err = w.Write([]byte(stats))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}
