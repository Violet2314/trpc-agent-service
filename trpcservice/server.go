package trpcservice

import (
	"encoding/json"
	"net/http"
)

// HTTPOptions contains optional feature handlers assembled by main.
type HTTPOptions struct {
	AdminHandler http.Handler
}

// NewHTTPHandler creates the node HTTP handler. Feature-specific routes are
// registered here as their implementation tasks are completed.
func NewHTTPHandler(options ...HTTPOptions) http.Handler {
	return NewHTTPMux(options...)
}

// NewHTTPMux creates an extensible root mux for channel adapters.
func NewHTTPMux(options ...HTTPOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", getOnly(statusHandler("ok")))
	mux.HandleFunc("/readyz", getOnly(statusHandler("ready")))
	if len(options) > 0 && options[0].AdminHandler != nil {
		mux.Handle("/api/v1/", options[0].AdminHandler)
	}
	return mux
}

func getOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}

func statusHandler(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  status,
			"version": Version,
		})
	}
}
