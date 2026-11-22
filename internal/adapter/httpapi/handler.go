package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stayfatal/Networking_Application_Layer/internal/domain"
	"github.com/stayfatal/Networking_Application_Layer/internal/usecase"
	"net/http"
	"strings"
	"time"
)

type Service interface {
	Connect(string) (<-chan domain.Chunk, error)
	Disconnect(string)
	Send(domain.Request) error
	Receive(domain.Chunk) error
}

func New(s Service, staticDir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", Only("GET", func(w http.ResponseWriter, r *http.Request) {
		sender := strings.TrimSpace(r.URL.Query().Get("sender"))
		if sender == "" || len(sender) > 100 {
			Error(w, 400, "invalid sender")
			return
		}
		f, ok := w.(http.Flusher)
		if !ok {
			Error(w, 500, "streaming unavailable")
			return
		}
		ch, err := s.Connect(sender)
		if err != nil {
			Error(w, 409, err.Error())
			return
		}
		defer s.Disconnect(sender)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		w.Header().Set("X-Accel-Buffering", "no")
		fmt.Fprint(w, ": connected\n\n")
		f.Flush()
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				fmt.Fprint(w, ": keepalive\n\n")
				f.Flush()
			case c := <-ch:
				b, _ := json.Marshal(c)
				fmt.Fprintf(w, "event: generation\ndata: %s\n\n", b)
				f.Flush()
			}
		}
	}))
	mux.HandleFunc("/api/v1/send", Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var req domain.Request
		if Decode(r, &req) != nil || req.Validate() != nil {
			Error(w, 400, "invalid request")
			return
		}
		if err := s.Send(req); err != nil {
			if errors.Is(err, usecase.ErrConflict) {
				Error(w, 409, err.Error())
			} else {
				Error(w, 503, err.Error())
			}
			return
		}
		JSON(w, 202, map[string]string{"request_id": req.RequestID, "status": "accepted"})
	}))
	mux.HandleFunc("/api/v1/receive", Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var c domain.Chunk
		if Decode(r, &c) != nil || c.RequestID == "" {
			Error(w, 400, "invalid chunk")
			return
		}
		if err := s.Receive(c); err != nil {
			status := 503
			if errors.Is(err, usecase.ErrNotFound) {
				status = 404
			} else if errors.Is(err, usecase.ErrInvalidChunk) {
				status = 400
			}
			Error(w, status, err.Error())
			return
		}
		JSON(w, 202, map[string]string{"request_id": c.RequestID, "status": "accepted"})
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("/", http.FileServer(http.Dir(staticDir)))
	return mux
}
