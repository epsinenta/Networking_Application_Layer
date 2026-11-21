package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"prodname/common"
	"strings"
	"sync"
	"time"
)

type hub struct {
	sync.Mutex
	clients  map[string]chan common.Chunk
	requests map[string]common.Request
	received map[string]map[int]bool
}

var h = hub{clients: map[string]chan common.Chunk{}, requests: map[string]common.Request{}, received: map[string]map[int]bool{}}

func main() {
	mux := http.NewServeMux()
	transport := common.Env("TRANSPORT_URL", "http://localhost:8082")
	mux.HandleFunc("/events", common.Only("GET", func(w http.ResponseWriter, r *http.Request) {
		sender := strings.TrimSpace(r.URL.Query().Get("sender"))
		if sender == "" || len(sender) > 100 {
			common.Error(w, 400, "invalid sender")
			return
		}
		f, ok := w.(http.Flusher)
		if !ok {
			common.Error(w, 500, "streaming unavailable")
			return
		}
		ch := make(chan common.Chunk, 32)
		h.Lock()
		if _, ok := h.clients[sender]; ok {
			h.Unlock()
			common.Error(w, 409, "name already connected")
			return
		}
		h.clients[sender] = ch
		h.Unlock()
		defer func() {
			h.Lock()
			delete(h.clients, sender)
			for id, req := range h.requests {
				if req.Sender == sender {
					delete(h.requests, id)
					delete(h.received, id)
				}
			}
			h.Unlock()
		}()
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
	mux.HandleFunc("/api/v1/send", common.Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var req common.Request
		if common.Decode(r, &req) != nil || req.Validate() != nil {
			common.Error(w, 400, "invalid request")
			return
		}
		h.Lock()
		_, connected := h.clients[req.Sender]
		_, exists := h.requests[req.RequestID]
		if connected && !exists {
			h.requests[req.RequestID] = req
		}
		h.Unlock()
		if !connected || exists {
			common.Error(w, 409, "sender not connected or request already exists")
			return
		}
		if err := common.Post(transport+"/api/v1/input", req); err != nil {
			h.Lock()
			delete(h.requests, req.RequestID)
			h.Unlock()
			common.Error(w, 503, err.Error())
			return
		}
		common.JSON(w, 202, map[string]string{"request_id": req.RequestID, "status": "accepted"})
		go func() {
			time.Sleep(5 * time.Minute)
			h.Lock()
			req, ok := h.requests[req.RequestID]
			if ok {
				delete(h.requests, req.RequestID)
				if ch := h.clients[req.Sender]; ch != nil {
					select {
					case ch <- common.Chunk{Request: req, IsError: true, Payload: []string{}, ErrorMessage: "Время ожидания истекло"}:
					default:
					}
				}
			}
			h.Unlock()
		}()
	}))
	mux.HandleFunc("/api/v1/receive", common.Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var c common.Chunk
		if common.Decode(r, &c) != nil || c.RequestID == "" {
			common.Error(w, 400, "invalid chunk")
			return
		}
		h.Lock()
		req, ok := h.requests[c.RequestID]
		if !ok || req.Sender != c.Sender || req.SentAt != c.SentAt {
			h.Unlock()
			common.Error(w, 404, "request not found")
			return
		}
		ch := h.clients[req.Sender]
		if c.IsError {
			delete(h.requests, c.RequestID)
			delete(h.received, c.RequestID)
		} else {
			if c.ChunkIndex < 1 || c.ChunkTotal < 1 || c.ChunkIndex > c.ChunkTotal {
				h.Unlock()
				common.Error(w, 400, "invalid chunk index")
				return
			}
			if h.received[c.RequestID] == nil {
				h.received[c.RequestID] = map[int]bool{}
			}
			h.received[c.RequestID][c.ChunkIndex] = true
			if len(h.received[c.RequestID]) == c.ChunkTotal {
				delete(h.requests, c.RequestID)
				delete(h.received, c.RequestID)
			}
		}
		if ch != nil {
			select {
			case ch <- c:
			default:
				h.Unlock()
				common.Error(w, 503, "client queue full")
				return
			}
		}
		h.Unlock()
		common.JSON(w, 202, map[string]string{"request_id": c.RequestID, "status": "accepted"})
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("/", http.FileServer(http.Dir(common.Env("STATIC_DIR", "frontend/dist"))))
	log.Fatal(http.ListenAndServe(":"+common.Env("PORT", "8081"), mux))
}
