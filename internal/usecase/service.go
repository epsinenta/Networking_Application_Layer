package usecase

import (
	"errors"
	"github.com/stayfatal/Networking_Application_Layer/internal/domain"
	"sync"
	"time"
)

var ErrConflict = errors.New("sender not connected or request already exists")
var ErrConnected = errors.New("name already connected")
var ErrNotFound = errors.New("request not found")
var ErrInvalidChunk = errors.New("invalid chunk index")
var ErrQueueFull = errors.New("client queue full")

type Forwarder interface{ Post(string, any) error }
type Service struct {
	mu        sync.Mutex
	clients   map[string]chan domain.Chunk
	requests  map[string]domain.Request
	received  map[string]map[int]bool
	forwarder Forwarder
	url       string
}

func New(f Forwarder, url string) *Service {
	return &Service{clients: map[string]chan domain.Chunk{}, requests: map[string]domain.Request{}, received: map[string]map[int]bool{}, forwarder: f, url: url}
}
func (s *Service) Connect(sender string) (<-chan domain.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients[sender] != nil {
		return nil, ErrConnected
	}
	ch := make(chan domain.Chunk, 32)
	s.clients[sender] = ch
	return ch, nil
}
func (s *Service) Disconnect(sender string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, sender)
	for id, r := range s.requests {
		if r.Sender == sender {
			delete(s.requests, id)
			delete(s.received, id)
		}
	}
}
func (s *Service) Send(req domain.Request) error {
	s.mu.Lock()
	_, connected := s.clients[req.Sender]
	_, exists := s.requests[req.RequestID]
	if connected && !exists {
		s.requests[req.RequestID] = req
	}
	s.mu.Unlock()
	if !connected || exists {
		return ErrConflict
	}
	if err := s.forwarder.Post(s.url+"/api/v1/input", req); err != nil {
		s.mu.Lock()
		delete(s.requests, req.RequestID)
		s.mu.Unlock()
		return err
	}
	go s.expire(req.RequestID)
	return nil
}
func (s *Service) expire(id string) {
	time.Sleep(5 * time.Minute)
	s.mu.Lock()
	defer s.mu.Unlock()
	if req, ok := s.requests[id]; ok {
		delete(s.requests, id)
		delete(s.received, id)
		if ch := s.clients[req.Sender]; ch != nil {
			select {
			case ch <- domain.Chunk{Request: req, IsError: true, Payload: []string{}, ErrorMessage: "Время ожидания истекло"}:
			default:
			}
		}
	}
}
func (s *Service) Receive(c domain.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.requests[c.RequestID]
	if !ok || req.Sender != c.Sender || req.SentAt != c.SentAt {
		return ErrNotFound
	}
	if !c.IsError && (c.ChunkIndex < 1 || c.ChunkTotal < 1 || c.ChunkIndex > c.ChunkTotal) {
		return ErrInvalidChunk
	}
	ch := s.clients[req.Sender]
	if ch != nil {
		select {
		case ch <- c:
		default:
			return ErrQueueFull
		}
	}
	if c.IsError {
		delete(s.requests, c.RequestID)
		delete(s.received, c.RequestID)
	} else {
		if s.received[c.RequestID] == nil {
			s.received[c.RequestID] = map[int]bool{}
		}
		s.received[c.RequestID][c.ChunkIndex] = true
		if len(s.received[c.RequestID]) == c.ChunkTotal {
			delete(s.requests, c.RequestID)
			delete(s.received, c.RequestID)
		}
	}
	return nil
}
