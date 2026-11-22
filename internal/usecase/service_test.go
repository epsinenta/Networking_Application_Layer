package usecase

import (
	"errors"
	"github.com/stayfatal/Networking_Application_Layer/internal/domain"
	"testing"
)

type senderStub struct {
	err   error
	calls int
}

func (s *senderStub) Post(_ string, _ any) error { s.calls++; return s.err }
func TestRequestLifecycle(t *testing.T) {
	p := &senderStub{}
	s := New(p, "http://transport")
	ch, err := s.Connect("user")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Connect("user"); !errors.Is(err, ErrConnected) {
		t.Fatal(err)
	}
	r := domain.Request{RequestID: "1", Sender: "user", SentAt: "time"}
	if err = s.Send(r); err != nil {
		t.Fatal(err)
	}
	if err = s.Send(r); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if err = s.Receive(domain.Chunk{Request: r, ChunkIndex: i, ChunkTotal: 2}); err != nil {
			t.Fatal(err)
		}
		if c := <-ch; c.ChunkIndex != i {
			t.Fatal(c)
		}
	}
	if err = s.Receive(domain.Chunk{Request: r}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	s.Disconnect("user")
}
func TestFailedForwardAllowsRetry(t *testing.T) {
	p := &senderStub{err: errors.New("unavailable")}
	s := New(p, "")
	s.Connect("user")
	r := domain.Request{RequestID: "1", Sender: "user"}
	if s.Send(r) == nil {
		t.Fatal("expected error")
	}
	p.err = nil
	if err := s.Send(r); err != nil {
		t.Fatal(err)
	}
}
