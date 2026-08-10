package workflows

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/storage"
)

type recordingLeaser struct {
	mu           sync.Mutex
	lease        storage.Lease
	err          error
	acquireCount int
	name         string
}

func (l *recordingLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquireCount++
	l.name = name
	return l.lease, l.err
}

type controllableLease struct {
	mu           sync.Mutex
	lost         chan struct{}
	lostOnce     sync.Once
	releaseCount int
}

func newControllableLease() *controllableLease     { return &controllableLease{lost: make(chan struct{})} }
func (l *controllableLease) Epoch() uint64         { return 1 }
func (l *controllableLease) Lost() <-chan struct{} { return l.lost }
func (l *controllableLease) lose()                 { l.lostOnce.Do(func() { close(l.lost) }) }
func (l *controllableLease) Release(context.Context) error {
	l.mu.Lock()
	l.releaseCount++
	l.mu.Unlock()
	l.lose()
	return nil
}

func TestSessionResourceConcurrentSupervisorsCannotOwnSameSession(t *testing.T) {
	f := newSupervisorFixture(t)
	first := f.supervisor(t, f.backend.Leaser)
	second := f.supervisor(t, f.backend.Leaser)
	if err := first.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	err := second.Activate(context.Background(), supervisorServices(t))
	var owned *SessionOwnedError
	if !errors.Is(err, ErrSessionOwned) || !errors.As(err, &owned) {
		t.Fatalf("second Activate error = %T %v, want SessionOwnedError", err, err)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionResourceRejectsInvalidConfigurationAndServices(t *testing.T) {
	if _, err := NewSupervisor(SupervisorConfig{}); err == nil {
		t.Fatal("NewSupervisor empty config error = nil")
	}
	f := newSupervisorFixture(t)
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), tool.SessionResourceServices{}); err == nil {
		t.Fatal("Activate invalid services error = nil")
	}
}
