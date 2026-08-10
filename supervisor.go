package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/storage"
)

var (
	ErrSupervisorActive        = errors.New("workflow supervisor already active")
	ErrSupervisorClosed        = errors.New("workflow supervisor closed")
	ErrSessionOwned            = errors.New("workflow session already owned")
	ErrAdoption                = errors.New("workflow adoption failed")
	ErrShutdownTimeout         = errors.New("workflow supervisor shutdown timed out")
	errSupervisorOwnershipLost = errors.New("workflow supervisor ownership lost")
)

type SessionOwnedError struct {
	SessionID   uuid.UUID
	HolderEpoch uint64
}

func (e *SessionOwnedError) Error() string {
	return fmt.Sprintf("%s: session %s", ErrSessionOwned, e.SessionID)
}
func (e *SessionOwnedError) Unwrap() error { return ErrSessionOwned }

type AdoptionError struct {
	RunID uuid.UUID
	Op    string
	Err   error
}

func (e *AdoptionError) Error() string {
	return fmt.Sprintf("%s: run %s during %s", ErrAdoption, e.RunID, e.Op)
}
func (e *AdoptionError) Unwrap() error { return ErrAdoption }

type SupervisorConfig struct {
	SessionID       uuid.UUID
	Catalog         *Catalog
	Registry        *RunRegistry
	Inputs          *InputStore
	Leaser          storage.Leaser
	Now             func() time.Time
	ShutdownTimeout time.Duration
	MaxWorkers      int
}

type supervisorState uint8

const (
	supervisorInactive supervisorState = iota
	supervisorActive
	supervisorClosed
)

type Supervisor struct {
	sessionID uuid.UUID
	catalog   *Catalog
	registry  *RunRegistry
	inputs    *InputStore
	leaser    storage.Leaser
	now       func() time.Time
	timeout   time.Duration
	workers   chan struct{}

	mu          sync.Mutex
	state       supervisorState
	accepting   bool
	leaseLost   bool
	lease       storage.Lease
	ownerCtx    context.Context
	cancel      context.CancelFunc
	runs        map[uuid.UUID]*runController
	lastErr     error
	activity    tool.WorkflowActivityPublisher
	shutdownErr error
	workWG      sync.WaitGroup
	watchWG     sync.WaitGroup
	shutdown    sync.Once
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if config.SessionID.IsZero() || config.Catalog == nil || config.Registry == nil || config.Inputs == nil || config.Leaser == nil {
		return nil, errors.New("workflows: supervisor requires session, catalog, registry, input store, and leaser")
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	timeout := config.ShutdownTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	workers := config.MaxWorkers
	if workers == 0 {
		workers = 4
	}
	if workers < 1 || workers > 64 {
		return nil, errors.New("workflows: supervisor worker count must be within 1..64")
	}
	return &Supervisor{sessionID: config.SessionID, catalog: config.Catalog, registry: config.Registry, inputs: config.Inputs, leaser: config.Leaser, now: now, timeout: timeout, workers: make(chan struct{}, workers), runs: make(map[uuid.UUID]*runController)}, nil
}

func (s *Supervisor) Activate(ctx context.Context, services tool.SessionResourceServices) error {
	if err := services.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.state == supervisorActive {
		s.mu.Unlock()
		return ErrSupervisorActive
	}
	if s.state == supervisorClosed {
		s.mu.Unlock()
		return ErrSupervisorClosed
	}
	s.mu.Unlock()

	lease, err := acquireSessionLease(ctx, s.leaser, s.sessionID)
	if err != nil {
		return err
	}
	ownerCtx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.lease, s.ownerCtx, s.cancel = lease, ownerCtx, cancel
	s.activity = services.WorkflowActivityPublisher()
	s.state, s.accepting = supervisorActive, true
	s.mu.Unlock()

	s.watchWG.Add(1)
	go s.watchLease(lease)
	if err := s.loadRuns(ctx); err != nil {
		cancel()
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), s.timeout)
		_ = lease.Release(releaseCtx)
		releaseCancel()
		s.mu.Lock()
		s.state, s.accepting, s.lease = supervisorInactive, false, nil
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *Supervisor) loadRuns(ctx context.Context) error {
	after := ""
	for {
		page, err := s.registry.List(ctx, s.sessionID, ListRunsRequest{After: after, Limit: MaxRunPageSize})
		if err != nil {
			return fmt.Errorf("workflows: list runs for activation: %w", err)
		}
		for i := range page.Runs {
			run := cloneRun(page.Runs[i])
			// Terminal completed/cancelled runs are never restarted. Failed
			// runs may still need a bounded run_failed activity reconciliation;
			// that path does not execute Flow again.
			if run.Status == RunCompleted || run.Status == RunCancelled {
				continue
			}
			controller := &runController{supervisor: s, id: run.ID}
			s.mu.Lock()
			s.runs[run.ID] = controller
			s.mu.Unlock()
			s.workWG.Add(1)
			go func() {
				defer s.workWG.Done()
				select {
				case s.workers <- struct{}{}:
					defer func() { <-s.workers }()
				case <-s.ownerCtx.Done():
					return
				}
				if err := controller.reconcile(s.ownerCtx); err != nil {
					s.recordError(err)
				}
			}()
		}
		if page.Next == "" {
			return nil
		}
		after = page.Next
	}
}

func (s *Supervisor) watchLease(lease storage.Lease) {
	defer s.watchWG.Done()
	<-lease.Lost()
	s.mu.Lock()
	if s.lease == lease && s.state == supervisorActive {
		s.leaseLost = true
		s.accepting = false
		if s.cancel != nil {
			s.cancel()
		}
	}
	s.mu.Unlock()
}

func (s *Supervisor) Shutdown(ctx context.Context) error {
	s.shutdown.Do(func() {
		s.mu.Lock()
		s.state, s.accepting = supervisorClosed, false
		cancel, lease := s.cancel, s.lease
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		waitCtx, waitCancel := boundedContext(ctx, s.timeout)
		if err := waitGroup(waitCtx, &s.workWG); err != nil {
			s.shutdownErr = fmt.Errorf("%w", ErrShutdownTimeout)
		}
		waitCancel()
		if lease != nil {
			releaseCtx, releaseCancel := boundedContext(ctx, s.timeout)
			if err := lease.Release(releaseCtx); err != nil && s.shutdownErr == nil {
				s.shutdownErr = fmt.Errorf("workflows: release session ownership: %w", err)
			}
			releaseCancel()
		}
		watchCtx, watchCancel := boundedContext(ctx, s.timeout)
		if err := waitGroup(watchCtx, &s.watchWG); err != nil && s.shutdownErr == nil {
			s.shutdownErr = fmt.Errorf("%w", ErrShutdownTimeout)
		}
		watchCancel()
	})
	return s.shutdownErr
}

func (s *Supervisor) Resume(ctx context.Context, runID uuid.UUID, payload json.RawMessage) error {
	controller, err := s.controller(runID)
	if err != nil {
		return err
	}
	return controller.resume(ctx, payload)
}

func (s *Supervisor) Cancel(ctx context.Context, runID uuid.UUID, reason string) error {
	controller, err := s.controller(runID)
	if err != nil {
		return err
	}
	return controller.cancelRun(ctx, reason)
}

func (s *Supervisor) controller(runID uuid.UUID) (*runController, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == supervisorClosed {
		return nil, ErrSupervisorClosed
	}
	if s.state != supervisorActive || !s.accepting {
		return nil, errors.New("workflows: supervisor is not accepting work")
	}
	controller := s.runs[runID]
	if controller == nil {
		controller = &runController{supervisor: s, id: runID}
		s.runs[runID] = controller
	}
	return controller, nil
}

func (s *Supervisor) WaitIdle(ctx context.Context) error {
	if err := waitGroup(ctx, &s.workWG); err != nil {
		return err
	}
	return s.LastError()
}

func (s *Supervisor) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

func (s *Supervisor) recordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.lastErr == nil {
		s.lastErr = err
	}
	s.mu.Unlock()
}

func (s *Supervisor) ownershipLost() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseLost || s.state != supervisorActive {
		return true
	}
	// The watcher is deliberately asynchronous. Check the lease channel at
	// every write boundary as well, so a closed Lost channel cannot race a
	// terminal registry update before the watcher gets scheduled.
	if s.lease != nil {
		select {
		case <-s.lease.Lost():
			s.leaseLost = true
			s.accepting = false
			if s.cancel != nil {
				s.cancel()
			}
			return true
		default:
		}
	}
	return false
}

func (s *Supervisor) operationContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	s.mu.Lock()
	if s.state != supervisorActive || !s.accepting || s.ownerCtx == nil {
		s.mu.Unlock()
		return nil, nil, ErrSupervisorClosed
	}
	owner := s.ownerCtx
	s.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(owner, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

func waitGroup(ctx context.Context, group *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
