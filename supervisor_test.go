package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

type supervisorTestDefinition struct {
	meta Metadata

	mu           sync.Mutex
	starts       int
	gets         int
	histories    int
	resumes      int
	cancels      int
	startEntered chan context.Context
	startRelease chan struct{}
	ignoreCancel bool
	getResult    *Result
	getErr       error
	startResult  *Result
	startErr     error
	resumeResult *Result
	resumeErr    error
}

func newSupervisorTestDefinition() *supervisorTestDefinition {
	return &supervisorTestDefinition{
		meta:         Metadata{name: "source_document_extract", version: "v1"},
		startResult:  &Result{Run: flow.GraphRunState{Status: flow.RunCompleted}},
		resumeResult: &Result{Run: flow.GraphRunState{Status: flow.RunCompleted}},
	}
}

func (d *supervisorTestDefinition) Metadata() Metadata                  { return d.meta.clone() }
func (d *supervisorTestDefinition) registeredCopy() (Definition, error) { return d, nil }
func (d *supervisorTestDefinition) ValidateInput(json.RawMessage) (ValidatedInput, error) {
	return ValidatedInput{owner: &registration{}, value: struct{}{}}, nil
}
func (d *supervisorTestDefinition) ValidateResume(json.RawMessage) (ValidatedResume, error) {
	return ValidatedResume{owner: &registration{}, value: struct{}{}}, nil
}
func (d *supervisorTestDefinition) Start(ctx context.Context, _ ValidatedInput, _ ...flow.RunOption) (*Result, error) {
	d.mu.Lock()
	d.starts++
	entered, release := d.startEntered, d.startRelease
	result, err := d.startResult, d.startErr
	d.mu.Unlock()
	if entered != nil {
		entered <- ctx
	}
	if release != nil {
		if d.ignoreCancel {
			<-release
			return result, err
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return result, err
}
func (d *supervisorTestDefinition) Resume(context.Context, flow.GraphRunID, ValidatedResume, ...flow.RunOption) (*Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resumes++
	return d.resumeResult, d.resumeErr
}
func (d *supervisorTestDefinition) Get(context.Context, flow.GraphRunID) (*Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.gets++
	return d.getResult, d.getErr
}
func (d *supervisorTestDefinition) History(context.Context, flow.GraphRunID) ([]flow.GraphRunState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.histories++
	if d.getResult == nil {
		return nil, d.getErr
	}
	return []flow.GraphRunState{d.getResult.Run}, d.getErr
}
func (d *supervisorTestDefinition) Cancel(context.Context, flow.GraphRunID, string, ...flow.RunOption) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cancels++
	return nil
}
func (d *supervisorTestDefinition) counts() (int, int, int, int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts, d.gets, d.histories, d.resumes, d.cancels
}

type supervisorFixture struct {
	backend  *storage.Composite
	registry *RunRegistry
	inputs   *InputStore
	catalog  *Catalog
	def      *supervisorTestDefinition
	session  uuid.UUID
	now      time.Time
}

func newSupervisorFixture(t *testing.T) supervisorFixture {
	t.Helper()
	backend := memstore.New()
	registry, err := NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := NewInputStore(backend.Blobs)
	if err != nil {
		t.Fatal(err)
	}
	def := newSupervisorTestDefinition()
	catalog := NewCatalog()
	if err := catalog.Register(def); err != nil {
		t.Fatal(err)
	}
	return supervisorFixture{backend: backend, registry: registry, inputs: inputs, catalog: catalog, def: def, session: testUUID(90), now: time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC)}
}

func (f supervisorFixture) supervisor(t *testing.T, leaser storage.Leaser) *Supervisor {
	t.Helper()
	s, err := NewSupervisor(SupervisorConfig{SessionID: f.session, Catalog: f.catalog, Registry: f.registry, Inputs: f.inputs, Leaser: leaser, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	return s
}

type processPublisherStub struct{}

func (processPublisherStub) PublishProcessLifecycle(context.Context, tool.ProcessLifecycleMetadata) error {
	return nil
}

type completionStub struct{}

func (completionStub) NotifyProcessCompletion(context.Context, tool.ProcessCompletionNotification) error {
	return nil
}

type activityStub struct{}

func (activityStub) PublishWorkflowActivity(context.Context, tool.WorkflowActivityMetadata) error {
	return nil
}

func supervisorServices(t *testing.T) tool.SessionResourceServices {
	t.Helper()
	services, err := tool.NewSessionResourceServices(processPublisherStub{}, completionStub{}, activityStub{})
	if err != nil {
		t.Fatal(err)
	}
	return services
}

func (f supervisorFixture) createRun(t *testing.T, status RunStatus, id byte) *Run {
	t.Helper()
	ref, err := f.inputs.Put(context.Background(), f.session, []byte(`{"count":1}`))
	if err != nil {
		t.Fatal(err)
	}
	run := testRun(f.session, testUUID(id))
	run.GraphRunID = testGraphRunID(id)
	run.LedgerLocator = "flow/runs/" + run.GraphRunID.String()
	run.Input = ref
	run.Status = status
	run.CreatedAt, run.UpdatedAt = f.now, f.now
	created, err := f.registry.Create(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestSupervisorActivateAcquiresBeforeStartingPendingExactlyOnce(t *testing.T) {
	f := newSupervisorFixture(t)
	f.createRun(t, RunPending, 91)
	leaser := &recordingLeaser{lease: newControllableLease()}
	s := f.supervisor(t, leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}
	starts, _, _, _, _ := f.def.counts()
	if starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}
	if leaser.acquireCount != 1 || leaser.name != "sessions/"+f.session.String()+"/workflows/owner" {
		t.Fatalf("lease acquisition = %d %q", leaser.acquireCount, leaser.name)
	}
	if err := s.Activate(context.Background(), supervisorServices(t)); !errors.Is(err, ErrSupervisorActive) {
		t.Fatalf("second Activate error = %v", err)
	}
	starts, _, _, _, _ = f.def.counts()
	if starts != 1 {
		t.Fatalf("starts after second Activate = %d", starts)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLossCancelsLocalExecutionWithoutTerminalWrite(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunPending, 92)
	f.def.startEntered = make(chan context.Context, 1)
	f.def.startRelease = make(chan struct{})
	lease := newControllableLease()
	s := f.supervisor(t, &recordingLeaser{lease: lease})
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	executionCtx := <-f.def.startEntered
	lease.lose()
	<-executionCtx.Done()
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == RunCancelled || got.Status == RunFailed {
		t.Fatalf("lease loss wrote terminal status %s", got.Status)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLossSuppressesTerminalWriteWhenExecutionIgnoresCancellation(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunPending, 82)
	f.def.startEntered = make(chan context.Context, 1)
	f.def.startRelease = make(chan struct{})
	f.def.ignoreCancel = true
	lease := newControllableLease()
	s := f.supervisor(t, &recordingLeaser{lease: lease})
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	<-f.def.startEntered
	lease.lose()
	close(f.def.startRelease)
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != RunRunning {
		t.Fatalf("status = %s, want nonterminal running", got.Status)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorShutdownIsIdempotentRejectsWorkAndReleases(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunInterrupted, 93)
	lease := newControllableLease()
	s := f.supervisor(t, &recordingLeaser{lease: lease})
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if lease.releaseCount != 1 {
		t.Fatalf("release count = %d, want 1", lease.releaseCount)
	}
	if err := s.Resume(context.Background(), run.ID, json.RawMessage(`{"increment":1}`)); !errors.Is(err, ErrSupervisorClosed) {
		t.Fatalf("Resume after shutdown error = %v", err)
	}
}

func TestSupervisorShutdownWaitIsBoundedAndStillReleasesLease(t *testing.T) {
	f := newSupervisorFixture(t)
	f.createRun(t, RunPending, 83)
	f.def.startEntered = make(chan context.Context, 1)
	f.def.startRelease = make(chan struct{})
	f.def.ignoreCancel = true
	lease := newControllableLease()
	s, err := NewSupervisor(SupervisorConfig{
		SessionID: f.session, Catalog: f.catalog, Registry: f.registry, Inputs: f.inputs,
		Leaser: &recordingLeaser{lease: lease}, Now: func() time.Time { return f.now },
		ShutdownTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	<-f.def.startEntered
	if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Shutdown error = %v, want timeout", err)
	}
	if lease.releaseCount != 1 {
		t.Fatalf("release count = %d, want 1", lease.releaseCount)
	}
	close(f.def.startRelease)
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("second Shutdown error = %v, want stable timeout", err)
	}
}

var _ tool.SessionResource = (*Supervisor)(nil)
