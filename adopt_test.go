package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/flow/pkg/flow"
)

type adoptingTestDefinition struct {
	*supervisorTestDefinition
	adoptResult *Result
	adoptErr    error
	adopts      int
}

func (d *adoptingTestDefinition) registeredCopy() (Definition, error) { return d, nil }

func (d *adoptingTestDefinition) Adopt(context.Context, flow.GraphRunID, ...flow.RunOption) (*Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.adopts++
	return d.adoptResult, d.adoptErr
}

func TestAdoptRunningReconstructsHistoryWithoutUserResume(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunRunning, 94)
	f.def.getResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 4, Status: flow.RunRunning}}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, gets, histories, resumes, _ := f.def.counts()
	if gets != 1 || histories != 1 || resumes != 0 {
		t.Fatalf("gets=%d histories=%d resumes=%d, want 1,1,0", gets, histories, resumes)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptRunningContinuesThroughDefinitionAdoptionAPI(t *testing.T) {
	f := newSupervisorFixture(t)
	definition := &adoptingTestDefinition{supervisorTestDefinition: f.def}
	f.catalog = NewCatalog()
	if err := f.catalog.Register(definition); err != nil {
		t.Fatal(err)
	}
	run := f.createRun(t, RunRunning, 104)
	f.def.getResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 4, Status: flow.RunRunning}}
	definition.adoptResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 5, Status: flow.RunCompleted}}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	definition.mu.Lock()
	adopts := definition.adopts
	definition.mu.Unlock()
	if adopts != 1 {
		t.Fatalf("adoption API calls = %d, want one", adopts)
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != RunCompleted || got.CheckpointRevision != 5 {
		t.Fatalf("adopted run = %#v, want completed at checkpoint 5", got)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptPendingWithDurableCheckpointDoesNotStartAgain(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunPending, 100)
	f.def.getResult = &Result{Run: flow.GraphRunState{
		GraphRunID: run.GraphRunID,
		Revision:   3,
		Status:     flow.RunCompleted,
	}}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	starts, gets, histories, _, _ := f.def.counts()
	if starts != 0 || gets != 1 || histories != 1 {
		t.Fatalf("starts=%d gets=%d histories=%d, want 0,1,1", starts, gets, histories)
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != RunCompleted || got.CheckpointRevision != 3 {
		t.Fatalf("adopted run = %#v, want completed at checkpoint 3", got)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptInterruptedWaitsForValidatedUserResume(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunInterrupted, 95)
	f.def.getResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 2, Status: flow.RunInterrupted}}
	f.def.resumeResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 3, Status: flow.RunCompleted}}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _, _, resumes, _ := f.def.counts()
	if resumes != 0 {
		t.Fatalf("automatic resumes = %d, want 0", resumes)
	}
	if err := s.Resume(context.Background(), run.ID, json.RawMessage(`{"increment":1}`)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	_, _, _, resumes, _ = f.def.counts()
	if resumes != 1 {
		t.Fatalf("resumes = %d, want 1", resumes)
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != RunCompleted {
		t.Fatalf("status = %s, want completed", got.Status)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorRunControllerSerializesConcurrentResume(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunInterrupted, 84)
	f.def.getResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 2, Status: flow.RunInterrupted}}
	f.def.resumeResult = &Result{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 3, Status: flow.RunCompleted}}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	const callers = 16
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			<-start
			_ = s.Resume(context.Background(), run.ID, json.RawMessage(`{"increment":1}`))
		}()
	}
	close(start)
	group.Wait()
	_, _, _, resumes, _ := f.def.counts()
	if resumes != 1 {
		t.Fatalf("definition resumes = %d, want exactly 1", resumes)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptTerminalRunsNeverRestart(t *testing.T) {
	f := newSupervisorFixture(t)
	f.createRun(t, RunCompleted, 96)
	f.createRun(t, RunCancelled, 97)
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	starts, gets, histories, resumes, _ := f.def.counts()
	if starts+gets+histories+resumes != 0 {
		t.Fatalf("terminal run work counts = %d,%d,%d,%d", starts, gets, histories, resumes)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptDefiniteMissingCheckpointMarksFailedWithSafeBoundedSummary(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunRunning, 98)
	f.def.getErr = &flow.CheckpointNotFoundError{GraphRunID: run.GraphRunID}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != RunFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if len(got.StatusSummary) > MaxStatusSummaryBytes || strings.Contains(got.StatusSummary, run.GraphRunID.String()) {
		t.Fatalf("unsafe summary %q", got.StatusSummary)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptAmbiguousCheckpointErrorLeavesDurableStateUnchanged(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunRunning, 99)
	f.def.getErr = &storageAmbiguousTestError{}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err == nil {
		t.Fatal("WaitIdle error = nil, want adoption error")
	}
	got, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != RunRunning {
		t.Fatalf("status = %s, want unchanged running", got.Status)
	}
	var adoptionErr *AdoptionError
	if !errors.As(s.LastError(), &adoptionErr) {
		t.Fatalf("LastError = %T %v, want AdoptionError", s.LastError(), s.LastError())
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type storageAmbiguousTestError struct{}

func (*storageAmbiguousTestError) Error() string { return "secret backend details" }
