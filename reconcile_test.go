package workflows

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
)

type recordingActivityPublisher struct {
	mu       sync.Mutex
	items    []tool.WorkflowActivityMetadata
	failOnce bool
}

type deduplicatingActivityPublisher struct {
	seen       map[uuid.UUID]struct{}
	deliveries int
}

func (p *deduplicatingActivityPublisher) PublishWorkflowActivity(_ context.Context, item tool.WorkflowActivityMetadata) error {
	if _, duplicate := p.seen[item.EventID]; duplicate {
		return nil
	}
	p.seen[item.EventID] = struct{}{}
	p.deliveries++
	return nil
}

func (p *recordingActivityPublisher) PublishWorkflowActivity(_ context.Context, item tool.WorkflowActivityMetadata) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = append(p.items, item)
	if p.failOnce {
		p.failOnce = false
		return errors.New("publisher unavailable: SECRET")
	}
	return nil
}

func (p *recordingActivityPublisher) snapshot() []tool.WorkflowActivityMetadata {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]tool.WorkflowActivityMetadata(nil), p.items...)
}

func TestReconcileRetriesStableIDsAndAdvancesCursorAfterDurableDuplicate(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunRunning, 116)
	run.StatusSummary = "workflow running"
	updated, err := f.registry.CompareAndSwap(context.Background(), run.Revision, *run)
	if err != nil {
		t.Fatal(err)
	}
	_, metadata, history := activityProjectionFixture(t)
	history[0].Run.GraphRunID = updated.GraphRunID
	history = history[:1]
	publisher := &recordingActivityPublisher{failOnce: true}

	if _, err := reconcileActivityHistory(context.Background(), f.registry, publisher, metadata, updated, history); err == nil {
		t.Fatal("first reconciliation error = nil")
	}
	afterFailure, err := f.registry.Get(context.Background(), f.session, updated.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFailure.ActivityCursor != 0 {
		t.Fatalf("cursor after publisher failure = %d, want 0", afterFailure.ActivityCursor)
	}
	got, err := reconcileActivityHistory(context.Background(), f.registry, publisher, metadata, afterFailure, history)
	if err != nil {
		t.Fatalf("retry reconciliation: %v", err)
	}
	items := publisher.snapshot()
	if len(items) < 2 || items[0].EventID != items[1].EventID {
		t.Fatalf("retry did not reuse stable ID: %#v", items)
	}
	if got.ActivityCursor != 1 {
		t.Fatalf("cursor = %d, want 1 checkpoint consumed", got.ActivityCursor)
	}
}

func TestWorkflowHooksOnlyWakeAfterDurableCheckpoint(t *testing.T) {
	var mu sync.Mutex
	var revisions []uint64
	hooks := workflowHooks(func(_ flow.GraphRunID, revision uint64) {
		mu.Lock()
		revisions = append(revisions, revision)
		mu.Unlock()
	})
	if hooks.OnRunFinish != nil || hooks.OnVertexFinish != nil {
		t.Fatal("non-durable hooks must not publish or wake completion")
	}
	hooks.OnCheckpoint(context.Background(), testGraphRunID(117), 4, 0)
	mu.Lock()
	defer mu.Unlock()
	if len(revisions) != 1 || revisions[0] != 4 {
		t.Fatalf("checkpoint wakes = %v, want [4]", revisions)
	}
}

func TestReconcileDurableDuplicateAdvancesCursorWithoutSecondDelivery(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunRunning, 121)
	_, metadata, history := activityProjectionFixture(t)
	history = history[:1]
	history[0].Run.GraphRunID = run.GraphRunID
	projected, err := projectActivities(*run, metadata, history)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &deduplicatingActivityPublisher{seen: make(map[uuid.UUID]struct{})}
	if err := publisher.PublishWorkflowActivity(context.Background(), projected[0]); err != nil {
		t.Fatal(err)
	}
	updated, err := reconcileActivityHistory(context.Background(), f.registry, publisher, metadata, run, history)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.deliveries != 1 {
		t.Fatalf("Hub-equivalent deliveries = %d, want 1", publisher.deliveries)
	}
	if updated.ActivityCursor != 1 {
		t.Fatalf("cursor = %d, want 1", updated.ActivityCursor)
	}
}

func TestActivityIDSeparatesTransitionOrdinal(t *testing.T) {
	base := activityIdentity{SessionID: testUUID(118), RunID: testUUID(119), Kind: activityVertexCompleted, Revision: 7, VertexID: uuid.UUID(testUUID(120))}
	first := stableActivityID(base)
	base.Ordinal = 1
	second := stableActivityID(base)
	if first == second {
		t.Fatal("different semantic transition ordinals shared an ID")
	}
}

func TestReconcileDefiniteSupervisorFailurePublishesRunFailedWithoutFlowHistory(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunRunning, 122)
	publisher := &recordingActivityPublisher{}
	supervisor := f.supervisor(t, f.backend.Leaser)
	supervisor.activity = publisher
	supervisor.state = supervisorActive
	controller := &runController{supervisor: supervisor, id: run.ID}
	if err := controller.failDefinite(context.Background(), run, "safe failure"); err != nil {
		t.Fatalf("failDefinite: %v", err)
	}
	stored, err := f.registry.Get(context.Background(), f.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != RunFailed {
		t.Fatalf("stored status = %s, want failed", stored.Status)
	}
	items := publisher.snapshot()
	if len(items) != 1 || items[0].Kind != "run_failed" || items[0].Status != "failed" {
		t.Fatalf("failure activity = %#v, want one run_failed", items)
	}
}
