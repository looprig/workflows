package workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/workflows"
	"github.com/looprig/workflows/internal/testworkflow"
	workflowtools "github.com/looprig/workflows/tools"
)

type bridgePublisher struct {
	mu       sync.Mutex
	events   []tool.WorkflowActivityMetadata
	seen     map[uuid.UUID]struct{}
	failOnce bool
}

func (p *bridgePublisher) PublishWorkflowActivity(_ context.Context, event tool.WorkflowActivityMetadata) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen == nil {
		p.seen = make(map[uuid.UUID]struct{})
	}
	if _, duplicate := p.seen[event.EventID]; duplicate {
		return nil
	}
	p.seen[event.EventID] = struct{}{}
	p.events = append(p.events, event)
	if p.failOnce {
		p.failOnce = false
		return errors.New("journal acknowledgement lost after append")
	}
	return nil
}

func (p *bridgePublisher) snapshot() []tool.WorkflowActivityMetadata {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]tool.WorkflowActivityMetadata(nil), p.events...)
}

type bridgeProcessPublisher struct{}

func (bridgeProcessPublisher) PublishProcessLifecycle(context.Context, tool.ProcessLifecycleMetadata) error {
	return nil
}

type bridgeCompletionNotifier struct{}

func (bridgeCompletionNotifier) NotifyProcessCompletion(context.Context, tool.ProcessCompletionNotification) error {
	return nil
}

type bridgeFixture struct {
	backend  *storage.Composite
	registry *workflows.RunRegistry
	inputs   *workflows.InputStore
	catalog  *workflows.Catalog
	def      *workflows.TypedDefinition[testworkflow.CounterState]
	session  uuid.UUID
	publish  *bridgePublisher
	ids      []uuid.UUID
}

func newBridgeFixture(t *testing.T, gate <-chan struct{}, publisher *bridgePublisher) bridgeFixture {
	t.Helper()
	return newBridgeFixtureWithDefinition(t, publisher, func(store flow.CheckpointStore) (*workflows.TypedDefinition[testworkflow.CounterState], error) {
		return testworkflow.NewDefinitionWithGate(store, gate)
	})
}

func newBridgeFixtureWithDefinition(
	t *testing.T,
	publisher *bridgePublisher,
	build func(flow.CheckpointStore) (*workflows.TypedDefinition[testworkflow.CounterState], error),
) bridgeFixture {
	t.Helper()
	backend := memstore.New()
	store := flow.NewMemStore()
	def, err := build(store)
	if err != nil {
		t.Fatal(err)
	}
	catalog := workflows.NewCatalog()
	if err := catalog.Register(def); err != nil {
		t.Fatal(err)
	}
	registry, err := workflows.NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := workflows.NewInputStore(backend.Blobs)
	if err != nil {
		t.Fatal(err)
	}
	return bridgeFixture{
		backend: backend, registry: registry, inputs: inputs, catalog: catalog,
		def: def, session: uuid.UUID{15: 1}, publish: publisher,
		ids: []uuid.UUID{{15: 2}, {15: 3}, {15: 4}},
	}
}

func (f bridgeFixture) supervisor(t *testing.T) *workflows.Supervisor {
	t.Helper()
	supervisor, err := workflows.NewSupervisor(workflows.SupervisorConfig{
		SessionID: f.session, Catalog: f.catalog, Registry: f.registry, Inputs: f.inputs,
		Leaser: f.backend.Leaser, Now: func() time.Time { return time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	services, err := tool.NewSessionResourceServices(bridgeProcessPublisher{}, bridgeCompletionNotifier{}, f.publish)
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Activate(context.Background(), services); err != nil {
		t.Fatal(err)
	}
	return supervisor
}

func (f *bridgeFixture) bundle(t *testing.T, supervisor *workflows.Supervisor) []tool.InvokableTool {
	t.Helper()
	bundle, err := workflowtools.NewBundle(workflowtools.Config{
		SessionID: f.session, Catalog: f.catalog, Registry: f.registry, Inputs: f.inputs, Supervisor: supervisor,
		Now: func() time.Time {
			return time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC)
		},
		NewID: func() (uuid.UUID, error) {
			if len(f.ids) == 0 {
				return uuid.New()
			}
			id := f.ids[0]
			f.ids = f.ids[1:]
			return id, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

type bridgeStartResult struct {
	RunID string `json:"run_id"`
}

func invokeStart(t *testing.T, start tool.InvokableTool) bridgeStartResult {
	t.Helper()
	result, err := start.InvokableRun(context.Background(), `{"definition_name":"counter_flow","definition_version":"v1","input":{"count":0}}`)
	if err != nil {
		t.Fatal(err)
	}
	block, ok := result.Content[0].(*content.TextBlock)
	if !ok {
		t.Fatalf("start result block = %T", result.Content[0])
	}
	var decoded bridgeStartResult
	if err := json.Unmarshal([]byte(block.Text), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RunID == "" {
		t.Fatalf("start result = %s", block.Text)
	}
	return decoded
}

func activityKinds(events []tool.WorkflowActivityMetadata) []string {
	kinds := make([]string, len(events))
	for i, event := range events {
		kinds[i] = event.Kind
	}
	return kinds
}

func TestBridgeEndToEnd(t *testing.T) {
	gate := make(chan struct{})
	publisher := &bridgePublisher{}
	fixture := newBridgeFixture(t, gate, publisher)
	supervisor := fixture.supervisor(t)
	defer func() { _ = supervisor.Shutdown(context.Background()) }()
	bundle := fixture.bundle(t, supervisor)

	startResult := make(chan bridgeStartResult, 1)
	startErr := make(chan error, 1)
	go func() {
		result, err := bundle[1].InvokableRun(context.Background(), `{"definition_name":"counter_flow","definition_version":"v1","input":{"count":0}}`)
		if err != nil {
			startErr <- err
			return
		}
		block := result.Content[0].(*content.TextBlock)
		var decoded bridgeStartResult
		if err := json.Unmarshal([]byte(block.Text), &decoded); err != nil {
			startErr <- err
			return
		}
		startResult <- decoded
	}()
	var started bridgeStartResult
	select {
	case err := <-startErr:
		t.Fatal(err)
	case started = <-startResult:
		if started.RunID == "" {
			t.Fatal("workflow start returned an empty run ID")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workflow start waited for graph completion instead of durable seed")
	}
	close(gate)
	if err := supervisor.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	runID, err := uuid.Parse(started.RunID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.registry.Get(context.Background(), fixture.session, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != workflows.RunCompleted {
		t.Fatalf("run status = %s, want completed", run.Status)
	}
	result, err := fixture.def.Get(context.Background(), run.GraphRunID)
	if err != nil {
		t.Fatal(err)
	}
	var state testworkflow.CounterState
	if err := json.Unmarshal(result.State, &state); err != nil {
		t.Fatal(err)
	}
	if state.Count != 2 {
		t.Fatalf("final counter = %d, want 2", state.Count)
	}
	if got, want := activityKinds(publisher.snapshot()), []string{"run_started", "vertex_completed", "vertex_completed", "run_completed"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("activity sequence = %v, want %v", got, want)
	}
}

func TestBridgeRecoveryAfterJournalAppendBeforeCursorCAS(t *testing.T) {
	publisher := &bridgePublisher{failOnce: true}
	fixture := newBridgeFixture(t, nil, publisher)
	supervisor := fixture.supervisor(t)
	bundle := fixture.bundle(t, supervisor)
	started := invokeStart(t, bundle[1])
	if err := supervisor.WaitIdle(context.Background()); err == nil {
		t.Fatal("first supervisor WaitIdle error = nil, want simulated journal acknowledgement loss")
	}
	if err := supervisor.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted := fixture.supervisor(t)
	if err := restarted.WaitIdle(context.Background()); err != nil {
		t.Fatalf("restarted supervisor WaitIdle: %v", err)
	}
	runID, err := uuid.Parse(started.RunID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.registry.Get(context.Background(), fixture.session, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != workflows.RunCompleted {
		t.Fatalf("recovered run status = %s, want completed", run.Status)
	}
	events := publisher.snapshot()
	seen := make(map[uuid.UUID]struct{}, len(events))
	for _, event := range events {
		if _, duplicate := seen[event.EventID]; duplicate {
			t.Fatalf("duplicate activity event ID %s", event.EventID)
		}
		seen[event.EventID] = struct{}{}
	}
	if len(events) != 4 {
		t.Fatalf("recovered activity count = %d, want 4: %v", len(events), activityKinds(events))
	}
	if err := restarted.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
