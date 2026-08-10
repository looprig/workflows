package workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/workflows"
	"github.com/looprig/workflows/internal/testworkflow"
)

type recoveryResume struct {
	Increment int `json:"increment"`
}

func newInterruptingDefinition(store flow.CheckpointStore) (*workflows.TypedDefinition[testworkflow.CounterState], error) {
	graph := flow.NewGraph[testworkflow.CounterState](flow.GraphID{4})
	vertex := flow.VertexID{5}
	task := flow.NewFuncTask(func(ctx context.Context, count int) (int, error) {
		resume, ok := flow.ResumePayload[recoveryResume](ctx)
		if !ok {
			return 0, flow.Interrupt(ctx, "awaiting increment")
		}
		return count + resume.Increment, nil
	})
	if err := flow.AddVertex(graph, vertex, task,
		func(state testworkflow.CounterState) int { return state.Count },
		func(state *testworkflow.CounterState, output int) error { state.Count = output; return nil },
	); err != nil {
		return nil, err
	}
	runner, err := graph.Compile(vertex, vertex, flow.WithStore(store))
	if err != nil {
		return nil, err
	}
	metadata, err := workflows.NewMetadata(
		"counter_flow", "v1", "Deterministic resumable counter.",
		json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer","minimum":0}},"required":["count"],"additionalProperties":false}`),
		json.RawMessage(`{"type":"object","properties":{"increment":{"type":"integer","minimum":1}},"required":["increment"],"additionalProperties":false}`),
		[]workflows.VertexMetadata{workflows.NewVertexMetadataForID(vertex, "apply increment")},
	)
	if err != nil {
		return nil, err
	}
	return workflows.NewTypedDefinition(
		metadata, runner, store,
		workflows.StrictJSONDecoder[testworkflow.CounterState],
		workflows.TypedResumeDecoder(workflows.StrictJSONDecoder[recoveryResume]),
		func(testworkflow.CounterState) string { return "counter completed" },
	)
}

func createRecoveryRun(t *testing.T, fixture bridgeFixture, status workflows.RunStatus, suffix byte) workflows.Run {
	t.Helper()
	input, err := fixture.inputs.Put(context.Background(), fixture.session, []byte(`{"count":0}`))
	if err != nil {
		t.Fatal(err)
	}
	graphRunID := flow.GraphRunID(uuid.UUID{15: suffix})
	run := workflows.Run{
		SessionID: fixture.session, ToolExecutionID: uuid.UUID{14: suffix},
		DefinitionName: "counter_flow", DefinitionVersion: "v1", ID: uuid.UUID{13: suffix},
		GraphRunID: graphRunID, Input: input, Status: status,
		LedgerLocator: "flow/runs/" + graphRunID.String(),
		CreatedAt:     time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC),
		UpdatedAt:     time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC),
	}
	created, err := fixture.registry.Create(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	return *created
}

func startRecoveryFlow(t *testing.T, fixture bridgeFixture, run workflows.Run) *workflows.Result {
	t.Helper()
	definition, err := fixture.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := definition.ValidateInput(json.RawMessage(`{"count":0}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := definition.Start(context.Background(), validated, flow.WithGraphRunID(run.GraphRunID))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertRecoveredStatus(t *testing.T, fixture bridgeFixture, runID uuid.UUID, want workflows.RunStatus) *workflows.Run {
	t.Helper()
	got, err := fixture.registry.Get(context.Background(), fixture.session, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != want {
		t.Fatalf("recovered run status = %s, want %s", got.Status, want)
	}
	return got
}

func transitionRecoveryRun(t *testing.T, fixture bridgeFixture, run workflows.Run, status workflows.RunStatus, checkpoint uint64) workflows.Run {
	t.Helper()
	next := run
	next.Status = status
	next.CheckpointRevision = checkpoint
	updated, err := fixture.registry.CompareAndSwap(context.Background(), run.Revision, next)
	if err != nil {
		t.Fatal(err)
	}
	return *updated
}

func TestBridgeRecoveryAfterRegistryCreateBeforeFirstJournalAppend(t *testing.T) {
	publisher := &bridgePublisher{}
	fixture := newBridgeFixture(t, nil, publisher)
	run := createRecoveryRun(t, fixture, workflows.RunPending, 31)
	if _, err := fixture.def.History(context.Background(), run.GraphRunID); err == nil {
		t.Fatal("journal history exists before recovery, want registry-only crash window")
	} else {
		var missing *flow.CheckpointNotFoundError
		if !errors.As(err, &missing) {
			t.Fatalf("history error = %T %v, want CheckpointNotFoundError", err, err)
		}
	}

	supervisor := fixture.supervisor(t)
	defer func() { _ = supervisor.Shutdown(context.Background()) }()
	if err := supervisor.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := assertRecoveredStatus(t, fixture, run.ID, workflows.RunCompleted)
	if got.CheckpointRevision == 0 {
		t.Fatal("recovered run checkpoint revision = 0, want durable journal progress")
	}
	if kinds := activityKinds(publisher.snapshot()); len(kinds) != 4 || kinds[0] != "run_started" || kinds[len(kinds)-1] != "run_completed" {
		t.Fatalf("recovered activity sequence = %v, want complete projected history", kinds)
	}
}

func TestBridgeRecoveryAfterInterruptionBeforeResume(t *testing.T) {
	publisher := &bridgePublisher{}
	fixture := newBridgeFixtureWithDefinition(t, publisher, newInterruptingDefinition)
	run := createRecoveryRun(t, fixture, workflows.RunPending, 32)
	result := startRecoveryFlow(t, fixture, run)
	if result.Run.Status != flow.RunInterrupted {
		t.Fatalf("seed flow status = %s, want interrupted", result.Run.Status)
	}
	running := transitionRecoveryRun(t, fixture, run, workflows.RunRunning, 0)
	interrupted := transitionRecoveryRun(t, fixture, running, workflows.RunInterrupted, result.Run.Revision)

	supervisor := fixture.supervisor(t)
	defer func() { _ = supervisor.Shutdown(context.Background()) }()
	if err := supervisor.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered := assertRecoveredStatus(t, fixture, interrupted.ID, workflows.RunInterrupted)
	if recovered.CheckpointRevision != result.Run.Revision {
		t.Fatalf("checkpoint revision after adoption = %d, want unchanged interrupted revision %d", recovered.CheckpointRevision, result.Run.Revision)
	}
	checkpoint, err := fixture.def.Get(context.Background(), interrupted.GraphRunID)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Run.Status != flow.RunInterrupted {
		t.Fatalf("checkpoint status after adoption = %s, want interrupted", checkpoint.Run.Status)
	}
	if err := supervisor.Resume(context.Background(), interrupted.ID, json.RawMessage(`{"increment":2}`)); err != nil {
		t.Fatal(err)
	}
	assertRecoveredStatus(t, fixture, interrupted.ID, workflows.RunCompleted)
	if kinds := activityKinds(publisher.snapshot()); len(kinds) != 5 || kinds[0] != "run_started" || kinds[1] != "run_interrupted" || kinds[2] != "run_resumed" || kinds[4] != "run_completed" {
		t.Fatalf("recovered activity sequence = %v, want start, interrupt, resume, vertex, complete", kinds)
	}
}

func TestBridgeRecoveryAfterTerminalJournalAppendBeforeTerminalRegistryUpdate(t *testing.T) {
	publisher := &bridgePublisher{}
	fixture := newBridgeFixture(t, nil, publisher)
	run := createRecoveryRun(t, fixture, workflows.RunRunning, 33)
	result := startRecoveryFlow(t, fixture, run)
	if result.Run.Status != flow.RunCompleted {
		t.Fatalf("seed flow status = %s, want completed", result.Run.Status)
	}
	assertRecoveredStatus(t, fixture, run.ID, workflows.RunRunning)

	supervisor := fixture.supervisor(t)
	defer func() { _ = supervisor.Shutdown(context.Background()) }()
	if err := supervisor.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := assertRecoveredStatus(t, fixture, run.ID, workflows.RunCompleted)
	if got.CheckpointRevision != result.Run.Revision {
		t.Fatalf("recovered checkpoint revision = %d, want terminal revision %d", got.CheckpointRevision, result.Run.Revision)
	}
	if kinds := activityKinds(publisher.snapshot()); len(kinds) != 4 || kinds[0] != "run_started" || kinds[len(kinds)-1] != "run_completed" {
		t.Fatalf("recovered activity sequence = %v, want complete projected history", kinds)
	}
}

func TestBridgeRecoveryAfterFlowAppendBeforeRegistryTransition(t *testing.T) {
	publisher := &bridgePublisher{}
	fixture := newBridgeFixture(t, nil, publisher)
	input, err := fixture.inputs.Put(context.Background(), fixture.session, []byte(`{"count":0}`))
	if err != nil {
		t.Fatal(err)
	}
	graphRunID := flow.GraphRunID(uuid.UUID{15: 22})
	run := workflows.Run{
		SessionID: fixture.session, ToolExecutionID: uuid.UUID{15: 23},
		DefinitionName: "counter_flow", DefinitionVersion: "v1", ID: uuid.UUID{15: 24},
		GraphRunID: graphRunID, Input: input, Status: workflows.RunPending,
		LedgerLocator: "flow/runs/" + graphRunID.String(),
		CreatedAt:     time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC),
		UpdatedAt:     time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC),
	}
	if _, err := fixture.registry.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	definition, err := fixture.catalog.Resolve("counter_flow", "v1")
	if err != nil {
		t.Fatal(err)
	}
	validated, err := definition.ValidateInput(json.RawMessage(`{"count":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definition.Start(context.Background(), validated, flow.WithGraphRunID(graphRunID)); err != nil {
		t.Fatal(err)
	}

	supervisor := fixture.supervisor(t)
	if err := supervisor.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = supervisor.Shutdown(context.Background()) }()
	got, err := fixture.registry.Get(context.Background(), fixture.session, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != workflows.RunCompleted {
		t.Fatalf("repaired run status = %s, want completed", got.Status)
	}
	if kinds := activityKinds(publisher.snapshot()); len(kinds) != 4 || kinds[0] != "run_started" || kinds[len(kinds)-1] != "run_completed" {
		t.Fatalf("repaired activity sequence = %v, want complete projected history", kinds)
	}
}
