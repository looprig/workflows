package workflows_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/workflows"
)

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
