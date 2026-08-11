package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/workflows"
)

func TestRunStartRejectsPrepareRunMutationBeforeCreate(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		mutate func(*workflows.Run)
	}{
		{name: "session ID", field: "session_id", mutate: func(run *workflows.Run) { run.SessionID = testID(40) }},
		{name: "tool execution ID", field: "tool_execution_id", mutate: func(run *workflows.Run) { run.ToolExecutionID = testID(41) }},
		{name: "definition name", field: "definition_name", mutate: func(run *workflows.Run) { run.DefinitionName = "other_flow" }},
		{name: "definition version", field: "definition_version", mutate: func(run *workflows.Run) { run.DefinitionVersion = "v2" }},
		{name: "run ID", field: "id", mutate: func(run *workflows.Run) { run.ID = testID(42) }},
		{name: "graph run ID", field: "graph_run_id", mutate: func(run *workflows.Run) { run.GraphRunID = flow.GraphRunID(testID(43)) }},
		{name: "parent run ID", field: "parent_run_id", mutate: func(run *workflows.Run) { run.ParentRunID = testID(44) }},
		{name: "input", field: "input", mutate: func(run *workflows.Run) { run.Input.Key = "rewritten-input" }},
		{name: "status", field: "status", mutate: func(run *workflows.Run) { run.Status = workflows.RunRunning }},
		{name: "status summary", field: "status_summary", mutate: func(run *workflows.Run) { run.StatusSummary = "rewritten" }},
		{name: "cancel requested", field: "cancel_requested", mutate: func(run *workflows.Run) { run.CancelRequested = true }},
		{name: "checkpoint revision", field: "checkpoint_revision", mutate: func(run *workflows.Run) { run.CheckpointRevision = 1 }},
		{name: "activity cursor", field: "activity_cursor", mutate: func(run *workflows.Run) { run.ActivityCursor = 1 }},
		{name: "ledger locator", field: "ledger_locator", mutate: func(run *workflows.Run) { run.LedgerLocator = "rewritten-ledger" }},
		{name: "artifacts", field: "artifacts", mutate: func(run *workflows.Run) { run.Artifacts = []workflows.ArtifactReference{{ID: "rewritten"}} }},
		{name: "created at", field: "created_at", mutate: func(run *workflows.Run) { run.CreatedAt = run.CreatedAt.Add(time.Second) }},
		{name: "updated at", field: "updated_at", mutate: func(run *workflows.Run) { run.UpdatedAt = run.UpdatedAt.Add(time.Second) }},
		{name: "revision", field: "revision", mutate: func(run *workflows.Run) { run.Revision = 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			createCalls := 0
			registry := registryStub{create: func(context.Context, workflows.Run) (*workflows.Run, error) {
				createCalls++
				return nil, errors.New("registry create must not be called")
			}}
			bundle, err := NewBundle(Config{
				SessionID:  testID(20),
				Catalog:    prepareRunTestCatalog(t),
				Registry:   registry,
				Inputs:     inputStoreStub{},
				Supervisor: &controlStub{},
				NewID:      prepareRunTestIDs(),
				PrepareRun: func(_ context.Context, run workflows.Run) (workflows.Run, error) {
					test.mutate(&run)
					return run, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			_, err = bundle[1].InvokableRun(context.Background(), `{"definition_name":"prepare_flow","definition_version":"v1","input":{}}`)
			if err == nil {
				t.Fatal("InvokableRun() error = nil, want PrepareRun integrity error")
			}
			var integrityErr *PrepareRunIntegrityError
			if !errors.As(err, &integrityErr) {
				t.Fatalf("InvokableRun() error = %v, want PrepareRunIntegrityError", err)
			}
			if integrityErr.Field != test.field {
				t.Fatalf("integrity error field = %q, want %q", integrityErr.Field, test.field)
			}
			if createCalls != 0 {
				t.Fatalf("registry Create calls = %d, want 0", createCalls)
			}
		})
	}
}

func TestRunStartPermitsPrepareRunArtifactDescriptorMutation(t *testing.T) {
	var created *workflows.Run
	registry := registryStub{
		create: func(_ context.Context, run workflows.Run) (*workflows.Run, error) {
			copy := run
			created = &copy
			return &copy, nil
		},
		get: func(_ context.Context, _ uuid.UUID, _ uuid.UUID) (*workflows.Run, error) {
			if created == nil {
				t.Fatal("registry Get called before Create")
			}
			return created, nil
		},
	}
	bundle, err := NewBundle(Config{
		SessionID:  testID(20),
		Catalog:    prepareRunTestCatalog(t),
		Registry:   registry,
		Inputs:     inputStoreStub{},
		Supervisor: &controlStub{},
		NewID:      prepareRunTestIDs(),
		PrepareRun: func(_ context.Context, run workflows.Run) (workflows.Run, error) {
			run.ArtifactSessionID = testID(50)
			run.ArtifactRunID = testID(51)
			run.ArtifactInputKind = workflows.ArtifactInputParent
			run.ArtifactInputSessionID = testID(52)
			run.ArtifactInputRunID = testID(53)
			return run, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bundle[1].InvokableRun(context.Background(), `{"definition_name":"prepare_flow","definition_version":"v1","input":{}}`); err != nil {
		t.Fatalf("InvokableRun() error = %v", err)
	}
	if created == nil {
		t.Fatal("registry Create did not receive a run")
	}
	if created.ArtifactSessionID != testID(50) || created.ArtifactRunID != testID(51) || created.ArtifactInputKind != workflows.ArtifactInputParent || created.ArtifactInputSessionID != testID(52) || created.ArtifactInputRunID != testID(53) {
		t.Fatalf("created artifact descriptor = %#v", created)
	}
}

func prepareRunTestIDs() func() (uuid.UUID, error) {
	ids := []uuid.UUID{testID(21), testID(22), testID(23)}
	return func() (uuid.UUID, error) {
		id := ids[0]
		ids = ids[1:]
		return id, nil
	}
}

func prepareRunTestCatalog(t *testing.T) *workflows.Catalog {
	t.Helper()
	store := flow.NewMemStore()
	graph := flow.NewGraph[map[string]int](flow.GraphID{1})
	vertex := flow.VertexID{2}
	task := flow.NewFuncTask(func(_ context.Context, input int) (int, error) { return input, nil })
	if err := flow.AddVertex(graph, vertex, task,
		func(state map[string]int) int { return state["value"] },
		func(state *map[string]int, output int) error { (*state)["value"] = output; return nil }); err != nil {
		t.Fatal(err)
	}
	runner, err := graph.Compile(vertex, vertex, flow.WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := workflows.NewMetadata("prepare_flow", "v1", "PrepareRun test flow.", json.RawMessage(`{"type":"object","additionalProperties":false}`), nil, []workflows.VertexMetadata{workflows.NewVertexMetadata("prepare")})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := workflows.NewTypedDefinition(metadata, runner, store, workflows.StrictJSONDecoder[map[string]int], nil, func(map[string]int) string { return "running" })
	if err != nil {
		t.Fatal(err)
	}
	catalog := workflows.NewCatalog()
	if err := catalog.Register(definition); err != nil {
		t.Fatal(err)
	}
	return catalog
}
