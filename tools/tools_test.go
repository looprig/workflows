package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/workflows"
)

func TestDefinitionsExposeExactLifecycleNamesAndStrictSchemas(t *testing.T) {
	definitions := toolInfos()
	want := []string{
		"workflow_definition_list",
		"workflow_run_start",
		"workflow_run_get",
		"workflow_run_list",
		"workflow_run_resume",
		"workflow_run_cancel",
		"workflow_run_history",
	}
	if len(definitions) != len(want) {
		t.Fatalf("toolInfos() count = %d, want %d", len(definitions), len(want))
	}
	for i, info := range definitions {
		if info.Name != want[i] {
			t.Errorf("tool %d name = %q, want %q", i, info.Name, want[i])
		}
		if strings.TrimSpace(info.Desc) == "" || len(info.Desc) > maxToolDescriptionBytes {
			t.Errorf("%s description is empty or unbounded", info.Name)
		}
		var schema map[string]any
		if err := json.Unmarshal(info.Schema, &schema); err != nil {
			t.Fatalf("%s schema: %v", info.Name, err)
		}
		if schema["type"] != "object" || schema["additionalProperties"] != false {
			t.Errorf("%s schema is not a closed object: %s", info.Name, info.Schema)
		}
		encoded := string(info.Schema) + info.Name + info.Desc
		for _, forbidden := range []string{"WorkflowStart", "clause", `"name":"status"`} {
			if strings.Contains(encoded, forbidden) {
				t.Errorf("%s exposes forbidden legacy term %q", info.Name, forbidden)
			}
		}
	}
}

func TestDefinitionsBuildDirectSessionBoundBundle(t *testing.T) {
	bundle, err := NewBundle(Config{
		SessionID:  testID(1),
		Catalog:    workflows.NewCatalog(),
		Registry:   registryStub{},
		Inputs:     inputStoreStub{},
		Supervisor: &controlStub{},
	})
	if err != nil {
		t.Fatalf("NewBundle() error = %v", err)
	}
	if len(bundle) != 7 {
		t.Fatalf("NewBundle() count = %d, want 7", len(bundle))
	}
}

func TestRunToolsRejectUnknownFields(t *testing.T) {
	runtime := &boundRuntime{sessionID: testID(1)}
	for _, candidate := range newToolSet(runtime) {
		info, err := candidate.Info(context.Background())
		if err != nil {
			t.Fatalf("Info() error = %v", err)
		}
		_, err = candidate.InvokableRun(context.Background(), `{"unexpected":true}`)
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Errorf("%s unknown-field error = %v", info.Name, err)
		}
	}
}

func TestSafeRunResultOmitsPolicyAndFlowState(t *testing.T) {
	run := workflows.Run{
		SessionID: testID(2), ID: testID(3), ToolExecutionID: testID(4),
		DefinitionName: "source_document_extract", DefinitionVersion: "v1",
		Status: workflows.RunRunning, StatusSummary: "bounded summary",
		Input:     workflows.InputReference{Digest: strings.Repeat("a", 64), Key: "secret-policy-text", Size: 42},
		Artifacts: []workflows.ArtifactReference{{ID: "artifact-1", Kind: "report", Digest: strings.Repeat("b", 64), Size: 7}},
	}
	raw, err := json.Marshal(newRunResult(run))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"secret-policy-text", "input", "flow_state", "stack", "model_output"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("safe result contains %q: %s", forbidden, text)
		}
	}
}

func TestRunStartPersistsCanonicalInputAndReturnsAfterDurableSeed(t *testing.T) {
	backend := memstore.New()
	registry, err := workflows.NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := workflows.NewInputStore(backend.Blobs)
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	graph := flow.NewGraph[map[string]int](flow.GraphID{1})
	vertex := flow.VertexID{2}
	task := flow.NewFuncTask(func(ctx context.Context, input int) (int, error) {
		select {
		case <-release:
			return input + 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	if err := flow.AddVertex(graph, vertex, task,
		func(state map[string]int) int { return state["a"] },
		func(state *map[string]int, output int) error { (*state)["a"] = output; return nil }); err != nil {
		t.Fatal(err)
	}
	store := flow.NewMemStore()
	runner, err := graph.Compile(vertex, vertex, flow.WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	schema := json.RawMessage(`{"type":"object","properties":{"a":{"type":"integer"},"z":{"type":"integer"}},"required":["a","z"],"additionalProperties":false}`)
	metadata, err := workflows.NewMetadata("counter_flow", "v1", "Counter.", schema, nil, []workflows.VertexMetadata{workflows.NewVertexMetadata("increment")})
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

	ids := []uuid.UUID{testID(21), testID(22), testID(23)}
	bundle, err := NewBundle(Config{SessionID: testID(20), Catalog: catalog, Registry: registry, Inputs: inputs, Supervisor: &controlStub{},
		Now:   func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
		NewID: func() (uuid.UUID, error) { id := ids[0]; ids = ids[1:]; return id, nil }})
	if err != nil {
		t.Fatal(err)
	}
	start := bundle[1]
	done := make(chan *tool.ToolResult, 1)
	errs := make(chan error, 1)
	go func() {
		got, runErr := start.InvokableRun(context.Background(), `{"definition_name":"counter_flow","definition_version":"v1","input":{"z":2,"a":1}}`)
		if runErr != nil {
			errs <- runErr
			return
		}
		done <- got
	}()
	select {
	case err := <-errs:
		t.Fatalf("start error = %v", err)
	case got := <-done:
		var started runResult
		decodeToolResult(t, got, &started)
		if started.RunID != testID(21).String() || started.RunStatus != string(workflows.RunRunning) {
			t.Fatalf("start result = %+v", started)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("start waited for workflow completion instead of durable seed")
	}
	run, err := registry.Get(context.Background(), testID(20), testID(21))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := inputs.Get(context.Background(), testID(20), run.Input)
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"a":1,"z":2}` {
		t.Fatalf("canonical input = %s", canonical)
	}
	close(release)
}

func testID(seed byte) uuid.UUID {
	var id uuid.UUID
	id[15] = seed
	return id
}
