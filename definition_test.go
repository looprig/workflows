package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/looprig/flow/pkg/flow"
)

type counterState struct {
	Count int `json:"count"`
}

type counterResume struct {
	Increment int `json:"increment"`
}

func counterDefinition(t *testing.T, name, version string) (*TypedDefinition[counterState], *flow.MemStore) {
	t.Helper()

	store := flow.NewMemStore()
	graph := flow.NewGraph[counterState](flow.GraphID{1})
	vertex := flow.VertexID{2}
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	if err := flow.AddVertex(graph, vertex, task,
		func(s counterState) int { return s.Count },
		func(s *counterState, out int) error { s.Count = out; return nil }); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	runner, err := graph.Compile(vertex, vertex, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	meta, err := NewMetadata(
		name,
		version,
		"Increment a counter.",
		json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer","minimum":0}},"required":["count"],"additionalProperties":false}`),
		json.RawMessage(`{"type":"object","properties":{"increment":{"type":"integer","minimum":1}},"required":["increment"],"additionalProperties":false}`),
		[]VertexMetadata{NewVertexMetadata("increment")},
	)
	if err != nil {
		t.Fatalf("NewMetadata: %v", err)
	}
	def, err := NewTypedDefinition(
		meta,
		runner,
		store,
		StrictJSONDecoder[counterState],
		TypedResumeDecoder(StrictJSONDecoder[counterResume]),
		func(s counterState) string { return strings.Repeat("x", s.Count) },
	)
	if err != nil {
		t.Fatalf("NewTypedDefinition: %v", err)
	}
	return def, store
}

func registeredCounterDefinition(t *testing.T) Definition {
	t.Helper()
	def, _ := counterDefinition(t, "counter_flow", "v1")
	catalog := NewCatalog()
	if err := catalog.Register(def); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := catalog.Resolve("counter_flow", "v1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return got
}

func TestDefinitionValidationRetainsTypedStateAndGatesExecution(t *testing.T) {
	def := registeredCounterDefinition(t)

	if _, err := def.Start(context.Background(), ValidatedInput{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Start(unvalidated) error = %v, want ErrInvalidInput", err)
	}
	if _, err := def.ValidateInput(json.RawMessage(`{"count":1,"unknown":true}`)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ValidateInput(unknown field) error = %v, want ErrInvalidInput", err)
	}
	input, err := def.ValidateInput(json.RawMessage(`{"count":1}`))
	if err != nil {
		t.Fatalf("ValidateInput: %v", err)
	}
	result, err := def.Start(context.Background(), input)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	var state counterState
	if err := json.Unmarshal(result.State, &state); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if state.Count != 2 {
		t.Fatalf("typed state count = %d, want 2", state.Count)
	}
	if len(result.Summary) != 2 {
		t.Fatalf("summary length = %d, want 2", len(result.Summary))
	}
	history, err := def.History(context.Background(), result.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) == 0 || history[len(history)-1].GraphRunID != result.Run.GraphRunID {
		t.Fatalf("History returned the wrong run: %#v", history)
	}
}

func TestDefinitionResumeValidationIsStrict(t *testing.T) {
	def := registeredCounterDefinition(t)
	if _, err := def.ValidateResume(json.RawMessage(`{"increment":1,"extra":2}`)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ValidateResume(unknown field) error = %v, want ErrInvalidInput", err)
	}
}

func TestDefinitionValidationTokenCannotCrossDefinitions(t *testing.T) {
	first := registeredCounterDefinition(t)
	secondDefinition, _ := counterDefinition(t, "second_counter", "v1")
	catalog := NewCatalog()
	if err := catalog.Register(secondDefinition); err != nil {
		t.Fatalf("Register second: %v", err)
	}
	second, err := catalog.Resolve("second_counter", "v1")
	if err != nil {
		t.Fatalf("Resolve second: %v", err)
	}
	token, err := first.ValidateInput(json.RawMessage(`{"count":1}`))
	if err != nil {
		t.Fatalf("ValidateInput: %v", err)
	}
	if _, err := second.Start(context.Background(), token); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Start(foreign token) error = %v, want ErrInvalidInput", err)
	}
}

func TestDefinitionMetadataAndSchemasAreDefensiveCopies(t *testing.T) {
	inputSchema := json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)
	vertices := []VertexMetadata{NewVertexMetadata("only")}
	meta, err := NewMetadata("copy_test", "v1", "safe", inputSchema, inputSchema, vertices)
	if err != nil {
		t.Fatalf("NewMetadata: %v", err)
	}
	inputSchema[0] = '['
	vertices[0] = NewVertexMetadata("changed")

	first := meta.InputSchema()
	first[0] = '['
	gotVertices := meta.Vertices()
	gotVertices[0] = NewVertexMetadata("also_changed")
	if string(meta.InputSchema()) != `{"type":"object","properties":{},"additionalProperties":false}` {
		t.Fatalf("metadata input schema was mutated: %s", meta.InputSchema())
	}
	if meta.Vertices()[0].Label() != "only" {
		t.Fatalf("metadata vertices were mutated: %q", meta.Vertices()[0].Label())
	}
}

func TestDefinitionStatusSummaryIsBounded(t *testing.T) {
	def := registeredCounterDefinition(t)
	input, err := def.ValidateInput(json.RawMessage(`{"count":5000}`))
	if err != nil {
		t.Fatalf("ValidateInput: %v", err)
	}
	result, err := def.Start(context.Background(), input)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(result.Summary) != MaxStatusSummaryBytes {
		t.Fatalf("summary length = %d, want bound %d", len(result.Summary), MaxStatusSummaryBytes)
	}
}
