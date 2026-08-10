// Package testworkflow contains a small deterministic graph used only by the
// bridge integration and recovery tests.
package testworkflow

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/looprig/flow/pkg/flow"
	bridge "github.com/looprig/workflows"
)

// CounterState is intentionally small so integration tests can assert the
// durable result without exposing application payloads through activities.
type CounterState struct {
	Count int `json:"count"`
}

var (
	counterGraph = flow.GraphID{1}
	firstVertex  = flow.VertexID{2}
	secondVertex = flow.VertexID{3}
)

// NewDefinition builds the two-vertex counter over the caller's neutral Flow
// checkpoint store. The graph and vertex IDs are stable across restarts.
func NewDefinition(store flow.CheckpointStore) (*bridge.TypedDefinition[CounterState], error) {
	return NewDefinitionWithGate(store, nil)
}

// NewDefinitionWithGate is the same graph with an optional test-only gate
// before each vertex. It lets the integration test prove that workflow_start
// returns after the durable seed rather than after graph completion.
func NewDefinitionWithGate(store flow.CheckpointStore, gate <-chan struct{}) (*bridge.TypedDefinition[CounterState], error) {
	if store == nil {
		return nil, &bridge.InvalidSchemaError{Field: "checkpoint store", Err: errors.New("store is required")}
	}
	graph := flow.NewGraph[CounterState](counterGraph)
	task := flow.NewFuncTask(func(ctx context.Context, _ int) (int, error) {
		if gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		return 1, nil
	})
	if err := flow.AddVertex(graph, firstVertex, task,
		func(state CounterState) int { return state.Count },
		func(state *CounterState, output int) error { state.Count += output; return nil }); err != nil {
		return nil, err
	}
	if err := flow.AddVertex(graph, secondVertex, task,
		func(state CounterState) int { return state.Count },
		func(state *CounterState, output int) error { state.Count += output; return nil }); err != nil {
		return nil, err
	}
	if err := graph.AddEdge(firstVertex, secondVertex); err != nil {
		return nil, err
	}
	runner, err := graph.Compile(firstVertex, secondVertex, flow.WithStore(store))
	if err != nil {
		return nil, err
	}
	metadata, err := bridge.NewMetadata(
		"counter_flow", "v1", "Deterministic two-step counter.",
		json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer","minimum":0}},"required":["count"],"additionalProperties":false}`), nil,
		[]bridge.VertexMetadata{
			bridge.NewVertexMetadataForID(firstVertex, "increment one"),
			bridge.NewVertexMetadataForID(secondVertex, "increment two"),
		},
	)
	if err != nil {
		return nil, err
	}
	return bridge.NewTypedDefinition(metadata, runner, store, bridge.StrictJSONDecoder[CounterState], nil,
		func(state CounterState) string { return "counter completed" })
}
