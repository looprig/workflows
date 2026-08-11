package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/workflows"
)

type counter struct {
	Count int `json:"count"`
}

type increment struct {
	By int `json:"by"`
}

var (
	graphID  = flow.GraphID{18}
	vertexID = flow.VertexID{1}
)

func main() {
	ctx := context.Background()
	checkpoints := flow.NewMemStore()
	definition := mustDefinition(checkpoints)

	input, err := definition.ValidateInput(json.RawMessage(`{"count":1}`))
	must(err)
	started, err := definition.Start(ctx, input, flow.WithGraphRunID(flow.GraphRunID{18, 1}))
	must(err)
	fmt.Println("started:", started.Run.Status)
	fmt.Println("interrupt:", started.Interrupts[0].Info)

	// A process restart rebuilds code, but reconnects it to the same checkpoint
	// store. Get proves the interrupted state survived without re-running work.
	recoveredDefinition := mustDefinition(checkpoints)
	recovered, err := recoveredDefinition.Get(ctx, started.Run.GraphRunID)
	must(err)
	fmt.Println("recovered:", recovered.Run.Status)

	resume, err := recoveredDefinition.ValidateResume(json.RawMessage(`{"by":2}`))
	must(err)
	resumed, err := recoveredDefinition.Resume(ctx, started.Run.GraphRunID, resume)
	must(err)
	var completed counter
	must(json.Unmarshal(resumed.State, &completed))
	fmt.Printf("resumed: %s count=%d\n", resumed.Run.Status, completed.Count)

	cancelInput, err := recoveredDefinition.ValidateInput(json.RawMessage(`{"count":10}`))
	must(err)
	toCancel, err := recoveredDefinition.Start(ctx, cancelInput, flow.WithGraphRunID(flow.GraphRunID{18, 2}))
	must(err)
	must(recoveredDefinition.Cancel(ctx, toCancel.Run.GraphRunID, "operator request"))
	cancelled, err := recoveredDefinition.Get(ctx, toCancel.Run.GraphRunID)
	must(err)
	fmt.Println("cancelled:", cancelled.Run.Status)

	history, err := recoveredDefinition.History(ctx, started.Run.GraphRunID)
	must(err)
	for index, state := range history {
		if state.Revision != uint64(index) {
			panic("checkpoint history is not contiguous")
		}
	}
	fmt.Println("history: append-only")
}

func mustDefinition(store flow.CheckpointStore) workflows.Definition {
	graph := flow.NewGraph[counter](graphID)
	task := flow.NewFuncTask(func(ctx context.Context, count int) (int, error) {
		payload, ok := flow.ResumePayload[increment](ctx)
		if !ok {
			return 0, flow.Interrupt(ctx, "awaiting increment")
		}
		return count + payload.By, nil
	})
	must(flow.AddVertex(
		graph,
		vertexID,
		task,
		func(state counter) int { return state.Count },
		func(state *counter, result int) error {
			state.Count = result
			return nil
		},
	))
	runner, err := graph.Compile(vertexID, vertexID, flow.WithStore(store))
	must(err)
	metadata, err := workflows.NewMetadata(
		"docs_counter",
		"v1",
		"A deterministic interrupt and resume example.",
		json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false}`),
		json.RawMessage(`{"type":"object","properties":{"by":{"type":"integer","minimum":1}},"required":["by"],"additionalProperties":false}`),
		[]workflows.VertexMetadata{workflows.NewVertexMetadataForID(vertexID, "apply increment")},
	)
	must(err)
	typed, err := workflows.NewTypedDefinition(
		metadata,
		runner,
		store,
		workflows.StrictJSONDecoder[counter],
		workflows.TypedResumeDecoder(workflows.StrictJSONDecoder[increment]),
		func(state counter) string { return fmt.Sprintf("counter=%d", state.Count) },
	)
	must(err)
	catalog := workflows.NewCatalog()
	must(catalog.Register(typed))
	definition, err := catalog.Resolve("docs_counter", "v1")
	must(err)
	return definition
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
