package workflows

import (
	"context"

	"github.com/looprig/flow/pkg/flow"
)

// workflowHooks only wakes reconciliation after Flow confirms a durable append.
// It deliberately leaves run/vertex completion hooks nil because those callbacks
// can observe state before the corresponding checkpoint is durable.
func workflowHooks(wake func(flow.GraphRunID, uint64)) flow.Hooks {
	if wake == nil {
		return flow.Hooks{}
	}
	return flow.Hooks{OnCheckpoint: func(_ context.Context, id flow.GraphRunID, revision uint64, _ flow.StepID) {
		wake(id, revision)
	}}
}
