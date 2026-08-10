package workflows

import (
	"context"
	"errors"

	"github.com/looprig/flow/pkg/flow"
)

func (c *runController) adopt(ctx context.Context, definition Definition, run *Run) error {
	history, err := definition.History(ctx, run.GraphRunID)
	if err != nil {
		return c.handleCheckpointReadError(ctx, run, "load history", err)
	}
	return c.adoptWithHistory(ctx, definition, run, history)
}

func (c *runController) adoptWithHistory(ctx context.Context, definition Definition, run *Run, history []flow.GraphRunState) error {
	if len(history) == 0 {
		return c.failDefinite(ctx, run, "workflow checkpoint missing")
	}
	result, err := definition.Get(ctx, run.GraphRunID)
	if err != nil {
		return c.handleCheckpointReadError(ctx, run, "reconstruct checkpoint", err)
	}
	return c.applyResult(ctx, run, result)
}

func (c *runController) handleCheckpointReadError(ctx context.Context, run *Run, op string, err error) error {
	if ctx.Err() != nil || c.supervisor.ownershipLost() {
		return nil
	}
	if !isDefiniteCheckpointError(err) {
		return &AdoptionError{RunID: run.ID, Op: op, Err: err}
	}
	if failErr := c.failDefinite(ctx, run, "workflow checkpoint unavailable"); failErr != nil {
		return failErr
	}
	return nil
}

func isDefiniteCheckpointError(err error) bool {
	var missing *flow.CheckpointNotFoundError
	var decode *flow.CheckpointDecodeError
	var graph *flow.GraphMismatchError
	var version *flow.GraphVersionMismatchError
	var identity *flow.GraphRunMismatchError
	return errors.As(err, &missing) || errors.As(err, &decode) || errors.As(err, &graph) || errors.As(err, &version) || errors.As(err, &identity)
}
