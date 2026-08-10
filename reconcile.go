package workflows

import (
	"context"
	"errors"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
)

type checkpointHistoryDefinition interface {
	workflowCheckpointHistory(context.Context, flow.GraphRunID) ([]*flow.Checkpoint, error)
}

// workflowCheckpointHistory retains full framework checkpoint metadata for the
// internal projector while Definition.History remains the narrow public API.
func (d *TypedDefinition[S]) workflowCheckpointHistory(ctx context.Context, id flow.GraphRunID) ([]*flow.Checkpoint, error) {
	return d.store.History(ctx, id)
}

func loadWorkflowCheckpointHistory(ctx context.Context, definition Definition, id flow.GraphRunID) ([]*flow.Checkpoint, error) {
	if provider, ok := definition.(checkpointHistoryDefinition); ok {
		return provider.workflowCheckpointHistory(ctx, id)
	}
	states, err := definition.History(ctx, id)
	if err != nil {
		return nil, err
	}
	history := make([]*flow.Checkpoint, len(states))
	for i := range states {
		// The public Task 8 history view intentionally omits vertex records and
		// may be a latest-only test adapter. Such a view cannot prove a complete
		// durable projection, so leave it unreconciled instead of inventing events.
		if states[i].Revision != uint64(i) || (states[i].CreatedAt.IsZero() && states[i].UpdatedAt.IsZero()) {
			return nil, nil
		}
		history[i] = &flow.Checkpoint{Run: states[i]}
	}
	return history, nil
}

func reconcileDefinitionActivities(ctx context.Context, registry *RunRegistry, publisher tool.WorkflowActivityPublisher, definition Definition, run *Run) (*Run, error) {
	if definition == nil {
		return nil, &ReconciliationError{RunID: run.ID, Op: "resolve history", Err: errors.New("definition unavailable")}
	}
	if _, ok := definition.(checkpointHistoryDefinition); !ok && run.Status != RunFailed {
		// A run-state-only Definition cannot prove vertex transitions. Existing
		// adapters remain compatible, but durable activity requires the full
		// checkpoint capability supplied by TypedDefinition.
		return run, nil
	}
	history, err := loadWorkflowCheckpointHistory(ctx, definition, run.GraphRunID)
	if err != nil {
		var missing *flow.CheckpointNotFoundError
		if errors.As(err, &missing) && run.Status == RunFailed {
			history = nil
		} else {
			return nil, &ReconciliationError{RunID: run.ID, Op: "read checkpoint history", Err: err}
		}
	}
	return reconcileActivityHistory(ctx, registry, publisher, definition.Metadata(), run, history)
}

// reconcileActivityHistory treats ActivityCursor as the number of contiguous
// checkpoint revisions fully published. It advances the cursor only after every
// activity for that checkpoint returns nil from the durable publisher. A nil
// duplicate result therefore advances the cursor without requiring a journal API.
func reconcileActivityHistory(ctx context.Context, registry *RunRegistry, publisher tool.WorkflowActivityPublisher, metadata Metadata, run *Run, history []*flow.Checkpoint) (*Run, error) {
	if registry == nil || publisher == nil || run == nil {
		return nil, &ReconciliationError{Op: "validate dependencies", Err: errors.New("registry, publisher, and run are required")}
	}
	projected, err := projectActivityHistory(run, metadata, history)
	if err != nil {
		return nil, &ReconciliationError{RunID: run.ID, Op: "project durable history", Err: err}
	}
	maxCursor := uint64(len(history))
	for _, activity := range projected {
		if activity.revision >= maxCursor {
			maxCursor = activity.revision + 1
		}
	}
	if run.ActivityCursor > maxCursor {
		return nil, &ReconciliationError{RunID: run.ID, Op: "validate cursor", Err: errors.New("cursor exceeds history")}
	}
	current := cloneRun(*run)
	for revision := current.ActivityCursor; revision < maxCursor; revision++ {
		for _, activity := range projected {
			if activity.revision != revision {
				continue
			}
			if err := publisher.PublishWorkflowActivity(ctx, activity.body); err != nil {
				return nil, &ReconciliationError{RunID: run.ID, Op: "publish durable activity", Err: err}
			}
		}
		next := cloneRun(current)
		next.ActivityCursor = revision + 1
		updated, err := registry.CompareAndSwap(ctx, current.Revision, next)
		if err != nil {
			return nil, &ReconciliationError{RunID: run.ID, Op: "advance activity cursor", Err: err}
		}
		current = *updated
	}
	return &current, nil
}
