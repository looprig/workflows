package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

type runController struct {
	supervisor *Supervisor
	id         uuid.UUID
	mu         sync.Mutex
}

func (c *runController) reconcile(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	run, err := c.supervisor.registry.Get(ctx, c.supervisor.sessionID, c.id)
	if err != nil {
		return err
	}
	definition, err := c.supervisor.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return &AdoptionError{RunID: run.ID, Op: "resolve definition", Err: err}
	}
	switch run.Status {
	case RunPending:
		history, historyErr := definition.History(ctx, run.GraphRunID)
		if historyErr != nil {
			var missing *flow.CheckpointNotFoundError
			if errors.As(historyErr, &missing) {
				return c.start(ctx, definition, run)
			}
			return c.handleCheckpointReadError(ctx, run, "load pending history", historyErr)
		}
		if len(history) == 0 {
			return c.start(ctx, definition, run)
		}
		// A checkpoint can be durable before the registry's first status
		// transition is acknowledged. Promote the registry through running so
		// the normal lifecycle rules can repair it from checkpoint truth.
		running, transitionErr := c.transition(ctx, run, RunRunning, "workflow adopting", history[len(history)-1].Revision)
		if errors.Is(transitionErr, errSupervisorOwnershipLost) {
			return nil
		}
		if transitionErr != nil {
			return transitionErr
		}
		return c.adoptWithHistory(ctx, definition, running, history)
	case RunRunning, RunInterrupted:
		return c.adopt(ctx, definition, run)
	case RunCompleted, RunCancelled:
		return nil
	case RunFailed:
		_, reconcileErr := c.reconcileActivities(ctx, definition, run)
		return reconcileErr
	default:
		return &AdoptionError{RunID: run.ID, Op: "validate status", Err: errors.New("invalid status")}
	}
}

func (c *runController) start(ctx context.Context, definition Definition, run *Run) error {
	raw, err := c.supervisor.inputs.Get(ctx, run.SessionID, run.Input)
	if err != nil {
		return c.failDefinite(ctx, run, "workflow input unavailable")
	}
	input, err := definition.ValidateInput(raw)
	if err != nil {
		return c.failDefinite(ctx, run, "workflow input invalid")
	}
	running, err := c.transition(ctx, run, RunRunning, "workflow running", run.CheckpointRevision)
	if err != nil {
		if errors.Is(err, errSupervisorOwnershipLost) {
			return nil
		}
		return err
	}
	result, err := definition.Start(ctx, input, flow.WithGraphRunID(run.GraphRunID))
	if err != nil {
		if ctx.Err() != nil || c.supervisor.ownershipLost() {
			return nil
		}
		return &AdoptionError{RunID: run.ID, Op: "start", Err: err}
	}
	return c.applyResult(ctx, running, result)
}

func (c *runController) resume(ctx context.Context, payload json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	opCtx, cancel, err := c.supervisor.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	ctx = opCtx
	run, err := c.supervisor.registry.Get(ctx, c.supervisor.sessionID, c.id)
	if err != nil {
		return err
	}
	if run.Status != RunInterrupted {
		return &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: run.Revision, Actual: run.Revision, Reason: "run is not interrupted"}
	}
	definition, err := c.supervisor.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return err
	}
	validated, err := definition.ValidateResume(payload)
	if err != nil {
		return err
	}
	running, err := c.transition(ctx, run, RunRunning, "workflow resuming", run.CheckpointRevision)
	if err != nil {
		if errors.Is(err, errSupervisorOwnershipLost) {
			return nil
		}
		return err
	}
	result, err := definition.Resume(ctx, run.GraphRunID, validated)
	if err != nil {
		if ctx.Err() != nil || c.supervisor.ownershipLost() {
			return nil
		}
		if isDefiniteCheckpointError(err) {
			return c.handleCheckpointReadError(ctx, running, "resume", err)
		}
		return &AdoptionError{RunID: run.ID, Op: "resume", Err: err}
	}
	return c.applyResult(ctx, running, result)
}

func (c *runController) cancelRun(ctx context.Context, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	opCtx, cancel, err := c.supervisor.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	ctx = opCtx
	run, err := c.supervisor.registry.Get(ctx, c.supervisor.sessionID, c.id)
	if err != nil {
		return err
	}
	if run.Status == RunCompleted || run.Status == RunCancelled || run.Status == RunFailed {
		return &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: run.Revision, Actual: run.Revision, Reason: "run is terminal"}
	}
	if run.Status != RunPending {
		definition, resolveErr := c.supervisor.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
		if resolveErr != nil {
			return resolveErr
		}
		if err := definition.Cancel(ctx, run.GraphRunID, boundSummary(reason)); err != nil {
			return &AdoptionError{RunID: run.ID, Op: "cancel", Err: err}
		}
		reconciled, reconcileErr := c.reconcileActivities(ctx, definition, run)
		if reconcileErr != nil {
			return reconcileErr
		}
		run = reconciled
	}
	_, err = c.transition(ctx, run, RunCancelled, "workflow cancelled", run.CheckpointRevision)
	if errors.Is(err, errSupervisorOwnershipLost) {
		return nil
	}
	return err
}

func (c *runController) applyResult(ctx context.Context, run *Run, result *Result) error {
	if ctx.Err() != nil || c.supervisor.ownershipLost() {
		return nil
	}
	if result == nil {
		return &AdoptionError{RunID: run.ID, Op: "apply result", Err: errors.New("nil result")}
	}
	if result.Run.GraphRunID != (flow.GraphRunID{}) && result.Run.GraphRunID != run.GraphRunID {
		return c.failDefinite(ctx, run, "workflow checkpoint identity mismatch")
	}
	definition, err := c.supervisor.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return &AdoptionError{RunID: run.ID, Op: "resolve activity definition", Err: err}
	}
	reconciled, err := c.reconcileActivities(ctx, definition, run)
	if err != nil {
		return err
	}
	run = reconciled
	var status RunStatus
	switch result.Run.Status {
	case flow.RunRunning:
		status = RunRunning
	case flow.RunInterrupted:
		status = RunInterrupted
	case flow.RunCompleted:
		status = RunCompleted
	case flow.RunCancelled:
		status = RunCancelled
	default:
		return c.failDefinite(ctx, run, "workflow checkpoint status invalid")
	}
	summary := boundSummary(result.Summary)
	if summary == "" {
		summary = "workflow " + string(status)
	}
	_, err = c.transition(ctx, run, status, summary, result.Run.Revision)
	return err
}

func (c *runController) reconcileActivities(ctx context.Context, definition Definition, run *Run) (*Run, error) {
	if c.supervisor.activity == nil {
		return run, nil
	}
	return reconcileDefinitionActivities(ctx, c.supervisor.registry, c.supervisor.activity, definition, run)
}

func (c *runController) transition(ctx context.Context, run *Run, status RunStatus, summary string, checkpoint uint64) (*Run, error) {
	if c.supervisor.ownershipLost() {
		return nil, errSupervisorOwnershipLost
	}
	next := cloneRun(*run)
	next.Status = status
	next.StatusSummary = boundSummary(summary)
	next.CheckpointRevision = checkpoint
	next.UpdatedAt = c.supervisor.now().UTC()
	updated, err := c.supervisor.registry.CompareAndSwap(ctx, run.Revision, next)
	if err != nil {
		return nil, fmt.Errorf("workflows: persist run transition: %w", err)
	}
	return updated, nil
}

func (c *runController) failDefinite(ctx context.Context, run *Run, summary string) error {
	if ctx.Err() != nil || c.supervisor.ownershipLost() {
		return nil
	}
	failed, err := c.transition(ctx, run, RunFailed, summary, run.CheckpointRevision)
	if err != nil || c.supervisor.activity == nil {
		return err
	}
	definition, err := c.supervisor.catalog.Resolve(failed.DefinitionName, failed.DefinitionVersion)
	if err != nil {
		return &AdoptionError{RunID: failed.ID, Op: "resolve failure activity definition", Err: err}
	}
	_, err = reconcileActivityHistory(ctx, c.supervisor.registry, c.supervisor.activity, definition.Metadata(), failed, nil)
	return err
}
