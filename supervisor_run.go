package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

const maxCancelAttempts = 8

type runController struct {
	supervisor *Supervisor
	id         uuid.UUID
	mu         sync.Mutex
	execution  atomic.Pointer[runExecution]
}

type runExecution struct {
	once   sync.Once
	cancel context.CancelFunc
}

func (e *runExecution) stop() { e.once.Do(e.cancel) }

func (c *runController) beginExecution(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	execution := &runExecution{cancel: cancel}
	c.execution.Store(execution)
	return ctx, func() {
		c.execution.CompareAndSwap(execution, nil)
		cancel()
	}
}

func (c *runController) signalExecutionCancel() {
	if execution := c.execution.Load(); execution != nil {
		execution.stop()
	}
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
	if run.CancelRequested && run.Status != RunFailed && run.Status != RunCompleted && run.Status != RunCancelled {
		return c.cancelLocked(ctx, run, "cancel requested")
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
	return c.startWithSeed(ctx, definition, run, nil)
}

func (c *runController) startWithSeed(ctx context.Context, definition Definition, run *Run, seed func()) error {
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
	options := []flow.RunOption{flow.WithGraphRunID(run.GraphRunID)}
	if seed != nil {
		options = append(options, flow.WithHooks(flow.Hooks{OnRunStart: func(context.Context, flow.GraphRunState) { seed() }}))
	}
	executionCtx, finishExecution := c.beginExecution(ctx)
	defer finishExecution()
	result, err := definition.Start(executionCtx, input, options...)
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
	executionCtx, finishExecution := c.beginExecution(ctx)
	defer finishExecution()
	result, err := definition.Resume(executionCtx, run.GraphRunID, validated)
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
	// Signal an in-flight Flow execution before waiting for the controller lock;
	// otherwise a long-running vertex could prevent the durable cancel intent
	// from ever reaching Runner.Cancel.
	c.signalExecutionCancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	opCtx, cancel, err := c.supervisor.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return c.cancelLocked(opCtx, nil, reason)
}

// cancelLocked records intent before touching Flow, then reconciles the
// resulting checkpoint back into the registry. The intent makes a cancellation
// recoverable if the owner exits between the registry write and Runner.Cancel.
func (c *runController) cancelLocked(ctx context.Context, requested *Run, reason string) error {
	if requested == nil {
		var err error
		requested, err = c.supervisor.registry.Get(ctx, c.supervisor.sessionID, c.id)
		if err != nil {
			return err
		}
	}
	run := requested
	if run.Status == RunCancelled {
		return nil
	}
	if run.Status == RunCompleted || run.Status == RunFailed {
		return &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: run.Revision, Actual: run.Revision, Reason: "run is terminal"}
	}
	if !run.CancelRequested {
		next := cloneRun(*run)
		next.CancelRequested = true
		next.StatusSummary = "workflow cancellation requested"
		next.UpdatedAt = c.supervisor.now().UTC()
		updated, err := c.supervisor.registry.CompareAndSwap(ctx, run.Revision, next)
		if err != nil {
			return fmt.Errorf("workflows: persist cancellation intent: %w", err)
		}
		run = updated
	}
	if run.Status == RunPending {
		_, err := c.transition(ctx, run, RunCancelled, "workflow cancelled", run.CheckpointRevision)
		if errors.Is(err, errSupervisorOwnershipLost) {
			return nil
		}
		return err
	}

	definition, err := c.supervisor.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return err
	}
	boundedReason := boundSummary(reason)
	if boundedReason == "" {
		boundedReason = "cancel requested"
	}
	for attempt := 0; attempt < maxCancelAttempts; attempt++ {
		if ctx.Err() != nil || c.supervisor.ownershipLost() {
			return nil
		}
		cancelErr := definition.Cancel(ctx, run.GraphRunID, boundedReason)
		if cancelErr == nil {
			result, getErr := definition.Get(ctx, run.GraphRunID)
			if getErr != nil {
				return c.handleCheckpointReadError(ctx, run, "read cancelled checkpoint", getErr)
			}
			if result == nil {
				return &AdoptionError{RunID: run.ID, Op: "read cancelled checkpoint", Err: errors.New("nil checkpoint result")}
			}
			if result.Run.Status == flow.RunCompleted {
				if applyErr := c.applyResult(ctx, run, result); applyErr != nil {
					return applyErr
				}
				return &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: run.Revision, Actual: run.Revision, Reason: "workflow completed before cancellation"}
			}
			return c.applyResult(ctx, run, result)
		}

		var conflict *flow.RevisionConflictError
		if !errors.As(cancelErr, &conflict) {
			// Cancel may lose a race with a terminal append. Re-read the
			// checkpoint before deciding whether this is idempotent success or
			// a different terminal outcome.
			result, getErr := definition.Get(ctx, run.GraphRunID)
			if getErr != nil {
				return &AdoptionError{RunID: run.ID, Op: "cancel", Err: cancelErr}
			}
			if result == nil {
				return &AdoptionError{RunID: run.ID, Op: "cancel", Err: errors.New("nil checkpoint result")}
			}
			if result.Run.Status == flow.RunCancelled {
				return c.applyResult(ctx, run, result)
			}
			if result.Run.Status == flow.RunCompleted {
				if applyErr := c.applyResult(ctx, run, result); applyErr != nil {
					return applyErr
				}
				return &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: run.Revision, Actual: run.Revision, Reason: "workflow completed before cancellation"}
			}
			return &AdoptionError{RunID: run.ID, Op: "cancel", Err: cancelErr}
		}

		latest, getErr := definition.Get(ctx, run.GraphRunID)
		if getErr != nil {
			return c.handleCheckpointReadError(ctx, run, "reread cancellation conflict", getErr)
		}
		if latest == nil {
			return &AdoptionError{RunID: run.ID, Op: "reread cancellation conflict", Err: errors.New("nil checkpoint result")}
		}
		switch latest.Run.Status {
		case flow.RunCancelled:
			return c.applyResult(ctx, run, latest)
		case flow.RunCompleted:
			if applyErr := c.applyResult(ctx, run, latest); applyErr != nil {
				return applyErr
			}
			return &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: run.Revision, Actual: run.Revision, Reason: "workflow completed before cancellation"}
		default:
			// Keep the durable intent and update only the in-memory checkpoint
			// reference; Flow owns the authoritative revision for the retry.
			run.CheckpointRevision = latest.Run.Revision
		}
	}
	return &AdoptionError{RunID: run.ID, Op: "cancel", Err: fmt.Errorf("revision conflict retry limit %d reached", maxCancelAttempts)}
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
