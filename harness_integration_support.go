//go:build harness_integration

package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/looprig/flow/pkg/flow"
)

// HarnessCancellationConflictDefinition is a tagged Harness-only Definition
// used to isolate Supervisor cancellation retry exhaustion while retaining the
// real Harness publisher and journal. It exposes one durable running checkpoint
// for activity projection, then returns a Flow revision conflict from every
// cancellation attempt. The test does not use this definition in production.
type HarnessCancellationConflictDefinition struct {
	metadata     Metadata
	registration *registration
	state        *harnessCancellationConflictState
}

type harnessCancellationConflictState struct {
	mu       sync.Mutex
	attempts int
	revision map[flow.GraphRunID]uint64
}

// NewHarnessCancellationConflictDefinition returns a registered-on-catalog
// test definition with a stable input contract and an always-running durable
// checkpoint. Its Cancel method is intentionally bounded to the Supervisor's
// retry test and is never reachable from an untagged build.
func NewHarnessCancellationConflictDefinition() (*HarnessCancellationConflictDefinition, error) {
	metadata, err := NewMetadata(
		"cancel_conflict_flow", "v1", "Harness cancellation conflict integration definition.",
		json.RawMessage(`{"type":"object","additionalProperties":true}`), nil, nil,
	)
	if err != nil {
		return nil, err
	}
	return &HarnessCancellationConflictDefinition{
		metadata: metadata,
		state:    &harnessCancellationConflictState{revision: make(map[flow.GraphRunID]uint64)},
	}, nil
}

func (d *HarnessCancellationConflictDefinition) Metadata() Metadata { return d.metadata.clone() }

func (d *HarnessCancellationConflictDefinition) registeredCopy() (Definition, error) {
	if d == nil {
		return nil, &InvalidSchemaError{Field: "definition", Err: errors.New("nil definition")}
	}
	copy := *d
	copy.metadata = d.metadata.clone()
	copy.registration = &registration{}
	return &copy, nil
}

func (d *HarnessCancellationConflictDefinition) ValidateInput(json.RawMessage) (ValidatedInput, error) {
	if d == nil || d.registration == nil {
		return ValidatedInput{}, invalidInput("input", errors.New("definition is not registered"))
	}
	return ValidatedInput{owner: d.registration, value: struct{}{}}, nil
}

func (d *HarnessCancellationConflictDefinition) ValidateResume(json.RawMessage) (ValidatedResume, error) {
	return ValidatedResume{}, invalidInput("resume", errors.New("resume is not supported"))
}

func (d *HarnessCancellationConflictDefinition) Start(ctx context.Context, input ValidatedInput, _ ...flow.RunOption) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d == nil || d.registration == nil || input.owner != d.registration {
		return nil, invalidInput("input", errors.New("validation token does not belong to this definition"))
	}
	return &Result{Run: flow.GraphRunState{Status: flow.RunRunning}}, nil
}

func (d *HarnessCancellationConflictDefinition) Resume(context.Context, flow.GraphRunID, ValidatedResume, ...flow.RunOption) (*Result, error) {
	return nil, errors.New("Harness cancellation conflict definition does not support resume")
}

func (d *HarnessCancellationConflictDefinition) Get(ctx context.Context, id flow.GraphRunID) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Result{Run: d.runState(id)}, nil
}

func (d *HarnessCancellationConflictDefinition) History(ctx context.Context, id flow.GraphRunID) ([]flow.GraphRunState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []flow.GraphRunState{d.runState(id)}, nil
}

// workflowCheckpointHistory supplies the full internal checkpoint shape needed
// by the production activity projector. It is deliberately one running
// checkpoint: the test proves publisher and cursor-CAS wiring before exercising
// Supervisor.Cancel's conflict loop, without pretending to implement a second
// Flow engine in the Harness test.
func (d *HarnessCancellationConflictDefinition) workflowCheckpointHistory(ctx context.Context, id flow.GraphRunID) ([]*flow.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []*flow.Checkpoint{{Run: d.runState(id)}}, nil
}

func (d *HarnessCancellationConflictDefinition) Cancel(ctx context.Context, id flow.GraphRunID, _ string, _ ...flow.RunOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.state.mu.Lock()
	d.state.attempts++
	d.state.revision[id]++
	revision := d.state.revision[id]
	d.state.mu.Unlock()
	return &flow.RevisionConflictError{GraphRunID: id, Expected: revision, Actual: revision - 1}
}

func (d *HarnessCancellationConflictDefinition) CancelAttempts() int {
	d.state.mu.Lock()
	defer d.state.mu.Unlock()
	return d.state.attempts
}

func (d *HarnessCancellationConflictDefinition) runState(id flow.GraphRunID) flow.GraphRunState {
	d.state.mu.Lock()
	revision := d.state.revision[id]
	d.state.mu.Unlock()
	now := time.Date(2026, time.August, 10, 17, 0, 0, 0, time.UTC)
	return flow.GraphRunState{GraphRunID: id, Revision: revision, Status: flow.RunRunning, CreatedAt: now, UpdatedAt: now}
}

var _ Definition = (*HarnessCancellationConflictDefinition)(nil)
var _ checkpointHistoryDefinition = (*HarnessCancellationConflictDefinition)(nil)
