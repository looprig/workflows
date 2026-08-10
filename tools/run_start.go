package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/workflows"
)

var runStartInfo = tool.ToolInfo{Name: "workflow_run_start", Desc: "Validate input, durably seed, and asynchronously start one new workflow run.", Schema: json.RawMessage(`{"type":"object","properties":{"definition_name":{"type":"string"},"definition_version":{"type":"string"},"input":{"type":"object"},"parent_run_id":{"type":"string"}},"required":["definition_name","definition_version","input"],"additionalProperties":false}`)}

type runStartTool struct{ *boundRuntime }

func (t *runStartTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runStartInfo.Clone()
	return &clone, nil
}
func (t *runStartTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		DefinitionName    string          `json:"definition_name"`
		DefinitionVersion string          `json:"definition_version"`
		Input             json.RawMessage `json:"input"`
		ParentRunID       string          `json:"parent_run_id"`
	}
	if err := decodeStrict(raw, &args); err != nil {
		return nil, err
	}
	definition, err := t.catalog.Resolve(args.DefinitionName, args.DefinitionVersion)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicalObject(args.Input)
	if err != nil {
		return nil, err
	}
	validated, err := definition.ValidateInput(canonical)
	if err != nil {
		return nil, err
	}
	var parent uuid.UUID
	if args.ParentRunID != "" {
		parent, err = parseID("parent_run_id", args.ParentRunID)
		if err != nil {
			return nil, err
		}
		if _, err = t.registry.Get(ctx, t.sessionID, parent); err != nil {
			return nil, err
		}
	}
	inputRef, err := t.inputs.Put(ctx, t.sessionID, canonical)
	if err != nil {
		return nil, err
	}
	runID, err := t.newID()
	if err != nil || runID.IsZero() {
		return nil, errors.New("workflow tools: mint workflow run ID")
	}
	graphUUID, err := t.newID()
	if err != nil || graphUUID.IsZero() {
		return nil, errors.New("workflow tools: mint graph run ID")
	}
	executionID, err := t.newID()
	if err != nil || executionID.IsZero() {
		return nil, errors.New("workflow tools: mint execution ID")
	}
	now := t.now().UTC()
	run := workflows.Run{SessionID: t.sessionID, ToolExecutionID: executionID, DefinitionName: args.DefinitionName, DefinitionVersion: args.DefinitionVersion,
		ID: runID, GraphRunID: flow.GraphRunID(graphUUID), ParentRunID: parent, Input: inputRef, Status: workflows.RunPending,
		LedgerLocator: "flow/runs/" + graphUUID.String(), CreatedAt: now, UpdatedAt: now}
	created, err := t.registry.Create(ctx, run)
	if err != nil {
		return nil, err
	}
	if starter, ok := t.supervisor.(supervisorStarter); ok {
		seeded, failed, startErr := starter.Start(ctx, created.ID)
		if startErr != nil {
			return nil, startErr
		}
		select {
		case <-seeded:
			current, getErr := t.registry.Get(ctx, t.sessionID, runID)
			if getErr != nil {
				return nil, getErr
			}
			return result(newRunResult(*current))
		case startErr := <-failed:
			return nil, startErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	seeded := make(chan struct{}, 1)
	failed := make(chan error, 1)
	go t.run(ctx, created, definition, validated, seeded, failed)
	select {
	case <-seeded:
		current, getErr := t.registry.Get(ctx, t.sessionID, runID)
		if getErr != nil {
			return nil, getErr
		}
		return result(newRunResult(*current))
	case err := <-failed:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *runStartTool) run(parent context.Context, run *workflows.Run, definition workflows.Definition, validated workflows.ValidatedInput, seeded chan<- struct{}, failed chan<- error) {
	ctx := context.WithoutCancel(parent)
	running := *run
	running.Status = workflows.RunRunning
	running.StatusSummary = "workflow starting"
	running.UpdatedAt = t.now().UTC()
	current, err := t.registry.CompareAndSwap(ctx, run.Revision, running)
	if err != nil {
		failed <- err
		return
	}
	hooks := flow.Hooks{OnRunStart: func(context.Context, flow.GraphRunState) {
		select {
		case seeded <- struct{}{}:
		default:
		}
	}}
	outcome, err := definition.Start(ctx, validated, flow.WithGraphRunID(run.GraphRunID), flow.WithHooks(hooks))
	if err != nil {
		t.failStart(ctx, current, "workflow seed failed")
		select {
		case failed <- fmt.Errorf("workflow tools: start failed: %w", err):
		default:
		}
		return
	}
	latest, getErr := t.registry.Get(ctx, t.sessionID, run.ID)
	if getErr != nil {
		return
	}
	next := *latest
	next.CheckpointRevision = outcome.Run.Revision
	next.StatusSummary = outcome.Summary
	next.UpdatedAt = t.now().UTC()
	switch outcome.Run.Status {
	case flow.RunInterrupted:
		next.Status = workflows.RunInterrupted
	case flow.RunCompleted:
		next.Status = workflows.RunCompleted
	case flow.RunCancelled:
		next.Status = workflows.RunCancelled
	default:
		next.Status = workflows.RunRunning
	}
	_, _ = t.registry.CompareAndSwap(ctx, latest.Revision, next)
}

func (t *runStartTool) failStart(ctx context.Context, run *workflows.Run, summary string) {
	if run == nil {
		return
	}
	next := *run
	next.Status = workflows.RunFailed
	next.StatusSummary = summary
	next.UpdatedAt = t.now().UTC()
	_, _ = t.registry.CompareAndSwap(ctx, run.Revision, next)
}
