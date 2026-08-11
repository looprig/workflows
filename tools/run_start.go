package tools

import (
	"context"
	"encoding/json"
	"errors"

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
	if _, err := definition.ValidateInput(canonical); err != nil {
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
		ArtifactSessionID: t.sessionID, ArtifactRunID: runID,
		LedgerLocator: "flow/runs/" + graphUUID.String(), CreatedAt: now, UpdatedAt: now}
	if t.prepareRun != nil {
		before := run
		before.Artifacts = append([]workflows.ArtifactReference(nil), run.Artifacts...)
		run.Artifacts = append([]workflows.ArtifactReference(nil), run.Artifacts...)
		prepared, prepareErr := t.prepareRun(ctx, run)
		if prepareErr != nil {
			return nil, prepareErr
		}
		if integrityErr := validatePreparedRun(before, prepared); integrityErr != nil {
			return nil, integrityErr
		}
		run = prepared
	}
	created, err := t.registry.Create(ctx, run)
	if err != nil {
		return nil, err
	}
	starter := t.supervisor.(supervisorStarter)
	seeded, failed, startErr := starter.Start(ctx, created.ID)
	if startErr != nil {
		return nil, startErr
	}
	if seeded == nil || failed == nil {
		return nil, errors.New("workflow tools: session-owned start controller returned invalid result channels")
	}
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
