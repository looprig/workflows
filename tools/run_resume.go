package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/workflows"
)

var runResumeInfo = tool.ToolInfo{Name: "workflow_run_resume", Desc: "Resume one interrupted workflow run with definition-validated resume input.", Schema: json.RawMessage(`{"type":"object","properties":{"run_id":{"type":"string"},"resume":{"type":"object"}},"required":["run_id","resume"],"additionalProperties":false}`)}

type runResumeTool struct{ *boundRuntime }

func (t *runResumeTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runResumeInfo.Clone()
	return &clone, nil
}
func (t *runResumeTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		RunID  string          `json:"run_id"`
		Resume json.RawMessage `json:"resume"`
	}
	if err := decodeStrict(raw, &args); err != nil {
		return nil, err
	}
	id, err := parseID("run_id", args.RunID)
	if err != nil {
		return nil, err
	}
	run, err := t.registry.Get(ctx, t.sessionID, id)
	if err != nil {
		return nil, err
	}
	if run.Status != workflows.RunInterrupted {
		return nil, fmt.Errorf("workflow tools: resume requires interrupted run: %w", workflows.ErrConflict)
	}
	definition, err := t.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicalObject(args.Resume)
	if err != nil {
		return nil, err
	}
	if _, err := definition.ValidateResume(canonical); err != nil {
		return nil, err
	}
	if err := t.supervisor.Resume(ctx, id, canonical); err != nil {
		return nil, err
	}
	updated, err := t.registry.Get(ctx, t.sessionID, id)
	if err != nil {
		return nil, err
	}
	return result(newRunResult(*updated))
}
