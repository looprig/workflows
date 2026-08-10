package tools

import (
	"context"
	"encoding/json"

	"github.com/looprig/harness/pkg/tool"
)

var runGetInfo = tool.ToolInfo{Name: "workflow_run_get", Desc: "Get bounded metadata and lifecycle status for one workflow run in this session.", Schema: json.RawMessage(`{"type":"object","properties":{"run_id":{"type":"string"}},"required":["run_id"],"additionalProperties":false}`)}

type runGetTool struct{ *boundRuntime }

func (t *runGetTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runGetInfo.Clone()
	return &clone, nil
}
func (t *runGetTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		RunID string `json:"run_id"`
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
	return result(newRunResult(*run))
}
