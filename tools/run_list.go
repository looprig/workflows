package tools

import (
	"context"
	"encoding/json"

	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/workflows"
)

var runListInfo = tool.ToolInfo{Name: "workflow_run_list", Desc: "List a bounded page of workflow runs owned by this session.", Schema: json.RawMessage(`{"type":"object","properties":{"after":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100}},"additionalProperties":false}`)}

type runListTool struct{ *boundRuntime }

func (t *runListTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runListInfo.Clone()
	return &clone, nil
}
func (t *runListTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		After string `json:"after"`
		Limit int    `json:"limit"`
	}
	if err := decodeStrict(raw, &args); err != nil {
		return nil, err
	}
	page, err := t.registry.List(ctx, t.sessionID, workflows.ListRunsRequest{After: args.After, Limit: args.Limit})
	if err != nil {
		return nil, err
	}
	runs := make([]runResult, len(page.Runs))
	for i := range page.Runs {
		runs[i] = newRunResult(page.Runs[i])
	}
	return result(struct {
		Runs []runResult `json:"runs"`
		Next string      `json:"next,omitempty"`
	}{runs, page.Next})
}
