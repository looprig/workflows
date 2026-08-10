package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/workflows"
)

const maxCancelReasonBytes = 512

var runCancelInfo = tool.ToolInfo{Name: "workflow_run_cancel", Desc: "Request terminal cancellation of one active workflow run in this session.", Schema: json.RawMessage(`{"type":"object","properties":{"run_id":{"type":"string"},"reason":{"type":"string","maxLength":512}},"required":["run_id"],"additionalProperties":false}`)}

type runCancelTool struct {
	*boundRuntime
	runtime *boundRuntime
}

func (t *runCancelTool) bound() *boundRuntime {
	if t.runtime != nil {
		return t.runtime
	}
	return t.boundRuntime
}

func (t *runCancelTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runCancelInfo.Clone()
	return &clone, nil
}
func (t *runCancelTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		RunID  string `json:"run_id"`
		Reason string `json:"reason"`
	}
	if err := decodeStrict(raw, &args); err != nil {
		return nil, err
	}
	id, err := parseID("run_id", args.RunID)
	if err != nil {
		return nil, err
	}
	if len(args.Reason) > maxCancelReasonBytes {
		return nil, fmt.Errorf("workflow tools: reason exceeds %d bytes", maxCancelReasonBytes)
	}
	runtime := t.bound()
	if runtime == nil {
		return nil, fmt.Errorf("workflow tools: cancel runtime is not initialized")
	}
	run, err := runtime.registry.Get(ctx, runtime.sessionID, id)
	if err != nil {
		return nil, err
	}
	if run.Status == workflows.RunCancelled {
		return result(cancelResult{RunID: id.String(), RunStatus: string(run.Status), Idempotent: true})
	}
	if run.Status == workflows.RunCompleted || run.Status == workflows.RunFailed {
		return nil, fmt.Errorf("workflow tools: terminal run cannot be cancelled: %w", workflows.ErrConflict)
	}
	reason := strings.TrimSpace(args.Reason)
	if reason == "" {
		reason = "cancel requested"
	}
	if err := runtime.supervisor.Cancel(ctx, id, reason); err != nil {
		return nil, err
	}
	updated, err := runtime.registry.Get(ctx, runtime.sessionID, id)
	if err != nil {
		return nil, err
	}
	return result(cancelResult{RunID: id.String(), RunStatus: string(updated.Status)})
}
