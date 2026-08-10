package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
)

const (
	maxHistoryPageSize = 100
	maxHistoryRecords  = 10_000
)

var runHistoryInfo = tool.ToolInfo{Name: "workflow_run_history", Desc: "Return a bounded page of durable checkpoint activity metadata for one session workflow run.", Schema: json.RawMessage(`{"type":"object","properties":{"run_id":{"type":"string"},"after_revision":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["run_id"],"additionalProperties":false}`)}

type runHistoryTool struct{ *boundRuntime }

func (t *runHistoryTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runHistoryInfo.Clone()
	return &clone, nil
}
func (t *runHistoryTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		RunID         string `json:"run_id"`
		AfterRevision uint64 `json:"after_revision"`
		Limit         int    `json:"limit"`
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
	definition, err := t.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return nil, err
	}
	history, err := definition.History(ctx, run.GraphRunID)
	if err != nil {
		return nil, err
	}
	if len(history) > maxHistoryRecords {
		return nil, fmt.Errorf("workflow tools: history exceeds %d records", maxHistoryRecords)
	}
	limit := args.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > maxHistoryPageSize {
		return nil, fmt.Errorf("workflow tools: history limit must be within 1..%d", maxHistoryPageSize)
	}
	type entry struct {
		Revision  uint64    `json:"revision"`
		RunStatus string    `json:"run_status"`
		Step      uint64    `json:"step"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	entries := make([]entry, 0, limit)
	var next *uint64
	for _, state := range history {
		if state.Revision <= args.AfterRevision && args.AfterRevision != 0 {
			continue
		}
		if len(entries) == limit {
			value := entries[len(entries)-1].Revision
			next = &value
			break
		}
		step, stepErr := safeFlowStep(state.Step)
		if stepErr != nil {
			return nil, stepErr
		}
		entries = append(entries, entry{state.Revision, safeFlowStatus(state.Status), step, state.CreatedAt, state.UpdatedAt})
	}
	return result(struct {
		RunID        string  `json:"run_id"`
		Entries      []entry `json:"entries"`
		NextRevision *uint64 `json:"next_revision,omitempty"`
	}{id.String(), entries, next})
}

func safeFlowStatus(status flow.RunStatus) string {
	switch status {
	case flow.RunRunning:
		return "running"
	case flow.RunInterrupted:
		return "interrupted"
	case flow.RunCompleted:
		return "completed"
	case flow.RunCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

func safeFlowStep(step flow.StepID) (uint64, error) {
	if step < 0 {
		return 0, fmt.Errorf("workflow tools: invalid negative checkpoint step")
	}
	return uint64(step), nil
}
