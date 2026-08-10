package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/tool"
)

const (
	maxHistoryPageSize = 100
)

var runHistoryInfo = tool.ToolInfo{Name: "workflow_run_history", Desc: "Return a bounded page of projected workflow activity metadata for one session run.", Schema: json.RawMessage(`{"type":"object","properties":{"run_id":{"type":"string"},"after_revision":{"type":"integer","minimum":0},"after_event_id":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["run_id"],"additionalProperties":false}`)}

type runHistoryTool struct{ *boundRuntime }

func (t *runHistoryTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := runHistoryInfo.Clone()
	return &clone, nil
}
func (t *runHistoryTool) InvokableRun(ctx context.Context, raw string) (*tool.ToolResult, error) {
	var args struct {
		RunID         string `json:"run_id"`
		AfterRevision uint64 `json:"after_revision"`
		AfterEventID  string `json:"after_event_id"`
		Limit         int    `json:"limit"`
	}
	if err := decodeStrict(raw, &args); err != nil {
		return nil, err
	}
	id, err := parseID("run_id", args.RunID)
	if err != nil {
		return nil, err
	}
	limit := args.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > maxHistoryPageSize {
		return nil, fmt.Errorf("workflow tools: history limit must be within 1..%d", maxHistoryPageSize)
	}
	var afterEventID uuid.UUID
	if args.AfterEventID != "" {
		afterEventID, err = parseID("after_event_id", args.AfterEventID)
		if err != nil {
			return nil, err
		}
	}
	reader, ok := t.supervisor.(supervisorHistory)
	if !ok {
		return nil, fmt.Errorf("workflow tools: supervisor does not provide projected history")
	}
	page, err := reader.History(ctx, id, args.AfterRevision, afterEventID, limit)
	if err != nil {
		return nil, err
	}
	type entry struct {
		Revision          uint64    `json:"revision"`
		EventID           string    `json:"event_id"`
		Kind              string    `json:"kind"`
		RunStatus         string    `json:"run_status"`
		VertexID          string    `json:"vertex_id,omitempty"`
		VertexLabel       string    `json:"vertex_label,omitempty"`
		CompletedVertices uint32    `json:"completed_vertices,omitempty"`
		TotalVertices     uint32    `json:"total_vertices,omitempty"`
		Message           string    `json:"message,omitempty"`
		OccurredAt        time.Time `json:"occurred_at"`
	}
	entries := make([]entry, 0, len(page.Records))
	for _, record := range page.Records {
		metadata := record.Metadata
		vertexID := ""
		if !metadata.VertexID.IsZero() {
			vertexID = metadata.VertexID.String()
		}
		entries = append(entries, entry{
			Revision: record.Revision, EventID: metadata.EventID.String(), Kind: metadata.Kind,
			RunStatus: metadata.Status, VertexID: vertexID, VertexLabel: metadata.VertexLabel,
			CompletedVertices: metadata.CompletedVertices, TotalVertices: metadata.TotalVertices,
			Message: metadata.Message, OccurredAt: metadata.OccurredAt,
		})
	}
	return result(struct {
		RunID        string     `json:"run_id"`
		Entries      []entry    `json:"entries"`
		NextRevision *uint64    `json:"next_revision,omitempty"`
		NextEventID  *uuid.UUID `json:"next_event_id,omitzero"`
	}{id.String(), entries, page.NextRevision, page.NextEventID})
}
