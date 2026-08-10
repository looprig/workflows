package workflows

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
)

const (
	maxActivityHistoryCheckpoints = 10_000
	maxActivityVerticesPerRecord  = 10_000
	maxActivityVertexLabelBytes   = 128
	maxActivityMessageBytes       = 512
	maxActivityProgress           = 1_000_000
)

type activityKind string

const (
	activityRunStarted      activityKind = "run_started"
	activityVertexCompleted activityKind = "vertex_completed"
	activityRunInterrupted  activityKind = "run_interrupted"
	activityRunResumed      activityKind = "run_resumed"
	activityRunCompleted    activityKind = "run_completed"
	activityRunCancelled    activityKind = "run_cancelled"
	activityRunFailed       activityKind = "run_failed"
)

var (
	ErrActivityValidation = errors.New("workflow activity validation failed")
	ErrReconciliation     = errors.New("workflow activity reconciliation failed")
)

type ActivityValidationError struct {
	Field string
	Rule  string
}

func (e *ActivityValidationError) Error() string {
	return fmt.Sprintf("%s: %s %s", ErrActivityValidation, e.Field, e.Rule)
}
func (e *ActivityValidationError) Unwrap() error { return ErrActivityValidation }

type ReconciliationError struct {
	RunID uuid.UUID
	Op    string
	Err   error
}

func (e *ReconciliationError) Error() string {
	return fmt.Sprintf("%s: run %s during %s", ErrReconciliation, e.RunID, e.Op)
}
func (e *ReconciliationError) Unwrap() error { return ErrReconciliation }

type projectedActivity struct {
	revision uint64
	body     tool.WorkflowActivityMetadata
}

type vertexTransitionKey struct {
	step int
	id   flow.VertexID
}

func projectActivities(run Run, metadata Metadata, history []*flow.Checkpoint) ([]tool.WorkflowActivityMetadata, error) {
	projected, err := projectActivityHistory(&run, metadata, history)
	if err != nil {
		return nil, err
	}
	result := make([]tool.WorkflowActivityMetadata, len(projected))
	for i := range projected {
		result[i] = projected[i].body
	}
	return result, nil
}

func projectActivityHistory(run *Run, metadata Metadata, history []*flow.Checkpoint) ([]projectedActivity, error) {
	if run == nil || run.SessionID.IsZero() || run.ID.IsZero() || uuid.UUID(run.GraphRunID).IsZero() {
		return nil, activityValidation("run", "requires complete identity")
	}
	if metadata.Name() != run.DefinitionName || metadata.Version() != run.DefinitionVersion {
		return nil, activityValidation("definition", "does not match run identity")
	}
	if len(history) > maxActivityHistoryCheckpoints {
		return nil, activityValidation("history", "exceeds checkpoint limit")
	}
	labels := metadata.Vertices()
	if len(labels) > maxActivityProgress {
		return nil, activityValidation("definition vertices", "exceed progress limit")
	}
	var total uint32
	for range labels {
		total++
	}
	completed := uint32(0)
	done := make(map[vertexTransitionKey]struct{})
	vertexOrdinals := make(map[flow.VertexID]uint32)
	terminal := false
	activities := make([]projectedActivity, 0, len(history)*2)
	for index, checkpoint := range history {
		if checkpoint == nil {
			return nil, activityValidation("checkpoint", "must not be nil")
		}
		revision := checkpoint.Run.Revision
		if revision != uint64(index) {
			return nil, activityValidation("checkpoint revision", "must be contiguous from zero")
		}
		if checkpoint.Run.GraphRunID != run.GraphRunID {
			return nil, activityValidation("checkpoint run ID", "does not match registry")
		}
		if len(checkpoint.Vertices) > maxActivityVerticesPerRecord {
			return nil, activityValidation("checkpoint vertices", "exceed record limit")
		}
		if terminal {
			return nil, activityValidation("history", "continues after terminal checkpoint")
		}
		if revision == 0 {
			occurred, err := activityTime(checkpoint.Run.CreatedAt, checkpoint.Run.UpdatedAt)
			if err != nil {
				return nil, err
			}
			activities = append(activities, makeActivity(*run, activityRunStarted, revision, uuid.UUID{}, 0, "", completed, total, "Workflow started", occurred, RunRunning))
		}
		if shouldProjectResume(*run, history, index) {
			occurred, err := activityTime(checkpoint.Run.UpdatedAt)
			if err != nil {
				return nil, err
			}
			activities = append(activities, makeActivity(*run, activityRunResumed, revision, uuid.UUID{}, 0, "", completed, total, "Workflow resumed", occurred, RunRunning))
		}

		transitions := make([]flow.VertexState, 0, len(checkpoint.Vertices))
		for _, vertex := range checkpoint.Vertices {
			key := vertexTransitionKey{step: int(vertex.Step), id: vertex.VertexID}
			if vertex.Status != flow.VertexDone {
				continue
			}
			if _, exists := done[key]; exists {
				continue
			}
			done[key] = struct{}{}
			transitions = append(transitions, vertex)
		}
		sort.Slice(transitions, func(i, j int) bool {
			if transitions[i].Step != transitions[j].Step {
				return transitions[i].Step < transitions[j].Step
			}
			return transitions[i].VertexID.String() < transitions[j].VertexID.String()
		})
		for _, vertex := range transitions {
			ordinal := vertexOrdinals[vertex.VertexID]
			vertexOrdinals[vertex.VertexID] = ordinal + 1
			label := ""
			if int(completed) < len(labels) {
				label = boundActivityText(labels[completed].Label(), maxActivityVertexLabelBytes)
			}
			completed++
			progress := completed
			if total > 0 && progress > total {
				progress = total
			}
			occurred, err := activityTime(vertex.CompletedAt, checkpoint.Run.UpdatedAt)
			if err != nil {
				return nil, err
			}
			activities = append(activities, makeActivity(*run, activityVertexCompleted, revision, uuid.UUID(vertex.VertexID), ordinal, label, progress, total, "Workflow step completed", occurred, RunRunning))
		}

		switch checkpoint.Run.Status {
		case flow.RunRunning:
		case flow.RunInterrupted:
			occurred, err := activityTime(checkpoint.Run.InterruptedAt, checkpoint.Run.UpdatedAt)
			if err != nil {
				return nil, err
			}
			activities = append(activities, makeActivity(*run, activityRunInterrupted, revision, uuid.UUID{}, 0, "", completed, total, "Workflow interrupted", occurred, RunInterrupted))
		case flow.RunCompleted:
			occurred, err := activityTime(checkpoint.Run.CompletedAt, checkpoint.Run.UpdatedAt)
			if err != nil {
				return nil, err
			}
			activities = append(activities, makeActivity(*run, activityRunCompleted, revision, uuid.UUID{}, 0, "", completed, total, "Workflow completed", occurred, RunCompleted))
			terminal = true
		case flow.RunCancelled:
			occurred, err := activityTime(checkpoint.Run.CancelledAt, checkpoint.Run.UpdatedAt)
			if err != nil {
				return nil, err
			}
			activities = append(activities, makeActivity(*run, activityRunCancelled, revision, uuid.UUID{}, 0, "", completed, total, "Workflow cancelled", occurred, RunCancelled))
			terminal = true
		default:
			return nil, activityValidation("checkpoint status", "is invalid")
		}
	}
	if run.Status == RunFailed && !terminal {
		occurred, err := activityTime(run.UpdatedAt)
		if err != nil {
			return nil, err
		}
		// Registry failure can be durable without a Flow checkpoint (for
		// example, invalid input before Start). Project it as one bounded
		// virtual checkpoint after the available history so the cursor can
		// acknowledge it without iterating over an untrusted revision.
		revision := uint64(len(history))
		activities = append(activities, makeActivity(*run, activityRunFailed, revision, uuid.UUID{}, 0, "", completed, total, "Workflow failed", occurred, RunFailed))
	}
	return activities, nil
}

func shouldProjectResume(run Run, history []*flow.Checkpoint, index int) bool {
	if run.Status != RunRunning || run.StatusSummary != "workflow resuming" || index == 0 {
		return false
	}
	checkpoint := history[index]
	return checkpoint.Run.Revision == run.CheckpointRevision+1 && history[index-1].Run.Status == flow.RunInterrupted
}

func makeActivity(run Run, kind activityKind, revision uint64, vertexID uuid.UUID, ordinal uint32, label string, completed, total uint32, message string, occurred time.Time, status RunStatus) projectedActivity {
	return projectedActivity{revision: revision, body: tool.WorkflowActivityMetadata{
		EventID:   stableActivityID(activityIdentity{SessionID: run.SessionID, RunID: run.ID, Kind: kind, Revision: revision, VertexID: vertexID, Ordinal: ordinal}),
		SessionID: run.SessionID, RunID: run.ID, WorkflowName: run.DefinitionName, WorkflowVersion: run.DefinitionVersion,
		Kind: string(kind), Status: string(status), VertexID: vertexID, VertexLabel: label,
		CompletedVertices: completed, TotalVertices: total,
		Message: boundActivityText(message, maxActivityMessageBytes), OccurredAt: occurred,
	}}
}

func activityValidation(field, rule string) error {
	return &ActivityValidationError{Field: field, Rule: rule}
}

func activityTime(values ...time.Time) (time.Time, error) {
	for _, value := range values {
		if !value.IsZero() {
			// Flow records use time.Now(), whose Location may be the process
			// local zone. Normalize the instant at this boundary instead of
			// rejecting valid durable timestamps merely because their location
			// metadata is not UTC.
			return value.UTC(), nil
		}
	}
	return time.Time{}, activityValidation("occurred_at", "requires a durable UTC timestamp")
}

func boundActivityText(value string, limit int) string {
	if limit <= 0 || !utf8.ValidString(value) {
		return ""
	}
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return strings.TrimSpace(value)
}
