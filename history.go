package workflows

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/tool"
)

const (
	maxWorkflowActivityHistoryRecords = 10_000
	maxWorkflowActivityHistoryPage    = 100
)

// ActivityHistoryRecord is the safe, projected view of one durable workflow
// activity. It intentionally contains Harness metadata only; Flow state,
// checkpoint payloads, policy text, and model output never cross this API.
type ActivityHistoryRecord struct {
	Revision uint64
	Metadata tool.WorkflowActivityMetadata
}

// ActivityHistoryPage is a bounded page of projected workflow activities.
// NextRevision is the exclusive revision cursor for the next request; a zero
// value means there is no next page.
type ActivityHistoryPage struct {
	Records      []ActivityHistoryRecord
	NextRevision *uint64
	NextEventID  *uuid.UUID
}

// History returns the projected durable activity history for one session run.
// afterRevision is the first revision to consider. A nonzero afterEventID
// resumes after that specific activity when a single checkpoint revision was
// too large for one page. Reads are serialized with the run controller so a
// concurrent lifecycle transition cannot produce a mixed registry/checkpoint
// view.
func (s *Supervisor) History(ctx context.Context, runID uuid.UUID, afterRevision uint64, afterEventID uuid.UUID, limit int) (ActivityHistoryPage, error) {
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > maxWorkflowActivityHistoryPage {
		return ActivityHistoryPage{}, fmt.Errorf("workflows: activity history page must be within 1..%d", maxWorkflowActivityHistoryPage)
	}
	controller, err := s.controller(runID)
	if err != nil {
		return ActivityHistoryPage{}, err
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return ActivityHistoryPage{}, err
	}
	defer cancel()
	controller.mu.Lock()
	defer controller.mu.Unlock()

	run, err := s.registry.Get(opCtx, s.sessionID, runID)
	if err != nil {
		return ActivityHistoryPage{}, err
	}
	definition, err := s.catalog.Resolve(run.DefinitionName, run.DefinitionVersion)
	if err != nil {
		return ActivityHistoryPage{}, &AdoptionError{RunID: run.ID, Op: "resolve history definition", Err: err}
	}
	history, err := loadWorkflowCheckpointHistory(opCtx, definition, run.GraphRunID)
	if err != nil {
		var missing *flow.CheckpointNotFoundError
		if errors.As(err, &missing) && run.Status == RunFailed {
			history = nil
		} else {
			return ActivityHistoryPage{}, &ReconciliationError{RunID: run.ID, Op: "read activity history", Err: err}
		}
	}
	if history == nil && run.Status != RunFailed {
		return ActivityHistoryPage{}, &ReconciliationError{RunID: run.ID, Op: "read activity history", Err: errors.New("full checkpoint history is unavailable")}
	}
	projected, err := projectActivityHistory(run, definition.Metadata(), history)
	if err != nil {
		return ActivityHistoryPage{}, &ReconciliationError{RunID: run.ID, Op: "project activity history", Err: err}
	}
	if len(projected) > maxWorkflowActivityHistoryRecords {
		return ActivityHistoryPage{}, &ReconciliationError{RunID: run.ID, Op: "bound activity history", Err: fmt.Errorf("history exceeds %d records", maxWorkflowActivityHistoryRecords)}
	}

	start := 0
	for start < len(projected) {
		if projected[start].revision < afterRevision {
			start++
			continue
		}
		if projected[start].revision == afterRevision && !afterEventID.IsZero() {
			if projected[start].body.EventID == afterEventID {
				start++
				break
			}
			start++
			continue
		}
		break
	}
	page := ActivityHistoryPage{Records: make([]ActivityHistoryRecord, 0, limit)}
	for start < len(projected) {
		revision := projected[start].revision
		end := start
		for end < len(projected) && projected[end].revision == revision {
			end++
		}
		if len(page.Records) > 0 && len(page.Records)+(end-start) > limit {
			break
		}
		if len(page.Records) == 0 && end-start > limit {
			end = start + limit
			for _, item := range projected[start:end] {
				page.Records = append(page.Records, ActivityHistoryRecord{Revision: item.revision, Metadata: item.body})
			}
			nextRevision := revision
			nextEventID := page.Records[len(page.Records)-1].Metadata.EventID
			page.NextRevision = &nextRevision
			page.NextEventID = &nextEventID
			return page, nil
		}
		for _, item := range projected[start:end] {
			page.Records = append(page.Records, ActivityHistoryRecord{Revision: item.revision, Metadata: item.body})
		}
		start = end
	}
	if start < len(projected) && len(page.Records) > 0 {
		next := page.Records[len(page.Records)-1].Revision + 1
		page.NextRevision = &next
	}
	return page, nil
}
