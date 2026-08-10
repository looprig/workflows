package workflows

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
)

// RunRegistry persists bounded session-owned workflow metadata in neutral KV.
type RunRegistry struct {
	kv storage.KV
}

func NewRunRegistry(kv storage.KV) (*RunRegistry, error) {
	if kv == nil {
		return nil, errors.New("workflows: run registry requires storage.KV")
	}
	return &RunRegistry{kv: kv}, nil
}

func (r *RunRegistry) Create(ctx context.Context, run Run) (*Run, error) {
	if r == nil || r.kv == nil {
		return nil, errors.New("workflows: run registry is not initialized")
	}
	if run.Revision != 0 {
		return nil, errors.New("workflows: new run revision must be zero")
	}
	key, err := runKey(run.SessionID, run.ID)
	if err != nil {
		return nil, err
	}
	raw, err := encodeRunRecord(run)
	if err != nil {
		return nil, err
	}
	revision, err := r.kv.Put(ctx, key, 0, raw)
	if err != nil {
		if errors.As(err, new(*storage.ConflictError)) {
			return nil, &ConflictError{SessionID: run.SessionID, RunID: run.ID, Expected: 0, Reason: "run already exists"}
		}
		return nil, fmt.Errorf("workflows: create run metadata: %w", err)
	}
	created := cloneRun(run)
	created.Revision = revision
	return &created, nil
}

func (r *RunRegistry) Get(ctx context.Context, sessionID, runID uuid.UUID) (*Run, error) {
	if r == nil || r.kv == nil {
		return nil, errors.New("workflows: run registry is not initialized")
	}
	key, err := runKey(sessionID, runID)
	if err != nil {
		return nil, err
	}
	raw, revision, err := r.kv.Get(ctx, key)
	if err != nil {
		if errors.As(err, new(*storage.KeyNotFoundError)) {
			return nil, &NotFoundError{Kind: "run", SessionID: sessionID, RunID: runID}
		}
		return nil, fmt.Errorf("workflows: get run metadata: %w", err)
	}
	run, err := decodeRunRecord(key, raw)
	if err != nil {
		return nil, err
	}
	if run.SessionID != sessionID || run.ID != runID {
		return nil, &CorruptRecordError{Key: key, Err: errors.New("record identity does not match its key")}
	}
	run.Revision = revision
	copy := cloneRun(run)
	return &copy, nil
}

func (r *RunRegistry) CompareAndSwap(ctx context.Context, expectedRevision uint64, next Run) (*Run, error) {
	if expectedRevision == 0 {
		return nil, errors.New("workflows: compare-and-swap requires a nonzero expected revision")
	}
	if next.Revision != expectedRevision {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: next.Revision, Reason: "record carries a different revision"}
	}
	current, err := r.Get(ctx, next.SessionID, next.ID)
	if err != nil {
		return nil, err
	}
	if current.Revision != expectedRevision {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: current.Revision, Reason: "stale revision"}
	}
	if !immutableIdentityEqual(*current, next) {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: current.Revision, Reason: "immutable run identity changed"}
	}
	if !validRunTransition(current.Status, next.Status) {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: current.Revision, Reason: fmt.Sprintf("illegal status transition %s -> %s", current.Status, next.Status)}
	}
	if next.CheckpointRevision < current.CheckpointRevision {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: current.Revision, Reason: "checkpoint revision moved backwards"}
	}
	if next.ActivityCursor < current.ActivityCursor {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: current.Revision, Reason: "activity cursor moved backwards"}
	}
	if next.UpdatedAt.Before(current.UpdatedAt) {
		return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Actual: current.Revision, Reason: "updated_at moved backwards"}
	}
	raw, err := encodeRunRecord(next)
	if err != nil {
		return nil, err
	}
	key, err := runKey(next.SessionID, next.ID)
	if err != nil {
		return nil, err
	}
	revision, err := r.kv.Put(ctx, key, expectedRevision, raw)
	if err != nil {
		if errors.As(err, new(*storage.ConflictError)) {
			return nil, &ConflictError{SessionID: next.SessionID, RunID: next.ID, Expected: expectedRevision, Reason: "stale revision"}
		}
		return nil, fmt.Errorf("workflows: update run metadata: %w", err)
	}
	updated := cloneRun(next)
	updated.Revision = revision
	return &updated, nil
}

type ListRunsRequest struct {
	After string
	Limit int
}

type RunPage struct {
	Runs []Run
	Next string
}

func (r *RunRegistry) List(ctx context.Context, sessionID uuid.UUID, request ListRunsRequest) (RunPage, error) {
	if r == nil || r.kv == nil {
		return RunPage{}, errors.New("workflows: run registry is not initialized")
	}
	prefix, err := runPrefix(sessionID)
	if err != nil {
		return RunPage{}, err
	}
	limit := request.Limit
	if limit == 0 {
		limit = DefaultRunPageSize
	}
	if limit < 1 || limit > MaxRunPageSize {
		return RunPage{}, fmt.Errorf("workflows: run page limit must be within 1..%d", MaxRunPageSize)
	}
	startKey := prefix
	if request.After != "" {
		cursorID, parseErr := uuid.Parse(request.After)
		if parseErr != nil || cursorID.IsZero() {
			return RunPage{}, errors.New("workflows: invalid run page cursor")
		}
		startKey, err = runKey(sessionID, cursorID)
		if err != nil {
			return RunPage{}, err
		}
	}
	keys, err := r.kv.Keys(ctx, prefix)
	if err != nil {
		return RunPage{}, fmt.Errorf("workflows: list run metadata: %w", err)
	}
	start := sort.Search(len(keys), func(i int) bool { return keys[i] > startKey })
	page := RunPage{Runs: make([]Run, 0, min(limit, len(keys)-start))}
	for i := start; i < len(keys) && len(page.Runs) < limit; i++ {
		key := keys[i]
		if !strings.HasPrefix(key, prefix) {
			return RunPage{}, &CorruptRecordError{Key: key, Err: errors.New("key escaped session prefix")}
		}
		runText := strings.TrimPrefix(key, prefix)
		runID, parseErr := uuid.Parse(runText)
		if parseErr != nil || runID.IsZero() {
			return RunPage{}, &CorruptRecordError{Key: key, Err: errors.New("key has invalid workflow run ID")}
		}
		run, getErr := r.Get(ctx, sessionID, runID)
		if getErr != nil {
			return RunPage{}, getErr
		}
		page.Runs = append(page.Runs, cloneRun(*run))
	}
	if start+len(page.Runs) < len(keys) && len(page.Runs) > 0 {
		page.Next = page.Runs[len(page.Runs)-1].ID.String()
	}
	return page, nil
}
