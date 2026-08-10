package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func testUUID(last byte) uuid.UUID {
	var id uuid.UUID
	id[15] = last
	return id
}

func testGraphRunID(last byte) flow.GraphRunID {
	return flow.GraphRunID(testUUID(last))
}

func testRun(sessionID, runID uuid.UUID) Run {
	created := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	return Run{
		SessionID:         sessionID,
		ToolExecutionID:   testUUID(21),
		DefinitionName:    "source_document_extract",
		DefinitionVersion: "v1",
		ID:                runID,
		GraphRunID:        testGraphRunID(31),
		ParentRunID:       testUUID(41),
		Input: InputReference{
			Digest: strings.Repeat("a", 64),
			Key:    "sessions/" + sessionID.String() + "/workflows/inputs/sha256/" + strings.Repeat("a", 64),
			Size:   128,
		},
		Status:             RunPending,
		StatusSummary:      "queued",
		LedgerLocator:      "flow/runs/" + testGraphRunID(31).String(),
		CreatedAt:          created,
		UpdatedAt:          created,
		CheckpointRevision: 0,
		ActivityCursor:     0,
		Artifacts: []ArtifactReference{{
			ID:     "source-document-001",
			Kind:   "source_document",
			Digest: strings.Repeat("b", 64),
			Size:   4096,
		}},
	}
}

func TestRunRegistryCreateGetPreservesImmutableCorrelationAndCopies(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	registry, err := NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatalf("NewRunRegistry: %v", err)
	}
	run := testRun(testUUID(1), testUUID(2))

	created, err := registry.Create(ctx, run)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Revision != 1 {
		t.Fatalf("Create revision = %d, want 1", created.Revision)
	}
	if created.SessionID != run.SessionID || created.ToolExecutionID != run.ToolExecutionID ||
		created.DefinitionName != run.DefinitionName || created.DefinitionVersion != run.DefinitionVersion ||
		created.ID != run.ID || created.GraphRunID != run.GraphRunID || created.ParentRunID != run.ParentRunID ||
		created.Input != run.Input {
		t.Fatalf("created correlation changed: %#v", created)
	}

	created.Artifacts[0].ID = "mutated"
	got, err := registry.Get(ctx, run.SessionID, run.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Artifacts[0].ID != "source-document-001" {
		t.Fatalf("stored artifact mutated through returned value: %#v", got.Artifacts)
	}
	got.Artifacts[0].ID = "mutated-again"
	again, err := registry.Get(ctx, run.SessionID, run.ID)
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	if again.Artifacts[0].ID != "source-document-001" {
		t.Fatalf("Get did not return a defensive copy: %#v", again.Artifacts)
	}
}

func TestRunRegistryKVContainsReferencesNotInputOrOutputText(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	registry, err := NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatalf("NewRunRegistry: %v", err)
	}
	run := testRun(testUUID(3), testUUID(4))
	const privateInput = "the complete private policy input"
	const privateOutput = "the complete generated report output"
	if _, err := registry.Create(ctx, run); err != nil {
		t.Fatalf("Create: %v", err)
	}
	raw, _, err := backend.KV.Get(ctx, mustRunKey(run.SessionID, run.ID))
	if err != nil {
		t.Fatalf("read raw registry value: %v", err)
	}
	if strings.Contains(string(raw), privateInput) || strings.Contains(string(raw), privateOutput) {
		t.Fatalf("registry leaked private input/output text: %s", raw)
	}
	if !strings.Contains(string(raw), run.Input.Digest) || !strings.Contains(string(raw), run.Input.Key) {
		t.Fatalf("registry omitted immutable input reference: %s", raw)
	}
}

func TestRunRegistryCreateIsAbsentOnlyAndCASRejectsStaleRevision(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	registry, err := NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatalf("NewRunRegistry: %v", err)
	}
	run := testRun(testUUID(5), testUUID(6))
	created, err := registry.Create(ctx, run)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := registry.Create(ctx, run); !errors.Is(err, ErrConflict) || !errors.As(err, new(*ConflictError)) {
		t.Fatalf("duplicate Create error = %v, want typed conflict", err)
	}

	next := *created
	next.Status = RunRunning
	next.UpdatedAt = next.UpdatedAt.Add(time.Second)
	updated, err := registry.CompareAndSwap(ctx, created.Revision, next)
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if updated.Revision != 2 {
		t.Fatalf("updated revision = %d, want 2", updated.Revision)
	}
	next.StatusSummary = "stale writer"
	if _, err := registry.CompareAndSwap(ctx, created.Revision, next); !errors.Is(err, ErrConflict) || !errors.As(err, new(*ConflictError)) {
		t.Fatalf("stale CompareAndSwap error = %v, want typed conflict", err)
	}
}

func TestRunRegistryCompareAndSwapRejectsDecreasingProgress(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Run)
		field  string
	}{
		{
			name: "checkpoint revision",
			mutate: func(run *Run) {
				run.CheckpointRevision--
			},
			field: "checkpoint revision",
		},
		{
			name: "activity cursor",
			mutate: func(run *Run) {
				run.ActivityCursor--
			},
			field: "activity cursor",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			backend := memstore.New()
			registry, err := NewRunRegistry(backend.KV)
			if err != nil {
				t.Fatalf("NewRunRegistry: %v", err)
			}
			run := testRun(testUUID(byte(50+i)), testUUID(byte(60+i)))
			run.CheckpointRevision = 4
			run.ActivityCursor = 3
			created, err := registry.Create(ctx, run)
			if err != nil {
				t.Fatalf("Create: %v", err)
			}

			next := *created
			next.UpdatedAt = next.UpdatedAt.Add(time.Second)
			tc.mutate(&next)
			_, err = registry.CompareAndSwap(ctx, created.Revision, next)
			var conflict *ConflictError
			if !errors.Is(err, ErrConflict) || !errors.As(err, &conflict) {
				t.Fatalf("CompareAndSwap error = %v, want typed conflict", err)
			}
			if !strings.Contains(conflict.Reason, tc.field) {
				t.Fatalf("conflict reason = %q, want %q", conflict.Reason, tc.field)
			}

			stored, err := registry.Get(ctx, run.SessionID, run.ID)
			if err != nil {
				t.Fatalf("Get after rejected update: %v", err)
			}
			if stored.CheckpointRevision != created.CheckpointRevision || stored.ActivityCursor != created.ActivityCursor {
				t.Fatalf("stored progress changed after rejection: checkpoint=%d cursor=%d", stored.CheckpointRevision, stored.ActivityCursor)
			}
		})
	}
}

func TestRunRegistryEnforcesStatusTransitionsAndImmutableIdentity(t *testing.T) {
	legal := map[RunStatus][]RunStatus{
		RunPending:     {RunPending, RunRunning, RunCancelled, RunFailed},
		RunRunning:     {RunRunning, RunInterrupted, RunCompleted, RunCancelled, RunFailed},
		RunInterrupted: {RunInterrupted, RunRunning, RunCancelled, RunFailed},
		RunCompleted:   {RunCompleted},
		RunCancelled:   {RunCancelled},
		RunFailed:      {RunFailed},
	}
	all := []RunStatus{RunPending, RunRunning, RunInterrupted, RunCompleted, RunCancelled, RunFailed}
	for from, allowed := range legal {
		allowedSet := make(map[RunStatus]bool, len(allowed))
		for _, status := range allowed {
			allowedSet[status] = true
		}
		for _, to := range all {
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				ctx := context.Background()
				backend := memstore.New()
				registry, err := NewRunRegistry(backend.KV)
				if err != nil {
					t.Fatalf("NewRunRegistry: %v", err)
				}
				run := testRun(testUUID(7), testUUID(8))
				run.Status = from
				created, err := registry.Create(ctx, run)
				if err != nil {
					t.Fatalf("Create: %v", err)
				}
				next := *created
				next.Status = to
				next.UpdatedAt = next.UpdatedAt.Add(time.Second)
				_, err = registry.CompareAndSwap(ctx, created.Revision, next)
				if allowedSet[to] && err != nil {
					t.Fatalf("legal transition rejected: %v", err)
				}
				if !allowedSet[to] && !errors.Is(err, ErrConflict) {
					t.Fatalf("illegal transition error = %v, want conflict", err)
				}
			})
		}
	}

	ctx := context.Background()
	backend := memstore.New()
	registry, _ := NewRunRegistry(backend.KV)
	created, err := registry.Create(ctx, testRun(testUUID(9), testUUID(10)))
	if err != nil {
		t.Fatalf("Create identity case: %v", err)
	}
	mutated := *created
	mutated.Input.Digest = strings.Repeat("c", 64)
	mutated.UpdatedAt = mutated.UpdatedAt.Add(time.Second)
	if _, err := registry.CompareAndSwap(ctx, created.Revision, mutated); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity mutation error = %v, want conflict", err)
	}
}

func TestRunRegistryListIsSessionScopedSortedAndBounded(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	registry, err := NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatalf("NewRunRegistry: %v", err)
	}
	firstSession := testUUID(11)
	secondSession := testUUID(12)
	for _, id := range []byte{9, 2, 7, 1, 5} {
		if _, err := registry.Create(ctx, testRun(firstSession, testUUID(id))); err != nil {
			t.Fatalf("Create first session run %d: %v", id, err)
		}
	}
	if _, err := registry.Create(ctx, testRun(secondSession, testUUID(3))); err != nil {
		t.Fatalf("Create second session: %v", err)
	}

	var got []uuid.UUID
	cursor := ""
	for {
		page, err := registry.List(ctx, firstSession, ListRunsRequest{After: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("List after %q: %v", cursor, err)
		}
		if len(page.Runs) > 2 {
			t.Fatalf("page size = %d, want <= 2", len(page.Runs))
		}
		for _, run := range page.Runs {
			if run.SessionID != firstSession {
				t.Fatalf("List crossed session boundary: %s", run.SessionID)
			}
			got = append(got, run.ID)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	want := []uuid.UUID{testUUID(1), testUUID(2), testUUID(5), testUUID(7), testUUID(9)}
	if len(got) != len(want) {
		t.Fatalf("listed IDs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listed IDs = %v, want sorted %v", got, want)
		}
	}
	if _, err := registry.List(ctx, firstSession, ListRunsRequest{Limit: MaxRunPageSize + 1}); err == nil {
		t.Fatal("List accepted a page larger than MaxRunPageSize")
	}
}

func TestRunRegistryStrictVersionedDecodeAndBounds(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "unknown envelope field", raw: []byte(`{"version":1,"run":{},"extra":true}`)},
		{name: "unknown run field", raw: []byte(`{"version":1,"run":{"unknown":true}}`)},
		{name: "unknown version", raw: []byte(`{"version":99,"run":{}}`)},
		{name: "trailing JSON", raw: []byte(`{"version":1,"run":{}} {}`)},
		{name: "oversized", raw: []byte(`{"version":1,"run":{"status_summary":"` + strings.Repeat("x", MaxRunRecordBytes) + `"}}`)},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			backend := memstore.New()
			registry, err := NewRunRegistry(backend.KV)
			if err != nil {
				t.Fatalf("NewRunRegistry: %v", err)
			}
			sessionID := testUUID(13)
			runID := testUUID(byte(20 + i))
			if _, err := backend.KV.Put(ctx, mustRunKey(sessionID, runID), 0, tc.raw); err != nil {
				t.Fatalf("seed corrupt record: %v", err)
			}
			_, err = registry.Get(ctx, sessionID, runID)
			if !errors.Is(err, ErrCorruptRecord) || !errors.As(err, new(*CorruptRecordError)) {
				t.Fatalf("Get corrupt record error = %v, want typed corrupt record", err)
			}
		})
	}
}

func TestRunRegistrySessionIsolationAndValidatedKeys(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	registry, err := NewRunRegistry(backend.KV)
	if err != nil {
		t.Fatalf("NewRunRegistry: %v", err)
	}
	owner := testUUID(14)
	other := testUUID(15)
	run := testRun(owner, testUUID(16))
	if _, err := registry.Create(ctx, run); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := registry.Get(ctx, other, run.ID); !errors.Is(err, ErrNotFound) || !errors.As(err, new(*NotFoundError)) {
		t.Fatalf("cross-session Get error = %v, want typed not found", err)
	}
	page, err := registry.List(ctx, other, ListRunsRequest{})
	if err != nil {
		t.Fatalf("cross-session List: %v", err)
	}
	if len(page.Runs) != 0 {
		t.Fatalf("cross-session List returned %#v", page.Runs)
	}
	if _, err := registry.Get(ctx, uuid.UUID{}, run.ID); err == nil {
		t.Fatal("Get accepted zero session ID")
	}
	if _, err := registry.Get(ctx, owner, uuid.UUID{}); err == nil {
		t.Fatal("Get accepted zero run ID")
	}

	keys, err := backend.KV.Keys(ctx, "sessions/")
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "sessions/"+owner.String()+"/workflows/runs/"+run.ID.String() {
		t.Fatalf("registry key = %v", keys)
	}
	if err := storage.ValidateName(keys[0]); err != nil {
		t.Fatalf("registry constructed invalid storage key: %v", err)
	}
}

func TestRunRegistryRejectsInvalidCreateRecords(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	registry, _ := NewRunRegistry(backend.KV)
	tests := []struct {
		name   string
		mutate func(*Run)
	}{
		{name: "zero session", mutate: func(r *Run) { r.SessionID = uuid.UUID{} }},
		{name: "zero tool execution", mutate: func(r *Run) { r.ToolExecutionID = uuid.UUID{} }},
		{name: "zero workflow run", mutate: func(r *Run) { r.ID = uuid.UUID{} }},
		{name: "zero graph run", mutate: func(r *Run) { r.GraphRunID = flow.GraphRunID{} }},
		{name: "invalid definition", mutate: func(r *Run) { r.DefinitionName = "../policy" }},
		{name: "invalid digest", mutate: func(r *Run) { r.Input.Digest = "not-a-digest" }},
		{name: "future revision", mutate: func(r *Run) { r.Revision = 9 }},
		{name: "non utc timestamp", mutate: func(r *Run) { r.CreatedAt = r.CreatedAt.In(time.FixedZone("offset", 3600)) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := testRun(testUUID(17), testUUID(18))
			tc.mutate(&run)
			if _, err := registry.Create(ctx, run); err == nil {
				t.Fatal("Create accepted invalid record")
			}
		})
	}
}

func TestRunRegistryCodecRoundTripIsJSON(t *testing.T) {
	run := testRun(testUUID(19), testUUID(20))
	raw, err := encodeRunRecord(run)
	if err != nil {
		t.Fatalf("encodeRunRecord: %v", err)
	}
	if !json.Valid(raw) {
		t.Fatalf("encoded record is not JSON: %q", raw)
	}
}
