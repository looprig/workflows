package workflows

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

func TestActivityProjectionIsDeterministicOrderedAndMetadataOnly(t *testing.T) {
	run, metadata, history := activityProjectionFixture(t)
	first, err := projectActivities(run, metadata, history)
	if err != nil {
		t.Fatalf("projectActivities: %v", err)
	}
	second, err := projectActivities(run, metadata, history)
	if err != nil {
		t.Fatalf("projectActivities second: %v", err)
	}
	wantKinds := []string{"run_started", "vertex_completed", "run_interrupted", "run_resumed", "vertex_completed", "run_completed"}
	if len(first) != len(wantKinds) {
		t.Fatalf("activity count = %d, want %d: %#v", len(first), len(wantKinds), first)
	}
	seen := make(map[uuid.UUID]struct{}, len(first))
	for i := range first {
		if first[i].Kind != wantKinds[i] {
			t.Errorf("activity %d kind = %q, want %q", i, first[i].Kind, wantKinds[i])
		}
		left, err := json.Marshal(first[i])
		if err != nil {
			t.Fatal(err)
		}
		right, err := json.Marshal(second[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(left, right) {
			t.Fatalf("activity %d is not byte deterministic:\n%s\n%s", i, left, right)
		}
		if _, duplicate := seen[first[i].EventID]; duplicate {
			t.Fatalf("distinct transition reused event ID %s", first[i].EventID)
		}
		seen[first[i].EventID] = struct{}{}
		body := string(left)
		for _, forbidden := range []string{"SECRET POLICY TEXT", "MODEL OUTPUT", "/private/artifact.docx", "stack trace"} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("activity leaked %q: %s", forbidden, body)
			}
		}
	}
	if first[3].Kind != "run_resumed" || first[3].OccurredAt != history[2].Run.UpdatedAt {
		t.Fatalf("terminal resume checkpoint ordering/timestamp = %#v", first[3])
	}
	for _, item := range first {
		if item.EventID[6]>>4 != 5 || item.EventID[8]>>6 != 2 {
			t.Fatalf("event ID %s is not RFC 4122 version-5/variant-1", item.EventID)
		}
	}
}

func TestActivityTextTruncatesAtRuneBoundaries(t *testing.T) {
	label := strings.Repeat("界", 100)
	message := strings.Repeat("🙂", 200)
	if got := boundActivityText(label, maxActivityVertexLabelBytes); len(got) > maxActivityVertexLabelBytes || !utf8.ValidString(got) || got == label {
		t.Fatalf("bounded label is invalid: bytes=%d valid=%v", len(got), utf8.ValidString(got))
	}
	if got := boundActivityText(message, maxActivityMessageBytes); len(got) > maxActivityMessageBytes || !utf8.ValidString(got) || got == message {
		t.Fatalf("bounded message is invalid: bytes=%d valid=%v", len(got), utf8.ValidString(got))
	}
}

func TestActivityProjectionEmitsOneTerminalForEachOutcome(t *testing.T) {
	run, metadata, history := activityProjectionFixture(t)
	base := history[0].Run.CreatedAt
	tests := []struct {
		name   string
		status RunStatus
		flow   flow.RunStatus
		kind   string
	}{
		{name: "completed", status: RunCompleted, flow: flow.RunCompleted, kind: "run_completed"},
		{name: "cancelled", status: RunCancelled, flow: flow.RunCancelled, kind: "run_cancelled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := run
			candidate.Status = test.status
			candidate.StatusSummary = "workflow " + string(test.status)
			candidate.CheckpointRevision = 0
			checkpoint := *history[0]
			checkpoint.Run.Status = test.flow
			checkpoint.Run.UpdatedAt = base
			checkpoint.Run.CompletedAt = base
			checkpoint.Run.CancelledAt = base
			items, err := projectActivities(candidate, metadata, []*flow.Checkpoint{&checkpoint})
			if err != nil {
				t.Fatal(err)
			}
			terminals := 0
			for _, item := range items {
				if item.Kind == test.kind {
					terminals++
				}
			}
			if terminals != 1 {
				t.Fatalf("%s terminal count = %d, want 1: %#v", test.kind, terminals, items)
			}
		})
	}

	run.Status = RunFailed
	run.UpdatedAt = base
	items, err := projectActivities(run, metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Kind != "run_failed" {
		t.Fatalf("failed projection = %#v, want one run_failed", items)
	}
}

func TestActivityProjectionLabelsVerticesByStableIdentity(t *testing.T) {
	run, _, history := activityProjectionFixture(t)
	vertexA := history[1].Vertices[0].VertexID
	vertexB := history[2].Vertices[0].VertexID
	history[1].Vertices[0].VertexID, history[2].Vertices[0].VertexID = vertexB, vertexA
	metadata, err := NewMetadata(
		"source_document_extract", "v1", "safe description",
		json.RawMessage(`{"type":"object","additionalProperties":false}`), nil,
		[]VertexMetadata{NewVertexMetadataForID(vertexA, "label A"), NewVertexMetadataForID(vertexB, "label B")},
	)
	if err != nil {
		t.Fatal(err)
	}
	activities, err := projectActivities(run, metadata, history)
	if err != nil {
		t.Fatalf("projectActivities: %v", err)
	}
	labels := make(map[uuid.UUID]string)
	for _, activity := range activities {
		if activity.Kind == string(activityVertexCompleted) {
			labels[activity.VertexID] = activity.VertexLabel
		}
	}
	if labels[uuid.UUID(vertexA)] != "label A" || labels[uuid.UUID(vertexB)] != "label B" {
		t.Fatalf("vertex labels = %#v, want identity-bound labels", labels)
	}
}

func TestActivityTimeNormalizesFlowLocalTimestamps(t *testing.T) {
	local := time.Date(2026, 8, 10, 11, 0, 0, 0, time.FixedZone("EDT", -4*60*60))
	got, err := activityTime(local)
	if err != nil {
		t.Fatalf("activityTime: %v", err)
	}
	if got.Location() != time.UTC || !got.Equal(local) {
		t.Fatalf("normalized time = %v (%s), want same instant in UTC", got, got.Location())
	}
}

func activityProjectionFixture(t *testing.T) (Run, Metadata, []*flow.Checkpoint) {
	t.Helper()
	base := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	sessionID, runID := testUUID(111), testUUID(112)
	graphRunID := testGraphRunID(113)
	vertexA := flow.VertexID(testUUID(114))
	vertexB := flow.VertexID(testUUID(115))
	metadata, err := NewMetadata(
		"source_document_extract", "v1", "safe description",
		json.RawMessage(`{"type":"object","additionalProperties":false}`), nil,
		[]VertexMetadata{NewVertexMetadata("first vertex"), NewVertexMetadata("second vertex")},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{
		SessionID: sessionID, ID: runID, GraphRunID: graphRunID,
		DefinitionName: metadata.Name(), DefinitionVersion: metadata.Version(),
		Status: RunRunning, StatusSummary: "workflow resuming", CheckpointRevision: 1,
		Artifacts: []ArtifactReference{{ID: "secret_artifact", Kind: "docx", Digest: strings.Repeat("a", 64), Size: 99}},
	}
	history := []*flow.Checkpoint{
		{
			Run:   flow.GraphRunState{GraphRunID: graphRunID, Revision: 0, Status: flow.RunRunning, CreatedAt: base, UpdatedAt: base},
			State: json.RawMessage(`{"policy":"SECRET POLICY TEXT","model":"MODEL OUTPUT","path":"/private/artifact.docx"}`),
		},
		{
			Run:        flow.GraphRunState{GraphRunID: graphRunID, Revision: 1, Status: flow.RunInterrupted, UpdatedAt: base.Add(time.Minute), InterruptedAt: base.Add(time.Minute)},
			Vertices:   []flow.VertexState{{VertexID: vertexA, Step: 0, Status: flow.VertexDone, CompletedAt: base.Add(time.Minute)}},
			Interrupts: []flow.InterruptRecord{{Vertex: vertexB, Kind: flow.Errored, Cause: "stack trace"}},
		},
		{
			Run:      flow.GraphRunState{GraphRunID: graphRunID, Revision: 2, Status: flow.RunCompleted, UpdatedAt: base.Add(2 * time.Minute), CompletedAt: base.Add(2 * time.Minute)},
			Vertices: []flow.VertexState{{VertexID: vertexB, Step: 1, Status: flow.VertexDone, CompletedAt: base.Add(2 * time.Minute), Err: "stack trace"}},
			State:    json.RawMessage(`{"output":"MODEL OUTPUT"}`),
		},
	}
	return run, metadata, history
}
