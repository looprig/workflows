package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

type projectedHistoryDefinition struct {
	*supervisorTestDefinition
	checkpoints []*flow.Checkpoint
}

func (d *projectedHistoryDefinition) registeredCopy() (Definition, error) { return d, nil }

func (d *projectedHistoryDefinition) workflowCheckpointHistory(context.Context, flow.GraphRunID) ([]*flow.Checkpoint, error) {
	return d.checkpoints, nil
}

func TestSupervisorHistoryReturnsProjectedMetadataWithSafeCursor(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.createRun(t, RunCompleted, 103)
	definition := &projectedHistoryDefinition{supervisorTestDefinition: f.def}
	f.catalog = NewCatalog()
	if err := f.catalog.Register(definition); err != nil {
		t.Fatal(err)
	}
	base := f.now
	definition.checkpoints = []*flow.Checkpoint{
		{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 0, Status: flow.RunRunning, CreatedAt: base, UpdatedAt: base}},
		{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 1, Status: flow.RunCompleted, CreatedAt: base, UpdatedAt: base.Add(time.Second), CompletedAt: base.Add(time.Second)}},
	}
	s := f.supervisor(t, f.backend.Leaser)
	if err := s.Activate(context.Background(), supervisorServices(t)); err != nil {
		t.Fatal(err)
	}
	page, err := s.History(context.Background(), run.ID, 0, uuid.UUID{}, 1)
	if err != nil {
		t.Fatalf("History first page: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].Metadata.Kind != string(activityRunStarted) || page.NextRevision == nil || *page.NextRevision != 1 {
		t.Fatalf("first page = %#v, want one run_started record and cursor 1", page)
	}
	second, err := s.History(context.Background(), run.ID, *page.NextRevision, uuid.UUID{}, 1)
	if err != nil {
		t.Fatalf("History second page: %v", err)
	}
	if len(second.Records) != 1 || second.Records[0].Metadata.Kind != string(activityRunCompleted) || second.NextRevision != nil {
		t.Fatalf("second page = %#v, want terminal record without next cursor", second)
	}
	definition.checkpoints = []*flow.Checkpoint{
		{Run: flow.GraphRunState{GraphRunID: run.GraphRunID, Revision: 0, Status: flow.RunCompleted, CreatedAt: base, UpdatedAt: base, CompletedAt: base}},
	}
	split, err := s.History(context.Background(), run.ID, 0, uuid.UUID{}, 1)
	if err != nil {
		t.Fatalf("History split page: %v", err)
	}
	if len(split.Records) != 1 || split.NextRevision == nil || *split.NextRevision != 0 || split.NextEventID == nil {
		t.Fatalf("split page = %#v, want event cursor within revision zero", split)
	}
	remainder, err := s.History(context.Background(), run.ID, *split.NextRevision, *split.NextEventID, 1)
	if err != nil {
		t.Fatalf("History split remainder: %v", err)
	}
	if len(remainder.Records) != 1 || remainder.NextRevision != nil || remainder.NextEventID != nil {
		t.Fatalf("split remainder = %#v, want final activity", remainder)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
