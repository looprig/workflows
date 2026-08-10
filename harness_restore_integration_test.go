//go:build harness_integration

package workflows_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	model "github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/workflows"
	"github.com/looprig/workflows/internal/testworkflow"
	workflowtools "github.com/looprig/workflows/tools"
)

const harnessWorkflowResourceIdentity = "workflows-harness-integration-v1"

var harnessWorkflowToolNames = []string{
	"workflow_definition_list",
	"workflow_run_start",
	"workflow_run_get",
	"workflow_run_list",
	"workflow_run_resume",
	"workflow_run_cancel",
	"workflow_run_history",
}

// harnessWorkflowTool is deliberately test-only. The production workflow
// package exposes a concrete InvokableTool bundle, while Harness's public loop
// composition path consumes tool.Definition factories and requires CallPreparer
// before it runs a tool. This adapter delegates Info and InvokableRun to the
// actual workflow tool and adds the smallest pure-call preparation contract
// needed to reach it through Session.Submit. It changes no production behavior.
type harnessWorkflowTool struct {
	delegate tool.InvokableTool
	info     tool.ToolInfo
}

func (t harnessWorkflowTool) Info(context.Context) (*tool.ToolInfo, error) {
	info := t.info.Clone()
	return &info, nil
}

func (t harnessWorkflowTool) PrepareCall(context.Context, uuid.UUID, string) (tool.Request, tool.PreparedArtifact, error) {
	return tool.Request{ToolName: t.info.Name}, tool.TokenArtifact{}, nil
}

func (t harnessWorkflowTool) InvokableRun(ctx context.Context, argsJSON string) (*tool.ToolResult, error) {
	return t.delegate.InvokableRun(ctx, argsJSON)
}

var _ tool.CallPreparer = harnessWorkflowTool{}

type harnessWorkflowAccess struct{}

func (harnessWorkflowAccess) Authorize(context.Context, tool.Request) (gate.Resolution, error) {
	return gate.Resolution{Approved: true}, nil
}

type harnessWorkflowResourceStorage struct {
	root string
}

func (p harnessWorkflowResourceStorage) StorageForSession(_ context.Context, id uuid.UUID) (rig.SessionResourceStorage, error) {
	return rig.SessionResourceStorage{
		Path:     filepath.Join(p.root, id.String()),
		Identity: harnessWorkflowResourceIdentity,
	}, nil
}

type harnessWorkflowLLM struct {
	streams atomic.Int32
}

func (*harnessWorkflowLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("workflow harness integration: Invoke is unused")
}

func (l *harnessWorkflowLLM) Stream(context.Context, inference.Request) (*stream.StreamReader[content.Chunk], error) {
	call := l.streams.Add(1)
	var chunk content.Chunk
	if call == 1 {
		chunk = &content.ToolUseChunk{
			Index:     0,
			ID:        "workflow-run-start-call",
			Name:      "workflow_run_start",
			InputJSON: `{"definition_name":"counter_flow","definition_version":"v1","input":{"count":0}}`,
		}
	} else {
		chunk = &content.TextChunk{Text: "workflow run started"}
	}
	emitted := false
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if emitted {
			return nil, io.EOF
		}
		emitted = true
		return chunk, nil
	}, nil), nil
}

type harnessWorkflowFixture struct {
	rig   *rig.Rig
	store *sessionstore.Store
}

func newHarnessWorkflowFixture(t *testing.T) harnessWorkflowFixture {
	t.Helper()

	flowStore := flow.NewMemStore()
	definition, err := testworkflow.NewDefinition(flowStore)
	if err != nil {
		t.Fatalf("testworkflow.NewDefinition: %v", err)
	}
	catalog := workflows.NewCatalog()
	if err := catalog.Register(definition); err != nil {
		t.Fatalf("catalog.Register: %v", err)
	}

	workflowBackend := memstore.New()
	registry, err := workflows.NewRunRegistry(workflowBackend.KV)
	if err != nil {
		t.Fatalf("workflows.NewRunRegistry: %v", err)
	}
	inputs, err := workflows.NewInputStore(workflowBackend.Blobs)
	if err != nil {
		t.Fatalf("workflows.NewInputStore: %v", err)
	}

	workflowDefinition := tool.NewBundleDefinition(
		"workflow_bundle",
		harnessWorkflowToolNames,
		tool.RequiresProcessServices,
		func(ctx context.Context, bindings tool.Bindings) ([]tool.InvokableTool, error) {
			if bindings.Process == nil || bindings.Process.Registry == nil {
				return nil, errors.New("workflow harness integration: process registry is missing")
			}
			resource, err := bindings.Process.Registry.GetOrCreate(
				ctx,
				workflows.SupervisorResourceName,
				func(string) (tool.SessionResource, error) {
					return workflows.NewSupervisor(workflows.SupervisorConfig{
						SessionID: bindings.SessionID,
						Catalog:   catalog,
						Registry:  registry,
						Inputs:    inputs,
						Leaser:    workflowBackend.Leaser,
						Now: func() time.Time {
							return time.Date(2026, time.August, 10, 17, 0, 0, 0, time.UTC)
						},
					})
				},
			)
			if err != nil {
				return nil, err
			}
			supervisor, ok := resource.(*workflows.Supervisor)
			if !ok {
				return nil, fmt.Errorf("workflow harness integration: resource type = %T, want *workflows.Supervisor", resource)
			}
			bundle, err := workflowtools.NewBundle(workflowtools.Config{
				SessionID:  bindings.SessionID,
				Catalog:    catalog,
				Registry:   registry,
				Inputs:     inputs,
				Supervisor: supervisor,
			})
			if err != nil {
				return nil, err
			}
			wrapped := make([]tool.InvokableTool, 0, len(bundle))
			for _, delegate := range bundle {
				info, infoErr := delegate.Info(ctx)
				if infoErr != nil {
					return nil, infoErr
				}
				wrapped = append(wrapped, harnessWorkflowTool{delegate: delegate, info: info.Clone()})
			}
			return wrapped, nil
		},
	)

	llm := &harnessWorkflowLLM{}
	loopDefinition, err := loop.Define(
		loop.WithName("workflow_harness"),
		loop.WithInference(llm, model.Model{
			Provider:  "test",
			APIFormat: model.APIFormatOpenAI,
			BaseURL:   "http://localhost",
			Name:      "workflow-harness",
		}),
		loop.WithTools(workflowDefinition),
		loop.WithAccessGate(harnessWorkflowAccess{}),
		loop.WithPolicyRevision("workflow-harness-integration-v1"),
	)
	if err != nil {
		t.Fatalf("loop.Define: %v", err)
	}

	store, err := sessionstore.Open(memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	defined, err := rig.Define(
		rig.WithLoops(loopDefinition),
		rig.WithPrimers("workflow_harness"),
		rig.WithSessionStore(store),
		rig.WithSessionResourceStorage(harnessWorkflowResourceStorage{root: t.TempDir()}),
	)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	return harnessWorkflowFixture{rig: defined, store: store}
}

func collectHarnessWorkflowActivities(t *testing.T, ctx context.Context, live session.SessionController) []event.WorkflowActivity {
	t.Helper()
	subscription, err := live.SubscribeEvents(event.EventFilter{
		Enduring: event.LoopScope{All: true},
	})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	defer subscription.Close()

	if _, err := live.Submit(ctx, []content.Block{&content.TextBlock{Text: "start the counter workflow"}}); err != nil {
		t.Fatalf("Session.Submit: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var activities []event.WorkflowActivity
	turnDone := false
	for {
		select {
		case delivery, ok := <-subscription.Events():
			if !ok {
				t.Fatalf("workflow activity subscription closed: %v", subscription.Err())
			}
			switch value := delivery.Event.(type) {
			case event.WorkflowActivity:
				activities = append(activities, value)
				if turnDone && len(activities) == 4 {
					return activities
				}
			case event.TurnDone:
				turnDone = true
				if len(activities) == 4 {
					return activities
				}
			case event.TurnFailed:
				t.Fatalf("workflow harness turn failed: %v", value.Err)
			case event.TurnInterrupted:
				t.Fatal("workflow harness turn interrupted")
			}
		case <-waitCtx.Done():
			t.Fatalf("timed out waiting for Harness workflow turn: %v; activities=%v", waitCtx.Err(), workflowActivityKinds(activities))
		}
	}
}

func replayHarnessWorkflowActivities(ctx context.Context, store *sessionstore.Store, sessionID uuid.UUID) ([]event.WorkflowActivity, error) {
	replayer, err := store.OpenEventReplayer(sessionID, sessionstore.ReplayRequest{})
	if err != nil {
		return nil, err
	}
	cursor, err := replayer.Open(ctx, journal.ReplayRequest{SessionID: sessionID, From: journal.Beginning()})
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var activities []event.WorkflowActivity
	for {
		value, _, nextErr := cursor.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			return activities, nil
		}
		if nextErr != nil {
			return nil, nextErr
		}
		if activity, ok := value.(event.WorkflowActivity); ok {
			activities = append(activities, activity)
		}
	}
}

func workflowActivityWires(t *testing.T, activities []event.WorkflowActivity) [][]byte {
	t.Helper()
	wires := make([][]byte, 0, len(activities))
	for _, activity := range activities {
		wire, err := event.MarshalEvent(activity)
		if err != nil {
			t.Fatalf("event.MarshalEvent(%s): %v", activity.EventID, err)
		}
		wires = append(wires, wire)
	}
	return wires
}

func TestHarnessSessionWorkflowActivityReplayMatchesAfterRestore(t *testing.T) {
	fixture := newHarnessWorkflowFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	live, err := fixture.rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("Rig.NewSession: %v", err)
	}
	sessionID := live.SessionID()
	liveActivities := collectHarnessWorkflowActivities(t, ctx, live)
	if got := workflowActivityKinds(liveActivities); got[0] != "run_started" || got[len(got)-1] != "run_completed" {
		t.Fatalf("live workflow activity sequence = %v, want started ... completed", got)
	}
	liveWires := workflowActivityWires(t, liveActivities)

	if err := live.Shutdown(ctx); err != nil {
		t.Fatalf("live.Shutdown: %v", err)
	}

	restored, err := fixture.rig.RestoreSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("Rig.RestoreSession: %v", err)
	}
	assertRestoredSessionEventPath(t, ctx, restored)
	replayed, err := replayHarnessWorkflowActivities(ctx, fixture.store, sessionID)
	if err != nil {
		t.Fatalf("sessionstore replay after restore: %v", err)
	}
	restoredWires := workflowActivityWires(t, replayed)
	if len(restoredWires) != len(liveWires) {
		t.Fatalf("restored replay activity count = %d, live count = %d", len(restoredWires), len(liveWires))
	}
	for i := range liveWires {
		if !bytes.Equal(restoredWires[i], liveWires[i]) {
			t.Fatalf("restored replay activity %d differs from live durable activity", i)
		}
	}
	if err := restored.Shutdown(ctx); err != nil {
		t.Fatalf("restored.Shutdown: %v", err)
	}
}

func assertRestoredSessionEventPath(t *testing.T, ctx context.Context, restored session.SessionController) {
	t.Helper()
	subscription, err := restored.SubscribeEvents(event.EventFilter{
		Enduring: event.LoopScope{All: true},
	})
	if err != nil {
		t.Fatalf("restored SubscribeEvents: %v", err)
	}
	defer subscription.Close()
	if _, err := restored.Submit(ctx, []content.Block{&content.TextBlock{Text: "continue after restore"}}); err != nil {
		t.Fatalf("restored Session.Submit: %v", err)
	}
	for {
		select {
		case delivery, ok := <-subscription.Events():
			if !ok {
				t.Fatalf("restored event subscription closed: %v", subscription.Err())
			}
			switch value := delivery.Event.(type) {
			case event.TurnDone:
				return
			case event.TurnFailed:
				t.Fatalf("restored Harness turn failed: %v", value.Err)
			case event.TurnInterrupted:
				t.Fatal("restored Harness turn interrupted")
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for restored Harness event path: %v", ctx.Err())
		}
	}
}

func workflowActivityKinds(activities []event.WorkflowActivity) []string {
	kinds := make([]string, len(activities))
	for i, activity := range activities {
		kinds[i] = string(activity.Kind)
	}
	return kinds
}
