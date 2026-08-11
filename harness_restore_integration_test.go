//go:build harness_integration

package workflows_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/hook"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	model "github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/workflows"
	"github.com/looprig/workflows/internal/testworkflow"
	workflowtools "github.com/looprig/workflows/tools"
)

const harnessWorkflowResourceIdentity = "workflows-harness-integration-v1"

func harnessWorkflowNow() time.Time {
	return time.Date(2026, time.August, 10, 17, 0, 0, 0, time.UTC)
}

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
	streams        atomic.Int32
	definitionName string
}

func (*harnessWorkflowLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("workflow harness integration: Invoke is unused")
}

func (l *harnessWorkflowLLM) Stream(context.Context, inference.Request) (*stream.StreamReader[content.Chunk], error) {
	call := l.streams.Add(1)
	var chunk content.Chunk
	if call == 1 {
		definitionName := l.definitionName
		if definitionName == "" {
			definitionName = "counter_flow"
		}
		chunk = &content.ToolUseChunk{
			Index:     0,
			ID:        "workflow-run-start-call",
			Name:      "workflow_run_start",
			InputJSON: fmt.Sprintf(`{"definition_name":%q,"definition_version":"v1","input":{"count":0}}`, definitionName),
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
	rig        *rig.Rig
	store      *sessionstore.Store
	registry   *workflows.RunRegistry
	llm        *harnessWorkflowLLM
	supervisor func() *workflows.Supervisor
}

type harnessWorkflowFixtureOptions struct {
	hooks              hook.Set
	definitionName     string
	buildDefinition    func(flow.CheckpointStore) (workflows.Definition, error)
	wrapWorkflowKV     func(storage.KV) storage.KV
	wrapWorkflowLeaser func(storage.Leaser) storage.Leaser
}

func newHarnessWorkflowBackend(t *testing.T, name string) *storage.Composite {
	t.Helper()
	backend := memstore.New()
	if backend == nil {
		t.Fatalf("%s backend: memstore.New returned nil", name)
	}
	if backend.Ledger == nil || backend.Leaser == nil || backend.KV == nil || backend.Blobs == nil {
		t.Fatalf("%s backend: incomplete local memstore (ledger_nil=%t leaser_nil=%t kv_nil=%t blobs_nil=%t)",
			name, backend.Ledger == nil, backend.Leaser == nil, backend.KV == nil, backend.Blobs == nil)
	}
	return backend
}

func newHarnessWorkflowFixture(t *testing.T) harnessWorkflowFixture {
	return newHarnessWorkflowFixtureWithOptions(t, harnessWorkflowFixtureOptions{})
}

func newHarnessWorkflowFixtureWithOptions(t *testing.T, options harnessWorkflowFixtureOptions) harnessWorkflowFixture {
	t.Helper()

	flowStore := flow.NewMemStore()
	buildDefinition := options.buildDefinition
	if buildDefinition == nil {
		buildDefinition = func(store flow.CheckpointStore) (workflows.Definition, error) {
			return testworkflow.NewDefinition(store)
		}
	}
	definition, err := buildDefinition(flowStore)
	if err != nil {
		t.Fatalf("testworkflow.NewDefinition: %v", err)
	}
	catalog := workflows.NewCatalog()
	if err := catalog.Register(definition); err != nil {
		t.Fatalf("catalog.Register: %v", err)
	}

	workflowBackend := newHarnessWorkflowBackend(t, "workflow")
	workflowKV := storage.KV(workflowBackend.KV)
	if options.wrapWorkflowKV != nil {
		workflowKV = options.wrapWorkflowKV(workflowKV)
	}
	registry, err := workflows.NewRunRegistry(workflowKV)
	if err != nil {
		t.Fatalf("workflows.NewRunRegistry: %v", err)
	}
	inputs, err := workflows.NewInputStore(workflowBackend.Blobs)
	if err != nil {
		t.Fatalf("workflows.NewInputStore: %v", err)
	}
	workflowLeaser := storage.Leaser(workflowBackend.Leaser)
	if options.wrapWorkflowLeaser != nil {
		workflowLeaser = options.wrapWorkflowLeaser(workflowLeaser)
	}

	var supervisor atomic.Pointer[workflows.Supervisor]
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
					created, createErr := workflows.NewSupervisor(workflows.SupervisorConfig{
						SessionID: bindings.SessionID,
						Catalog:   catalog,
						Registry:  registry,
						Inputs:    inputs,
						Leaser:    workflowLeaser,
						Now:       harnessWorkflowNow,
					})
					if createErr == nil {
						supervisor.Store(created)
					}
					return created, createErr
				},
			)
			if err != nil {
				return nil, err
			}
			activeSupervisor, ok := resource.(*workflows.Supervisor)
			if !ok {
				return nil, fmt.Errorf("workflow harness integration: resource type = %T, want *workflows.Supervisor", resource)
			}
			bundle, err := workflowtools.NewBundle(workflowtools.Config{
				SessionID:  bindings.SessionID,
				Catalog:    catalog,
				Registry:   registry,
				Inputs:     inputs,
				Supervisor: activeSupervisor,
				Now:        harnessWorkflowNow,
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

	store, err := sessionstore.Open(newHarnessWorkflowBackend(t, "session"))
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	defined, err := rig.Define(
		rig.WithLoops(loopDefinition),
		rig.WithPrimers("workflow_harness"),
		rig.WithSessionStore(store),
		rig.WithSessionResourceStorage(harnessWorkflowResourceStorage{root: t.TempDir()}),
		rig.WithHooks(options.hooks),
	)
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	llm.definitionName = options.definitionName
	return harnessWorkflowFixture{rig: defined, store: store, registry: registry, llm: llm, supervisor: supervisor.Load}
}

func collectHarnessWorkflowActivities(t *testing.T, ctx context.Context, live session.SessionController, store *sessionstore.Store, llm *harnessWorkflowLLM, supervisor func() *workflows.Supervisor) []event.WorkflowActivity {
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
			case event.TurnRejected:
				t.Fatalf("Harness workflow input rejected: reason=%v command_id=%s", value.Reason, value.Cause.CommandID)
			case event.InputCancelled:
				t.Fatalf("Harness workflow input cancelled: reason=%v command_id=%s", value.Reason, value.Cause.CommandID)
			case event.TurnDone:
				turnDone = true
				supervisor := supervisor()
				if supervisor == nil {
					t.Fatal("workflow supervisor was not created before the Harness turn completed")
				}
				if err := supervisor.WaitIdle(waitCtx); err != nil {
					t.Fatalf("workflow supervisor completed with error: %v", err)
				}
				if len(activities) == 4 {
					return activities
				}
			case event.TurnFailed:
				t.Fatalf("workflow harness turn failed: %v", value.Err)
			case event.TurnInterrupted:
				t.Fatal("workflow harness turn interrupted")
			}
		case <-waitCtx.Done():
			diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), time.Second)
			events, replayErr := replayHarnessEvents(diagnosticCtx, store, live.SessionID())
			diagnosticCancel()
			var supervisorErr error
			if supervisor := supervisor(); supervisor != nil {
				supervisorErr = supervisor.LastError()
			}
			t.Fatalf("timed out waiting for Harness workflow turn: %v; activities=%v; inference_streams=%d; durable_events=%v; replay_error=%v; supervisor_error=%v", waitCtx.Err(), workflowActivityKinds(activities), llm.streams.Load(), harnessEventKinds(events), replayErr, supervisorErr)
		}
	}
}

func replayHarnessEvents(ctx context.Context, store *sessionstore.Store, sessionID uuid.UUID) ([]event.Event, error) {
	replayer, err := store.OpenEventReplayer(sessionID, sessionstore.ReplayRequest{})
	if err != nil {
		return nil, err
	}
	cursor, err := replayer.Open(ctx, journal.ReplayRequest{SessionID: sessionID, From: journal.Beginning()})
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	var events []event.Event
	for {
		value, _, nextErr := cursor.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			return events, nil
		}
		if nextErr != nil {
			return nil, nextErr
		}
		events = append(events, value)
	}
}

func harnessEventKinds(events []event.Event) []string {
	kinds := make([]string, len(events))
	for i, value := range events {
		kinds[i] = fmt.Sprintf("%T", value)
	}
	return kinds
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
	liveActivities := collectHarnessWorkflowActivities(t, ctx, live, fixture.store, fixture.llm, fixture.supervisor)
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
	assertRestoredSessionEventPath(t, ctx, restored, fixture.store, fixture.llm, fixture.supervisor)
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

func assertRestoredSessionEventPath(t *testing.T, ctx context.Context, restored session.SessionController, store *sessionstore.Store, llm *harnessWorkflowLLM, supervisor func() *workflows.Supervisor) {
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
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		select {
		case delivery, ok := <-subscription.Events():
			if !ok {
				t.Fatalf("restored event subscription closed: %v", subscription.Err())
			}
			switch value := delivery.Event.(type) {
			case event.TurnDone:
				return
			case event.TurnRejected:
				t.Fatalf("restored Harness input rejected: reason=%v command_id=%s", value.Reason, value.Cause.CommandID)
			case event.InputCancelled:
				t.Fatalf("restored Harness input cancelled: reason=%v command_id=%s", value.Reason, value.Cause.CommandID)
			case event.TurnFailed:
				t.Fatalf("restored Harness turn failed: %v", value.Err)
			case event.TurnInterrupted:
				t.Fatal("restored Harness turn interrupted")
			}
		case <-waitCtx.Done():
			diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), time.Second)
			events, replayErr := replayHarnessEvents(diagnosticCtx, store, restored.SessionID())
			diagnosticCancel()
			var supervisorErr error
			if current := supervisor(); current != nil {
				supervisorErr = current.LastError()
			}
			t.Fatalf("timed out waiting for restored Harness event path: %v; durable_events=%v; replay_error=%v; inference_streams=%d; supervisor_error=%v", waitCtx.Err(), harnessEventKinds(events), replayErr, llm.streams.Load(), supervisorErr)
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

var errHarnessWorkflowActivityCrash = errors.New("test: harness workflow activity durable append crash")

// harnessWorkflowActivityFaultInjector is deliberately limited to the tagged
// Harness integration binary. It identifies the sealed WorkflowActivity event
// at the journal boundary, injects once for one stable activity ID, and keeps
// seeing retries without counting the same ID twice. That makes the matrix
// target the same source activity before and after restore instead of targeting
// a process-local append ordinal.
type harnessWorkflowActivityFaultInjector struct {
	target int
	phase  hook.JournalAppendFaultPhase

	mu       sync.Mutex
	seen     map[string]int
	observed []string
	injected bool
}

func (i *harnessWorkflowActivityFaultInjector) begin(ctx context.Context, call hook.Call) (context.Context, hook.FinishFunc) {
	if call.JournalAppend == nil || call.JournalAppend.Family != hook.RecordEvent || call.JournalAppend.EventType != "WorkflowActivity" {
		return ctx, nil
	}

	i.mu.Lock()
	if i.seen == nil {
		i.seen = make(map[string]int)
	}
	recordID := call.JournalAppend.RecordID
	ordinal, alreadySeen := i.seen[recordID]
	if !alreadySeen {
		ordinal = len(i.seen)
		i.seen[recordID] = ordinal
		i.observed = append(i.observed, recordID)
	}
	shouldInject := !alreadySeen && !i.injected && ordinal == i.target
	if shouldInject {
		i.injected = true
	}
	i.mu.Unlock()

	if !shouldInject {
		return ctx, nil
	}
	return hook.WithJournalAppendFault(ctx, hook.JournalAppendFault{
		Phase: i.phase,
		Err:   errHarnessWorkflowActivityCrash,
	}), nil
}

func (i *harnessWorkflowActivityFaultInjector) hooked() hook.Set {
	return hook.Set{Around: []hook.Around{{
		Operation: hook.OperationJournalAppend,
		Begin:     i.begin,
	}}}
}

func (i *harnessWorkflowActivityFaultInjector) snapshot() (observed int, injected bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.observed), i.injected
}

func waitHarnessWorkflowRun(t *testing.T, ctx context.Context, fixture harnessWorkflowFixture, sessionID uuid.UUID) workflows.Run {
	t.Helper()
	for {
		page, err := fixture.registry.List(ctx, sessionID, workflows.ListRunsRequest{Limit: 10})
		if err != nil {
			t.Fatalf("list live Harness workflow runs: %v", err)
		}
		if len(page.Runs) == 1 {
			return page.Runs[0]
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for live Harness workflow run: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitHarnessWorkflowSupervisorError(t *testing.T, ctx context.Context, fixture harnessWorkflowFixture) error {
	t.Helper()
	for {
		if supervisor := fixture.supervisor(); supervisor != nil {
			if err := supervisor.LastError(); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for live Harness supervisor error: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestHarnessWorkflowActivityBeforeAfterDurableAppendRecoveryMatrix(t *testing.T) {
	for _, phase := range []struct {
		name  string
		value hook.JournalAppendFaultPhase
	}{
		{name: "before durable append", value: hook.JournalAppendFaultBefore},
		{name: "after durable append", value: hook.JournalAppendFaultAfter},
	} {
		for target := 0; target < 4; target++ {
			phase, target := phase, target
			t.Run(fmt.Sprintf("%s/activity_%d", phase.name, target), func(t *testing.T) {
				injector := &harnessWorkflowActivityFaultInjector{target: target, phase: phase.value}
				fixture := newHarnessWorkflowFixtureWithOptions(t, harnessWorkflowFixtureOptions{hooks: injector.hooked()})
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()

				live, err := fixture.rig.NewSession(ctx)
				if err != nil {
					t.Fatalf("Rig.NewSession: %v", err)
				}
				sessionID := live.SessionID()
				submitDone := make(chan error, 1)
				go func() {
					_, submitErr := live.Submit(ctx, []content.Block{&content.TextBlock{Text: "start the counter workflow"}})
					submitDone <- submitErr
				}()

				initial := waitHarnessWorkflowRun(t, ctx, fixture, sessionID)
				if initial.Status != workflows.RunRunning && initial.Status != workflows.RunCompleted {
					t.Fatalf("live run status before injected crash = %s, want running or completed", initial.Status)
				}
				for {
					_, injected := injector.snapshot()
					if injected {
						break
					}
					select {
					case <-ctx.Done():
						observed, _ := injector.snapshot()
						t.Fatalf("timed out before injecting %s at activity %d after observing %d activities: %v", phase.name, target, observed, ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
				observed, injected := injector.snapshot()
				if !injected || observed <= target {
					t.Fatalf("fault injection state = observed %d injected %t, want target activity %d", observed, injected, target)
				}
				if supervisorErr := waitHarnessWorkflowSupervisorError(t, ctx, fixture); supervisorErr == nil {
					t.Fatal("live Harness supervisor error = nil after injected durable activity fault")
				}
				if err := live.Shutdown(ctx); err != nil {
					t.Logf("live Harness shutdown after injected persistence fault: %v", err)
				}
				select {
				case <-submitDone:
				default:
				}

				restored, err := fixture.rig.RestoreSession(ctx, sessionID)
				if err != nil {
					t.Fatalf("Rig.RestoreSession after %s: %v", phase.name, err)
				}
				restoredSupervisor := fixture.supervisor()
				if restoredSupervisor == nil || restoredSupervisor.SessionID() != sessionID {
					t.Fatalf("restored supervisor = %#v, want live session-owned supervisor", restoredSupervisor)
				}
				if err := restoredSupervisor.WaitIdle(ctx); err != nil {
					t.Fatalf("restored supervisor WaitIdle: %v", err)
				}
				final, err := fixture.registry.Get(ctx, sessionID, initial.ID)
				if err != nil {
					t.Fatalf("read recovered workflow run: %v", err)
				}
				if final.Status != workflows.RunCompleted || final.ActivityCursor != 5 {
					t.Fatalf("recovered run = %#v, want completed with cursor 5 for the five-checkpoint counter flow", final)
				}
				activities, err := replayHarnessWorkflowActivities(ctx, fixture.store, sessionID)
				if err != nil {
					t.Fatalf("replay recovered Harness workflow activities: %v", err)
				}
				if got := workflowActivityKinds(activities); len(got) != 4 || got[0] != "run_started" || got[len(got)-1] != "run_completed" {
					t.Fatalf("recovered live Harness activity sequence = %v, want four projected activities", got)
				}
				observedAfterRestore, _ := injector.snapshot()
				if observedAfterRestore != 4 {
					t.Fatalf("live Harness publisher observations after restore = %d, want four unique activities", observedAfterRestore)
				}
				seen := make(map[uuid.UUID]struct{}, len(activities))
				for _, activity := range activities {
					if _, duplicate := seen[activity.EventID]; duplicate {
						t.Fatalf("recovered Harness activity duplicated EventID %s", activity.EventID)
					}
					seen[activity.EventID] = struct{}{}
				}
				if err := restored.Shutdown(ctx); err != nil {
					t.Fatalf("restored Harness shutdown: %v", err)
				}
			})
		}
	}
}

type harnessWorkflowCursorConflictKV struct {
	inner storage.KV

	mu               sync.Mutex
	injected         bool
	puts             int
	putCalls         int
	cursorCandidates int
	currentCursor    uint64
	nextCursor       uint64
}

type harnessWorkflowRunProgressEnvelope struct {
	Run struct {
		ActivityCursor uint64 `json:"activity_cursor"`
	} `json:"run"`
}

func (k *harnessWorkflowCursorConflictKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	return k.inner.Get(ctx, key)
}

func (k *harnessWorkflowCursorConflictKV) Put(ctx context.Context, key string, expectedRevision uint64, value []byte) (uint64, error) {
	k.mu.Lock()
	k.putCalls++
	k.mu.Unlock()
	if expectedRevision != 0 {
		currentRaw, _, getErr := k.inner.Get(ctx, key)
		if getErr == nil {
			var current, next harnessWorkflowRunProgressEnvelope
			if json.Unmarshal(currentRaw, &current) == nil && json.Unmarshal(value, &next) == nil && next.Run.ActivityCursor > current.Run.ActivityCursor {
				k.mu.Lock()
				k.cursorCandidates++
				k.currentCursor = current.Run.ActivityCursor
				k.nextCursor = next.Run.ActivityCursor
				if !k.injected {
					k.injected = true
					k.puts++
					k.mu.Unlock()
					return 0, &storage.ConflictError{Name: key, Expected: expectedRevision}
				}
				k.mu.Unlock()
			}
		}
	}
	return k.inner.Put(ctx, key, expectedRevision, value)
}

func (k *harnessWorkflowCursorConflictKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	return k.inner.Keys(ctx, prefix)
}

func (k *harnessWorkflowCursorConflictKV) Delete(ctx context.Context, key string) error {
	return k.inner.Delete(ctx, key)
}

func (k *harnessWorkflowCursorConflictKV) injectedConflict() (bool, int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.injected, k.puts
}

func (k *harnessWorkflowCursorConflictKV) diagnostic() (putCalls, candidates int, current, next uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.putCalls, k.cursorCandidates, k.currentCursor, k.nextCursor
}

var _ storage.KV = (*harnessWorkflowCursorConflictKV)(nil)

func TestHarnessWorkflowActivityCursorCASConflictRecoversFromLivePublisher(t *testing.T) {
	observer := &harnessWorkflowActivityFaultInjector{target: -1}
	conflicts := &harnessWorkflowCursorConflictKV{}
	fixture := newHarnessWorkflowFixtureWithOptions(t, harnessWorkflowFixtureOptions{
		hooks: observer.hooked(),
		wrapWorkflowKV: func(inner storage.KV) storage.KV {
			conflicts.inner = inner
			return conflicts
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	live, err := fixture.rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("Rig.NewSession: %v", err)
	}
	sessionID := live.SessionID()
	go func() {
		_, _ = live.Submit(ctx, []content.Block{&content.TextBlock{Text: "start the counter workflow"}})
	}()
	initial := waitHarnessWorkflowRun(t, ctx, fixture, sessionID)
	for {
		injected, puts := conflicts.injectedConflict()
		if injected {
			if puts != 1 {
				t.Fatalf("injected cursor conflicts = %d, want exactly one", puts)
			}
			break
		}
		select {
		case <-ctx.Done():
			putCalls, candidates, current, next := conflicts.diagnostic()
			observed, _ := observer.snapshot()
			var supervisorErr error
			if supervisor := fixture.supervisor(); supervisor != nil {
				supervisorErr = supervisor.LastError()
			}
			t.Fatalf("timed out waiting for live cursor CAS conflict for run %s: puts=%d cursor_candidates=%d current_cursor=%d next_cursor=%d publisher_activities=%d supervisor=%v: %v", initial.ID, putCalls, candidates, current, next, observed, supervisorErr, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := waitHarnessWorkflowSupervisorError(t, ctx, fixture); err == nil {
		t.Fatal("live supervisor error = nil after injected cursor CAS conflict")
	}
	if err := live.Shutdown(ctx); err != nil {
		t.Logf("live Harness shutdown after cursor CAS conflict: %v", err)
	}

	restored, err := fixture.rig.RestoreSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("Rig.RestoreSession after cursor CAS conflict: %v", err)
	}
	if supervisor := fixture.supervisor(); supervisor == nil || supervisor.SessionID() != sessionID {
		t.Fatalf("restored supervisor = %#v, want session-owned supervisor", fixture.supervisor())
	}
	if err := fixture.supervisor().WaitIdle(ctx); err != nil {
		t.Fatalf("restored supervisor WaitIdle after cursor CAS conflict: %v", err)
	}
	final, err := fixture.registry.Get(ctx, sessionID, initial.ID)
	if err != nil {
		t.Fatalf("read cursor-conflict recovery run: %v", err)
	}
	if final.Status != workflows.RunCompleted || final.ActivityCursor != 5 {
		t.Fatalf("cursor-conflict recovery run = %#v, want completed with cursor 5", final)
	}
	activities, err := replayHarnessWorkflowActivities(ctx, fixture.store, sessionID)
	if err != nil {
		t.Fatalf("replay cursor-conflict Harness activities: %v", err)
	}
	observed, _ := observer.snapshot()
	if len(activities) != 4 || observed != 4 {
		t.Fatalf("cursor-conflict live publisher observations = %d journal activities = %d, want four each", observed, len(activities))
	}
	seen := make(map[uuid.UUID]struct{}, len(activities))
	for _, activity := range activities {
		if _, duplicate := seen[activity.EventID]; duplicate {
			t.Fatalf("cursor-conflict replay duplicated activity %s", activity.EventID)
		}
		seen[activity.EventID] = struct{}{}
	}
	if err := restored.Shutdown(ctx); err != nil {
		t.Fatalf("restored Harness shutdown after cursor CAS conflict: %v", err)
	}
}

// harnessWorkflowLeaseController is a tagged integration-only wrapper around
// the real session leaser. It closes only the wrapper's Lost channel when the
// test injects lease loss; Supervisor.Shutdown still performs the real lease
// release against the wrapped backend.
type harnessWorkflowLeaseController struct {
	inner storage.Leaser

	mu      sync.Mutex
	current *harnessWorkflowControlledLease
}

func (l *harnessWorkflowLeaseController) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	lease, err := l.inner.Acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	controlled := &harnessWorkflowControlledLease{inner: lease, lost: make(chan struct{})}
	l.mu.Lock()
	l.current = controlled
	l.mu.Unlock()
	return controlled, nil
}

func (l *harnessWorkflowLeaseController) lose() bool {
	l.mu.Lock()
	current := l.current
	l.mu.Unlock()
	if current == nil {
		return false
	}
	current.lose()
	return true
}

func (l *harnessWorkflowLeaseController) acquired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current != nil
}

type harnessWorkflowControlledLease struct {
	inner storage.Lease
	lost  chan struct{}
	once  sync.Once
}

func (l *harnessWorkflowControlledLease) Epoch() uint64         { return l.inner.Epoch() }
func (l *harnessWorkflowControlledLease) Lost() <-chan struct{} { return l.lost }
func (l *harnessWorkflowControlledLease) lose()                 { l.once.Do(func() { close(l.lost) }) }
func (l *harnessWorkflowControlledLease) Release(ctx context.Context) error {
	l.lose()
	return l.inner.Release(ctx)
}

var _ storage.Leaser = (*harnessWorkflowLeaseController)(nil)
var _ storage.Lease = (*harnessWorkflowControlledLease)(nil)

func TestHarnessSupervisorLeaseLossStopsLiveWorkflowOwner(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	leaseController := &harnessWorkflowLeaseController{}
	observer := &harnessWorkflowActivityFaultInjector{target: -1}
	fixture := newHarnessWorkflowFixtureWithOptions(t, harnessWorkflowFixtureOptions{
		hooks: observer.hooked(),
		buildDefinition: func(store flow.CheckpointStore) (workflows.Definition, error) {
			return testworkflow.NewDefinitionWithGateAndSignal(store, gate, entered)
		},
		wrapWorkflowLeaser: func(inner storage.Leaser) storage.Leaser {
			leaseController.inner = inner
			return leaseController
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	live, err := fixture.rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("Rig.NewSession: %v", err)
	}
	sessionID := live.SessionID()
	submitDone := make(chan error, 1)
	go func() {
		_, submitErr := live.Submit(ctx, []content.Block{&content.TextBlock{Text: "start the counter workflow"}})
		submitDone <- submitErr
	}()

	initial := waitHarnessWorkflowRun(t, ctx, fixture, sessionID)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for live workflow to enter its gated vertex: %v", ctx.Err())
	}
	var running *workflows.Run
	for {
		candidate, getErr := fixture.registry.Get(ctx, sessionID, initial.ID)
		if getErr != nil {
			t.Fatalf("read gated live workflow run: %v", getErr)
		}
		if candidate.Status == workflows.RunRunning {
			running = candidate
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for gated workflow status, got %s: %v", candidate.Status, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	supervisor := fixture.supervisor()
	if supervisor == nil || supervisor.SessionID() != sessionID {
		t.Fatalf("live Harness supervisor = %#v, want session-owned supervisor", supervisor)
	}
	if !leaseController.acquired() {
		t.Fatal("live Harness supervisor did not acquire the wrapped session lease")
	}
	if !leaseController.lose() {
		t.Fatal("lease-loss injection did not reach the live session lease")
	}
	if err := supervisor.WaitIdle(ctx); err != nil {
		t.Fatalf("supervisor reported an error after ownership loss: %v", err)
	}
	if got := supervisor.LastError(); got != nil {
		t.Fatalf("supervisor LastError after ownership loss = %v, want nil", got)
	}

	after, err := fixture.registry.Get(ctx, sessionID, running.ID)
	if err != nil {
		t.Fatalf("read workflow after supervisor lease loss: %v", err)
	}
	if after.Status != workflows.RunRunning || after.CancelRequested || after.ActivityCursor != 0 {
		t.Fatalf("workflow after supervisor lease loss = %#v, want running with no cancellation or published activities", after)
	}
	activities, err := replayHarnessWorkflowActivities(ctx, fixture.store, sessionID)
	if err != nil {
		t.Fatalf("replay Harness activities after supervisor lease loss: %v", err)
	}
	for _, activity := range activities {
		if activity.Kind == event.WorkflowActivityKind("run_completed") || activity.Kind == event.WorkflowActivityKind("run_cancelled") {
			t.Fatalf("workflow activity %q was published after supervisor lease loss", activity.Kind)
		}
	}
	if observed, _ := observer.snapshot(); observed != len(activities) {
		t.Fatalf("live publisher observation count = %d, journal activity count = %d", observed, len(activities))
	}
	if err := live.Shutdown(ctx); err != nil {
		t.Fatalf("live Harness shutdown after supervisor lease loss: %v", err)
	}
	select {
	case <-submitDone:
	case <-ctx.Done():
		t.Fatalf("live Harness submit did not finish after supervisor lease loss: %v", ctx.Err())
	}
}

func TestHarnessSupervisorCancellationConflictExhaustionUsesLivePublisher(t *testing.T) {
	definition, err := workflows.NewHarnessCancellationConflictDefinition()
	if err != nil {
		t.Fatalf("NewHarnessCancellationConflictDefinition: %v", err)
	}
	observer := &harnessWorkflowActivityFaultInjector{target: -1}
	fixture := newHarnessWorkflowFixtureWithOptions(t, harnessWorkflowFixtureOptions{
		hooks:          observer.hooked(),
		definitionName: "cancel_conflict_flow",
		buildDefinition: func(flow.CheckpointStore) (workflows.Definition, error) {
			return definition, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	live, err := fixture.rig.NewSession(ctx)
	if err != nil {
		t.Fatalf("Rig.NewSession: %v", err)
	}
	sessionID := live.SessionID()
	submitDone := make(chan error, 1)
	go func() {
		_, submitErr := live.Submit(ctx, []content.Block{&content.TextBlock{Text: "start the counter workflow"}})
		submitDone <- submitErr
	}()
	initial := waitHarnessWorkflowRun(t, ctx, fixture, sessionID)
	supervisor := fixture.supervisor()
	if supervisor == nil || supervisor.SessionID() != sessionID {
		t.Fatalf("live Harness supervisor = %#v, want session-owned supervisor", supervisor)
	}
	if err := supervisor.WaitIdle(ctx); err != nil {
		t.Fatalf("live supervisor initial activity reconciliation: %v", err)
	}
	before, err := fixture.registry.Get(ctx, sessionID, initial.ID)
	if err != nil {
		t.Fatalf("read workflow before cancellation conflicts: %v", err)
	}
	if before.Status != workflows.RunRunning || before.ActivityCursor != 1 {
		t.Fatalf("workflow before cancellation conflicts = %#v, want running with one published checkpoint activity", before)
	}
	activitiesBefore, err := replayHarnessWorkflowActivities(ctx, fixture.store, sessionID)
	if err != nil {
		t.Fatalf("replay Harness activities before cancellation conflicts: %v", err)
	}
	if len(activitiesBefore) != 1 || activitiesBefore[0].Kind != event.WorkflowActivityRunStarted {
		t.Fatalf("live pre-cancellation activities = %v, want one run_started activity", workflowActivityKinds(activitiesBefore))
	}
	observedBefore, _ := observer.snapshot()
	if observedBefore != len(activitiesBefore) {
		t.Fatalf("live pre-cancellation publisher observations = %d, journal activities = %d", observedBefore, len(activitiesBefore))
	}
	// This is the narrow live Harness seam for Supervisor retry exhaustion. It
	// deliberately does not claim to inject Flow Runner.Cancel's own checkpoint
	// append; that lower-level append boundary remains outside this Harness test.
	cancelErr := supervisor.Cancel(ctx, initial.ID, "integration cancellation conflict exhaustion")
	if cancelErr == nil {
		t.Fatal("live Supervisor.Cancel error = nil, want bounded revision-conflict exhaustion")
	}
	if !errors.Is(cancelErr, workflows.ErrAdoption) {
		t.Fatalf("live Supervisor.Cancel error = %v, want adoption exhaustion", cancelErr)
	}
	if got := definition.CancelAttempts(); got != 8 {
		t.Fatalf("live Supervisor cancellation attempts = %d, want exactly eight", got)
	}
	if err := supervisor.WaitIdle(ctx); err != nil {
		t.Fatalf("live supervisor WaitIdle after cancellation conflict exhaustion: %v", err)
	}
	after, err := fixture.registry.Get(ctx, sessionID, initial.ID)
	if err != nil {
		t.Fatalf("read workflow after cancellation conflict exhaustion: %v", err)
	}
	if after.Status != workflows.RunRunning || !after.CancelRequested || after.ActivityCursor != before.ActivityCursor {
		t.Fatalf("workflow after cancellation conflict exhaustion = %#v, want running with durable cancel intent and unchanged cursor", after)
	}
	activities, err := replayHarnessWorkflowActivities(ctx, fixture.store, sessionID)
	if err != nil {
		t.Fatalf("replay Harness activities after cancellation conflict exhaustion: %v", err)
	}
	observed, _ := observer.snapshot()
	if observed != len(activities) || len(activities) != len(activitiesBefore) {
		t.Fatalf("live publisher observations = %d, journal activities = %d, want unchanged live activity stream of %d", observed, len(activities), len(activitiesBefore))
	}
	seen := make(map[uuid.UUID]struct{}, len(activities))
	for _, activity := range activities {
		if activity.Kind == event.WorkflowActivityRunCancelled || activity.Kind == event.WorkflowActivityRunCompleted {
			t.Fatalf("terminal activity %q published despite cancellation conflict exhaustion", activity.Kind)
		}
		if _, duplicate := seen[activity.EventID]; duplicate {
			t.Fatalf("cancellation conflict replay duplicated activity %s", activity.EventID)
		}
		seen[activity.EventID] = struct{}{}
	}
	if err := live.Shutdown(ctx); err != nil {
		t.Fatalf("live Harness shutdown after cancellation conflict exhaustion: %v", err)
	}
	select {
	case <-submitDone:
	case <-ctx.Done():
		t.Fatalf("live Harness submit did not finish after cancellation conflict exhaustion: %v", ctx.Err())
	}
}
