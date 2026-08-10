package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/workflows"
)

func TestCancelIsIdempotentForCancelledRun(t *testing.T) {
	run := &workflows.Run{SessionID: testID(10), ID: testID(11), Status: workflows.RunCancelled}
	registry := registryStub{get: func(context.Context, uuid.UUID, uuid.UUID) (*workflows.Run, error) { return run, nil }}
	control := &controlStub{}
	candidate := &runCancelTool{boundRuntime: &boundRuntime{sessionID: run.SessionID, registry: registry, supervisor: control}}
	result, err := candidate.InvokableRun(context.Background(), `{"run_id":"`+run.ID.String()+`","reason":"already done"}`)
	if err != nil {
		t.Fatalf("InvokableRun() error = %v", err)
	}
	if control.cancelCalls != 0 {
		t.Fatalf("Cancel calls = %d, want 0", control.cancelCalls)
	}
	var got cancelResult
	decodeToolResult(t, result, &got)
	if got.RunStatus != string(workflows.RunCancelled) || !got.Idempotent {
		t.Fatalf("result = %+v", got)
	}
}

func TestCancelFailsClosedForOtherTerminalRun(t *testing.T) {
	run := &workflows.Run{SessionID: testID(12), ID: testID(13), Status: workflows.RunCompleted}
	registry := registryStub{get: func(context.Context, uuid.UUID, uuid.UUID) (*workflows.Run, error) { return run, nil }}
	candidate := &runCancelTool{boundRuntime: &boundRuntime{sessionID: run.SessionID, registry: registry, supervisor: &controlStub{}}}
	if _, err := candidate.InvokableRun(context.Background(), `{"run_id":"`+run.ID.String()+`"}`); err == nil {
		t.Fatal("InvokableRun() error = nil, want terminal conflict")
	}
}

func decodeToolResult(t *testing.T, result *tool.ToolResult, target any) {
	t.Helper()
	if result == nil || len(result.Content) != 1 {
		t.Fatalf("result = %#v", result)
	}
	block, ok := result.Content[0].(*content.TextBlock)
	if !ok {
		t.Fatalf("result block = %T", result.Content[0])
	}
	if err := json.Unmarshal([]byte(block.Text), target); err != nil {
		t.Fatal(err)
	}
}

type registryStub struct {
	get    func(context.Context, uuid.UUID, uuid.UUID) (*workflows.Run, error)
	create func(context.Context, workflows.Run) (*workflows.Run, error)
	list   func(context.Context, uuid.UUID, workflows.ListRunsRequest) (workflows.RunPage, error)
	cas    func(context.Context, uint64, workflows.Run) (*workflows.Run, error)
}

func (s registryStub) Get(ctx context.Context, sid, rid uuid.UUID) (*workflows.Run, error) {
	if s.get != nil {
		return s.get(ctx, sid, rid)
	}
	return nil, &workflows.NotFoundError{Kind: "run", SessionID: sid, RunID: rid}
}
func (s registryStub) Create(ctx context.Context, run workflows.Run) (*workflows.Run, error) {
	if s.create != nil {
		return s.create(ctx, run)
	}
	return &run, nil
}
func (s registryStub) List(ctx context.Context, sid uuid.UUID, request workflows.ListRunsRequest) (workflows.RunPage, error) {
	if s.list != nil {
		return s.list(ctx, sid, request)
	}
	return workflows.RunPage{}, nil
}
func (s registryStub) CompareAndSwap(ctx context.Context, revision uint64, run workflows.Run) (*workflows.Run, error) {
	if s.cas != nil {
		return s.cas(ctx, revision, run)
	}
	return &run, nil
}

type inputStoreStub struct{}

func (inputStoreStub) Put(context.Context, uuid.UUID, []byte) (workflows.InputReference, error) {
	return workflows.InputReference{}, nil
}

type controlStub struct {
	cancelCalls, resumeCalls int
	cancelErr, resumeErr     error
	startFunc                func(context.Context, uuid.UUID) (<-chan struct{}, <-chan error, error)
}

func (s *controlStub) Cancel(context.Context, uuid.UUID, string) error {
	s.cancelCalls++
	return s.cancelErr
}
func (s *controlStub) Resume(context.Context, uuid.UUID, json.RawMessage) error {
	s.resumeCalls++
	return s.resumeErr
}

func (s *controlStub) Start(ctx context.Context, runID uuid.UUID) (<-chan struct{}, <-chan error, error) {
	if s.startFunc != nil {
		return s.startFunc(ctx, runID)
	}
	seeded := make(chan struct{})
	close(seeded)
	return seeded, make(chan error, 1), nil
}

func (s *controlStub) History(context.Context, uuid.UUID, uint64, uuid.UUID, int) (workflows.ActivityHistoryPage, error) {
	return workflows.ActivityHistoryPage{}, nil
}
