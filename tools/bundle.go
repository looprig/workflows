package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/workflows"
)

const maxToolDescriptionBytes = 256

type runRegistry interface {
	Create(context.Context, workflows.Run) (*workflows.Run, error)
	Get(context.Context, uuid.UUID, uuid.UUID) (*workflows.Run, error)
	CompareAndSwap(context.Context, uint64, workflows.Run) (*workflows.Run, error)
	List(context.Context, uuid.UUID, workflows.ListRunsRequest) (workflows.RunPage, error)
}

type inputStore interface {
	Put(context.Context, uuid.UUID, []byte) (workflows.InputReference, error)
}

type supervisorControl interface {
	Resume(context.Context, uuid.UUID, json.RawMessage) error
	Cancel(context.Context, uuid.UUID, string) error
}

type supervisorStarter interface {
	Start(context.Context, uuid.UUID) (<-chan struct{}, <-chan error, error)
}

type supervisorHistory interface {
	History(context.Context, uuid.UUID, uint64, uuid.UUID, int) (workflows.ActivityHistoryPage, error)
}

type Config struct {
	SessionID  uuid.UUID
	Catalog    *workflows.Catalog
	Registry   runRegistry
	Inputs     inputStore
	Supervisor supervisorControl
	Now        func() time.Time
	NewID      func() (uuid.UUID, error)
	// PrepareRun runs after the workflow and GraphRun IDs are minted but before
	// the durable registry record is created. It is the narrow composition seam
	// for binding product-owned immutable execution metadata without making the
	// neutral bridge aware of application artifacts.
	PrepareRun func(context.Context, workflows.Run) (workflows.Run, error)
}

// PrepareRunIntegrityError reports a callback attempt to change workflow
// metadata outside the artifact namespace descriptor allowlist.
type PrepareRunIntegrityError struct {
	Field string
}

func (e *PrepareRunIntegrityError) Error() string {
	if e == nil {
		return "workflow tools: PrepareRun changed protected run metadata"
	}
	return fmt.Sprintf("workflow tools: PrepareRun changed protected run field %q", e.Field)
}

func validatePreparedRun(before, after workflows.Run) error {
	protected := after
	protected.ArtifactSessionID = before.ArtifactSessionID
	protected.ArtifactRunID = before.ArtifactRunID
	protected.ArtifactInputKind = before.ArtifactInputKind
	protected.ArtifactInputSessionID = before.ArtifactInputSessionID
	protected.ArtifactInputRunID = before.ArtifactInputRunID
	if reflect.DeepEqual(before, protected) {
		return nil
	}
	for _, field := range []struct {
		name  string
		equal func() bool
	}{
		{name: "session_id", equal: func() bool { return before.SessionID == after.SessionID }},
		{name: "tool_execution_id", equal: func() bool { return before.ToolExecutionID == after.ToolExecutionID }},
		{name: "definition_name", equal: func() bool { return before.DefinitionName == after.DefinitionName }},
		{name: "definition_version", equal: func() bool { return before.DefinitionVersion == after.DefinitionVersion }},
		{name: "id", equal: func() bool { return before.ID == after.ID }},
		{name: "graph_run_id", equal: func() bool { return before.GraphRunID == after.GraphRunID }},
		{name: "parent_run_id", equal: func() bool { return before.ParentRunID == after.ParentRunID }},
		{name: "input", equal: func() bool { return reflect.DeepEqual(before.Input, after.Input) }},
		{name: "status", equal: func() bool { return before.Status == after.Status }},
		{name: "status_summary", equal: func() bool { return before.StatusSummary == after.StatusSummary }},
		{name: "cancel_requested", equal: func() bool { return before.CancelRequested == after.CancelRequested }},
		{name: "checkpoint_revision", equal: func() bool { return before.CheckpointRevision == after.CheckpointRevision }},
		{name: "activity_cursor", equal: func() bool { return before.ActivityCursor == after.ActivityCursor }},
		{name: "ledger_locator", equal: func() bool { return before.LedgerLocator == after.LedgerLocator }},
		{name: "artifacts", equal: func() bool { return reflect.DeepEqual(before.Artifacts, after.Artifacts) }},
		{name: "created_at", equal: func() bool { return reflect.DeepEqual(before.CreatedAt, after.CreatedAt) }},
		{name: "updated_at", equal: func() bool { return reflect.DeepEqual(before.UpdatedAt, after.UpdatedAt) }},
		{name: "revision", equal: func() bool { return before.Revision == after.Revision }},
	} {
		if !field.equal() {
			return &PrepareRunIntegrityError{Field: field.name}
		}
	}
	return &PrepareRunIntegrityError{Field: "run_metadata"}
}

type boundRuntime struct {
	sessionID  uuid.UUID
	catalog    *workflows.Catalog
	registry   runRegistry
	inputs     inputStore
	supervisor supervisorControl
	now        func() time.Time
	newID      func() (uuid.UUID, error)
	prepareRun func(context.Context, workflows.Run) (workflows.Run, error)
}

func NewBundle(config Config) ([]tool.InvokableTool, error) {
	if config.SessionID.IsZero() || config.Catalog == nil || config.Registry == nil || config.Inputs == nil || config.Supervisor == nil {
		return nil, errors.New("workflow tools: session, catalog, registry, input store, and supervisor are required")
	}
	if _, ok := config.Supervisor.(supervisorStarter); !ok {
		return nil, errors.New("workflow tools: supervisor must provide the session-owned start controller")
	}
	if _, ok := config.Supervisor.(supervisorHistory); !ok {
		return nil, errors.New("workflow tools: supervisor must provide the projected history controller")
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	newID := config.NewID
	if newID == nil {
		newID = uuid.New
	}
	runtime := &boundRuntime{sessionID: config.SessionID, catalog: config.Catalog, registry: config.Registry, inputs: config.Inputs, supervisor: config.Supervisor, now: now, newID: newID, prepareRun: config.PrepareRun}
	return newToolSet(runtime), nil
}

func newToolSet(runtime *boundRuntime) []tool.InvokableTool {
	return []tool.InvokableTool{
		&definitionListTool{runtime}, &runStartTool{runtime}, &runGetTool{runtime},
		&runListTool{runtime}, &runResumeTool{runtime}, &runCancelTool{boundRuntime: runtime, runtime: runtime}, &runHistoryTool{runtime},
	}
}

func toolInfos() []tool.ToolInfo {
	return []tool.ToolInfo{definitionListInfo, runStartInfo, runGetInfo, runListInfo, runResumeInfo, runCancelInfo, runHistoryInfo}
}

func decodeStrict(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("workflow tools: invalid arguments: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return fmt.Errorf("workflow tools: invalid arguments: %w", err)
	}
	return nil
}

func result(value any) (*tool.ToolResult, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("workflow tools: encode bounded result: %w", err)
	}
	return tool.TextResult(string(raw)), nil
}

func parseID(field, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id.IsZero() {
		return uuid.UUID{}, fmt.Errorf("workflow tools: %s must be a nonzero UUID", field)
	}
	return id, nil
}

func canonicalObject(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("workflow tools: input is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("workflow tools: invalid JSON object: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow tools: input must contain one JSON object")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("workflow tools: canonicalize input: %w", err)
	}
	return canonical, nil
}
