package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

type Config struct {
	SessionID  uuid.UUID
	Catalog    *workflows.Catalog
	Registry   runRegistry
	Inputs     inputStore
	Supervisor supervisorControl
	Now        func() time.Time
	NewID      func() (uuid.UUID, error)
}

type boundRuntime struct {
	sessionID  uuid.UUID
	catalog    *workflows.Catalog
	registry   runRegistry
	inputs     inputStore
	supervisor supervisorControl
	now        func() time.Time
	newID      func() (uuid.UUID, error)
}

func NewBundle(config Config) ([]tool.InvokableTool, error) {
	if config.SessionID.IsZero() || config.Catalog == nil || config.Registry == nil || config.Inputs == nil || config.Supervisor == nil {
		return nil, errors.New("workflow tools: session, catalog, registry, input store, and supervisor are required")
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	newID := config.NewID
	if newID == nil {
		newID = uuid.New
	}
	runtime := &boundRuntime{sessionID: config.SessionID, catalog: config.Catalog, registry: config.Registry, inputs: config.Inputs, supervisor: config.Supervisor, now: now, newID: newID}
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
