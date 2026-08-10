package tools

import (
	"context"
	"encoding/json"

	"github.com/looprig/harness/pkg/tool"
)

var definitionListInfo = tool.ToolInfo{Name: "workflow_definition_list", Desc: "List available workflow definitions, versions, descriptions, and strict input schemas.", Schema: json.RawMessage(`{"type":"object","additionalProperties":false}`)}

const maxDefinitionResults = 100

type definitionListTool struct{ *boundRuntime }

func (t *definitionListTool) Info(context.Context) (*tool.ToolInfo, error) {
	clone := definitionListInfo.Clone()
	return &clone, nil
}
func (t *definitionListTool) InvokableRun(_ context.Context, raw string) (*tool.ToolResult, error) {
	var args struct{}
	if err := decodeStrict(raw, &args); err != nil {
		return nil, err
	}
	metadata := t.catalog.List()
	truncated := len(metadata) > maxDefinitionResults
	if truncated {
		metadata = metadata[:maxDefinitionResults]
	}
	type item struct {
		Name            string          `json:"name"`
		Version         string          `json:"version"`
		Description     string          `json:"description"`
		InputSchema     json.RawMessage `json:"input_schema"`
		ResumeSupported bool            `json:"resume_supported"`
	}
	items := make([]item, len(metadata))
	for i, m := range metadata {
		items[i] = item{m.Name(), m.Version(), m.Description(), m.InputSchema(), len(m.ResumeSchema()) > 0}
	}
	return result(struct {
		Definitions []item `json:"definitions"`
		Truncated   bool   `json:"truncated"`
	}{items, truncated})
}
