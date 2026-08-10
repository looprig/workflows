package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/looprig/flow/pkg/flow"
)

const (
	MaxStatusSummaryBytes = 1024
	maxDescriptionBytes   = 1024
)

var definitionNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{2,63}$`)

type VertexMetadata struct {
	id    flow.VertexID
	label string
}

func NewVertexMetadata(label string) VertexMetadata { return VertexMetadata{label: label} }

// NewVertexMetadataForID binds a safe display label to Flow's stable vertex
// identity. Definitions that provide IDs let activity projection remain
// correct when the graph completes vertices out of declaration order.
func NewVertexMetadataForID(id flow.VertexID, label string) VertexMetadata {
	return VertexMetadata{id: id, label: label}
}

func (m VertexMetadata) ID() flow.VertexID { return m.id }
func (m VertexMetadata) Label() string     { return m.label }
func (m VertexMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID    string `json:"id,omitempty"`
		Label string `json:"label"`
	}{ID: vertexMetadataID(m.id), Label: m.label})
}

func vertexMetadataID(id flow.VertexID) string {
	if id == (flow.VertexID{}) {
		return ""
	}
	return id.String()
}

type Metadata struct {
	name         string
	version      string
	description  string
	inputSchema  json.RawMessage
	resumeSchema json.RawMessage
	vertices     []VertexMetadata
}

func NewMetadata(name, version, description string, inputSchema, resumeSchema json.RawMessage, vertices []VertexMetadata) (Metadata, error) {
	if !definitionNamePattern.MatchString(name) {
		return Metadata{}, &InvalidSchemaError{Field: "name", Err: fmt.Errorf("must match %s", definitionNamePattern)}
	}
	if version == "" || len(version) > 64 || strings.TrimSpace(version) != version || strings.ContainsAny(version, "\x00\r\n\t") {
		return Metadata{}, &InvalidSchemaError{Field: "version", Err: errors.New("must be a non-empty immutable string of at most 64 bytes")}
	}
	if len(description) > maxDescriptionBytes || !utf8.ValidString(description) {
		return Metadata{}, &InvalidSchemaError{Field: "description", Err: errors.New("must be valid UTF-8 within the size limit")}
	}
	seenVertexIDs := make(map[flow.VertexID]struct{}, len(vertices))
	for _, vertex := range vertices {
		if vertex.label == "" || len(vertex.label) > 64 || !utf8.ValidString(vertex.label) || strings.ContainsAny(vertex.label, "\x00\r\n") {
			return Metadata{}, &InvalidSchemaError{Field: "vertex label", Err: errors.New("must be safe non-empty text within 64 bytes")}
		}
		if vertex.id != (flow.VertexID{}) {
			if _, exists := seenVertexIDs[vertex.id]; exists {
				return Metadata{}, &InvalidSchemaError{Field: "vertex ID", Err: errors.New("must be unique")}
			}
			seenVertexIDs[vertex.id] = struct{}{}
		}
	}
	return Metadata{
		name: name, version: version, description: description,
		inputSchema: slices.Clone(inputSchema), resumeSchema: slices.Clone(resumeSchema),
		vertices: slices.Clone(vertices),
	}, nil
}

func (m Metadata) Name() string                  { return m.name }
func (m Metadata) Version() string               { return m.version }
func (m Metadata) Description() string           { return m.description }
func (m Metadata) InputSchema() json.RawMessage  { return slices.Clone(m.inputSchema) }
func (m Metadata) ResumeSchema() json.RawMessage { return slices.Clone(m.resumeSchema) }
func (m Metadata) Vertices() []VertexMetadata    { return slices.Clone(m.vertices) }
func (m Metadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name         string           `json:"name"`
		Version      string           `json:"version"`
		Description  string           `json:"description"`
		InputSchema  json.RawMessage  `json:"input_schema"`
		ResumeSchema json.RawMessage  `json:"resume_schema,omitempty"`
		Vertices     []VertexMetadata `json:"vertices"`
	}{
		Name: m.name, Version: m.version, Description: m.description,
		InputSchema: slices.Clone(m.inputSchema), ResumeSchema: slices.Clone(m.resumeSchema),
		Vertices: slices.Clone(m.vertices),
	})
}
func (m Metadata) clone() Metadata {
	return Metadata{
		name: m.name, version: m.version, description: m.description,
		inputSchema: slices.Clone(m.inputSchema), resumeSchema: slices.Clone(m.resumeSchema),
		vertices: slices.Clone(m.vertices),
	}
}

type ValidatedInput struct {
	owner *registration
	value any
}

type ValidatedResume struct {
	owner *registration
	value any
}

type Result struct {
	Run        flow.GraphRunState
	State      json.RawMessage
	Interrupts []flow.Interruption
	Halt       *flow.Halt
	Summary    string
}

type Definition interface {
	Metadata() Metadata
	ValidateInput(json.RawMessage) (ValidatedInput, error)
	ValidateResume(json.RawMessage) (ValidatedResume, error)
	Start(context.Context, ValidatedInput, ...flow.RunOption) (*Result, error)
	Resume(context.Context, flow.GraphRunID, ValidatedResume, ...flow.RunOption) (*Result, error)
	Get(context.Context, flow.GraphRunID) (*Result, error)
	History(context.Context, flow.GraphRunID) ([]flow.GraphRunState, error)
	Cancel(context.Context, flow.GraphRunID, string, ...flow.RunOption) error
	registeredCopy() (Definition, error)
}
