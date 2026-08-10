package workflows

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestCompileStrictSchemaRejectsNestedObjectsAllowingAdditionalProperties(t *testing.T) {
	tests := []struct {
		name   string
		schema json.RawMessage
	}{
		{
			name: "property",
			schema: json.RawMessage(`{
				"type":"object",
				"properties":{"nested":{"type":"object","properties":{}}},
				"additionalProperties":false
			}`),
		},
		{
			name: "array items",
			schema: json.RawMessage(`{
				"type":"object",
				"properties":{"entries":{"type":"array","items":{"type":"object","properties":{}}}},
				"additionalProperties":false
			}`),
		},
		{
			name: "definition",
			schema: json.RawMessage(`{
				"type":"object",
				"properties":{"nested":{"$ref":"#/$defs/nested"}},
				"$defs":{"nested":{"type":"object","properties":{}}},
				"additionalProperties":false
			}`),
		},
		{
			name: "implicit object",
			schema: json.RawMessage(`{
				"type":"object",
				"properties":{"nested":{"properties":{}}},
				"additionalProperties":false
			}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileStrictSchema("input", tc.schema)
			if !errors.Is(err, ErrInvalidSchema) {
				t.Fatalf("compileStrictSchema error = %v, want ErrInvalidSchema", err)
			}
		})
	}
}

func TestCompileStrictSchemaAcceptsRecursivelyClosedObjects(t *testing.T) {
	schema := json.RawMessage(`{
		"type":"object",
		"properties":{
			"nested":{"$ref":"#/$defs/nested"},
			"entries":{"type":"array","items":{"type":"object","properties":{},"additionalProperties":false}}
		},
		"$defs":{"nested":{"type":"object","properties":{},"additionalProperties":false}},
		"additionalProperties":false
	}`)

	if _, err := compileStrictSchema("input", schema); err != nil {
		t.Fatalf("compileStrictSchema: %v", err)
	}
}

func TestCompileStrictSchemaDoesNotTreatKeywordDataAsSubschemas(t *testing.T) {
	schema := json.RawMessage(`{
		"type":"object",
		"properties":{"value":{"const":{"type":"object"}}},
		"additionalProperties":false
	}`)

	if _, err := compileStrictSchema("input", schema); err != nil {
		t.Fatalf("compileStrictSchema: %v", err)
	}
}
