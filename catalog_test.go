package workflows

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCatalogRejectsInvalidNamesAndDuplicateVersions(t *testing.T) {
	for _, name := range []string{"ab", "Upper_case", "starts-dash", strings.Repeat("a", 65)} {
		if _, err := NewMetadata(name, "v1", "safe", strictEmptyObjectSchema(), strictEmptyObjectSchema(), nil); !errors.Is(err, ErrInvalidSchema) {
			t.Errorf("NewMetadata(%q) error = %v, want ErrInvalidSchema", name, err)
		}
	}

	catalog := NewCatalog()
	first, _ := counterDefinition(t, "counter_flow", "v1")
	duplicate, _ := counterDefinition(t, "counter_flow", "v1")
	secondVersion, _ := counterDefinition(t, "counter_flow", "v2")
	if err := catalog.Register(first); err != nil {
		t.Fatalf("Register first: %v", err)
	}
	if err := catalog.Register(duplicate); !errors.Is(err, ErrDuplicateDefinition) {
		t.Fatalf("Register duplicate error = %v, want ErrDuplicateDefinition", err)
	}
	if err := catalog.Register(secondVersion); err != nil {
		t.Fatalf("Register second version: %v", err)
	}
}

func TestCatalogCompilesStrictSchemasAndRejectsRemoteRefs(t *testing.T) {
	tests := []struct {
		name   string
		schema json.RawMessage
	}{
		{name: "not object", schema: json.RawMessage(`{"type":"array"}`)},
		{name: "allows unknown", schema: json.RawMessage(`{"type":"object"}`)},
		{name: "remote ref", schema: json.RawMessage(`{"type":"object","properties":{"x":{"$ref":"https://example.invalid/schema.json"}},"additionalProperties":false}`)},
		{name: "too deep", schema: json.RawMessage(strings.Repeat(`{"allOf":[`, MaxSchemaDepth+1) + strings.Repeat(`]}`, MaxSchemaDepth+1))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, _ := counterDefinition(t, "schema_test", "v1")
			def.metadata.inputSchema = tt.schema
			if err := NewCatalog().Register(def); !errors.Is(err, ErrInvalidSchema) {
				t.Fatalf("Register error = %v, want ErrInvalidSchema", err)
			}
		})
	}
}

func TestCatalogListReturnsSafeSortedDiscoveryMetadata(t *testing.T) {
	catalog := NewCatalog()
	for _, key := range [][2]string{{"zeta_flow", "v2"}, {"alpha_flow", "v2"}, {"alpha_flow", "v1"}} {
		def, _ := counterDefinition(t, key[0], key[1])
		if err := catalog.Register(def); err != nil {
			t.Fatalf("Register(%v): %v", key, err)
		}
	}

	list := catalog.List()
	want := []string{"alpha_flow@v1", "alpha_flow@v2", "zeta_flow@v2"}
	for i, item := range list {
		if got := item.Name() + "@" + item.Version(); got != want[i] {
			t.Fatalf("List[%d] = %q, want %q", i, got, want[i])
		}
		if item.Description() == "" || len(item.InputSchema()) == 0 || len(item.ResumeSchema()) == 0 || len(item.Vertices()) != 1 {
			t.Fatalf("List[%d] omitted safe discovery metadata: %#v", i, item)
		}
	}
	list[0] = Metadata{}
	if catalog.List()[0].Name() != "alpha_flow" {
		t.Fatal("List returned mutable catalog storage")
	}
	discoveryJSON, err := json.Marshal(catalog.List()[0])
	if err != nil {
		t.Fatalf("Marshal discovery metadata: %v", err)
	}
	for _, field := range []string{`"name"`, `"version"`, `"description"`, `"input_schema"`, `"resume_schema"`, `"vertices"`, `"label"`} {
		if !strings.Contains(string(discoveryJSON), field) {
			t.Fatalf("discovery JSON %s is missing %s", discoveryJSON, field)
		}
	}

	if _, err := catalog.Resolve("missing_flow", "v1"); !errors.Is(err, ErrUnknownDefinition) {
		t.Fatalf("Resolve missing error = %v, want ErrUnknownDefinition", err)
	}
}

func TestCatalogCachesSchemasAtRegistration(t *testing.T) {
	definition, _ := counterDefinition(t, "cached_schema", "v1")
	catalog := NewCatalog()
	if err := catalog.Register(definition); err != nil {
		t.Fatalf("Register: %v", err)
	}
	definition.metadata.inputSchema[0] = '['
	resolved, err := catalog.Resolve("cached_schema", "v1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, err := resolved.ValidateInput(json.RawMessage(`{"count":1}`)); err != nil {
		t.Fatalf("cached schema changed with caller metadata: %v", err)
	}
}

func TestSchemaValidationBoundsInputAndRejectsTrailingJSON(t *testing.T) {
	def := registeredCounterDefinition(t)
	for _, input := range []json.RawMessage{
		json.RawMessage(`{"count":1} {"count":2}`),
		json.RawMessage(strings.Repeat(" ", MaxDocumentBytes) + `{"count":1}`),
		json.RawMessage(strings.Repeat(`{"x":`, MaxDocumentDepth+1) + `0` + strings.Repeat(`}`, MaxDocumentDepth+1)),
	} {
		if _, err := def.ValidateInput(input); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("ValidateInput error = %v, want ErrInvalidInput", err)
		}
	}
}

func TestSchemaValidationAllowsAnOrdinaryIDProperty(t *testing.T) {
	def, _ := counterDefinition(t, "ordinary_id", "v1")
	def.metadata.inputSchema = json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"count":{"type":"integer"}},"required":["count"],"additionalProperties":false}`)
	catalog := NewCatalog()
	if err := catalog.Register(def); err != nil {
		t.Fatalf("Register schema with ordinary id property: %v", err)
	}
	resolved, err := catalog.Resolve("ordinary_id", "v1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, err := resolved.ValidateInput(json.RawMessage(`{"count":1}`)); err != nil {
		t.Fatalf("ValidateInput ordinary id property: %v", err)
	}
}

func TestSchemaValidationRejectsMalformedContainersWithoutPanic(t *testing.T) {
	def := registeredCounterDefinition(t)
	for _, raw := range []json.RawMessage{json.RawMessage(`}`), json.RawMessage(`{"count":[1}`)} {
		if _, err := def.ValidateInput(raw); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("ValidateInput(%s) error = %v, want ErrInvalidInput", raw, err)
		}
	}
}

func strictEmptyObjectSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)
}
