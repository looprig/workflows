package workflows

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	MaxSchemaBytes        = 64 << 10
	MaxSchemaDepth        = 32
	MaxSchemaProperties   = 1024
	MaxDocumentBytes      = 256 << 10
	MaxDocumentDepth      = 32
	MaxDocumentProperties = 4096
)

type compiledSchema struct{ schema *jsonschema.Schema }

func compileStrictSchema(field string, raw json.RawMessage) (*compiledSchema, error) {
	document, err := decodeBoundedJSON(raw, MaxSchemaBytes, MaxSchemaDepth, MaxSchemaProperties)
	if err != nil {
		return nil, &InvalidSchemaError{Field: field, Err: err}
	}
	root, ok := document.(map[string]any)
	if !ok || root["type"] != "object" {
		return nil, &InvalidSchemaError{Field: field, Err: errors.New("root schema type must be object")}
	}
	if additional, ok := root["additionalProperties"]; !ok || additional != false {
		return nil, &InvalidSchemaError{Field: field, Err: errors.New("root schema must set additionalProperties to false")}
	}
	if err := rejectExternalReferences(document); err != nil {
		return nil, &InvalidSchemaError{Field: field, Err: err}
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	location := "urn:looprig:workflows:" + field
	if err := compiler.AddResource(location, document); err != nil {
		return nil, &InvalidSchemaError{Field: field, Err: err}
	}
	compiled, err := compiler.Compile(location)
	if err != nil {
		return nil, &InvalidSchemaError{Field: field, Err: err}
	}
	return &compiledSchema{schema: compiled}, nil
}

func (s *compiledSchema) validate(raw json.RawMessage) error {
	document, err := decodeBoundedJSON(raw, MaxDocumentBytes, MaxDocumentDepth, MaxDocumentProperties)
	if err != nil {
		return err
	}
	if _, ok := document.(map[string]any); !ok {
		return errors.New("document must be a JSON object")
	}
	return s.schema.Validate(document)
}

func decodeBoundedJSON(raw []byte, maxBytes, maxDepth, maxProperties int) (any, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty JSON document")
	}
	if len(raw) > maxBytes {
		return nil, fmt.Errorf("JSON document exceeds %d bytes", maxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	depth, properties := 0, 0
	containers := make([]json.Delim, 0, maxDepth)
	expectingKey := make([]bool, 0, maxDepth)
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		switch value := token.(type) {
		case json.Delim:
			switch value {
			case '{', '[':
				depth++
				if depth > maxDepth {
					return nil, fmt.Errorf("JSON depth exceeds %d", maxDepth)
				}
				containers = append(containers, value)
				expectingKey = append(expectingKey, value == '{')
			case '}', ']':
				if len(containers) == 0 {
					return nil, errors.New("JSON container closes before it opens")
				}
				want := json.Delim('}')
				if containers[len(containers)-1] == '[' {
					want = ']'
				}
				if value != want {
					return nil, errors.New("JSON containers are mismatched")
				}
				depth--
				containers = containers[:len(containers)-1]
				expectingKey = expectingKey[:len(expectingKey)-1]
				markValueComplete(containers, expectingKey)
			}
		case string:
			if len(containers) > 0 && containers[len(containers)-1] == '{' && expectingKey[len(expectingKey)-1] {
				properties++
				if properties > maxProperties {
					return nil, fmt.Errorf("JSON properties exceed %d", maxProperties)
				}
				expectingKey[len(expectingKey)-1] = false
			} else {
				markValueComplete(containers, expectingKey)
			}
		default:
			markValueComplete(containers, expectingKey)
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if err := requireEOF(decoder); err != nil {
		return nil, err
	}
	return document, nil
}

func markValueComplete(containers []json.Delim, expectingKey []bool) {
	if len(containers) > 0 && containers[len(containers)-1] == '{' {
		expectingKey[len(expectingKey)-1] = true
	}
}

func rejectExternalReferences(value any) error {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			switch key {
			case "$id":
				return fmt.Errorf("%s is not permitted", key)
			case "$ref", "$dynamicRef", "$recursiveRef":
				reference, ok := child.(string)
				if !ok || !strings.HasPrefix(reference, "#") {
					return fmt.Errorf("remote reference %q is not permitted", reference)
				}
			}
			if err := rejectExternalReferences(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := rejectExternalReferences(child); err != nil {
				return err
			}
		}
	}
	return nil
}
