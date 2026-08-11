package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDocumentationExampleArtifacts(t *testing.T) {
	root := filepath.Clean("..")
	for _, path := range []string{
		"examples/docs/stage18_workflows/main.go",
		"examples/docs/stage18_workflows/main_test.go",
		"testdata/docs/examples.json",
		".github/workflows/docs-examples.yml",
	} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Errorf("required documentation artifact %s: %v", path, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "testdata/docs/examples.json"))
	if err != nil {
		return
	}
	var manifest struct {
		SchemaVersion int    `json:"schemaVersion"`
		Repository    string `json:"repository"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Repository != "workflows" {
		t.Fatalf("manifest identity = (%d, %q), want (1, workflows)", manifest.SchemaVersion, manifest.Repository)
	}
}
