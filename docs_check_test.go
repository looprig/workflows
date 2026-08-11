package workflows

import (
	"os"
	"strings"
	"testing"
)

func TestStandaloneCheckTargetRemainsSelfContained(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatal(err)
	}
	want := "standalone-check: fmt-check vet test race integration tools-ready staticcheck gosec build dependency-check dependency-policy-test repository-provenance-test harness-gates-test notices-check"
	if !strings.Contains(string(data), want+"\n") {
		t.Fatalf("Makefile must contain the exact self-contained target:\n%s", want)
	}
	if strings.Contains(want, "harness-integration") {
		t.Fatal("standalone-check must not include tagged Harness integration suites before a compatible published Harness is pinned")
	}
}
