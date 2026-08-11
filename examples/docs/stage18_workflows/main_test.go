package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestStage18Output(t *testing.T) {
	command := exec.Command("go", "run", ".")
	command.Env = append(os.Environ(), "GOWORK=off", "GOCACHE="+t.TempDir())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go run .: %v\n%s", err, output)
	}
	want := strings.Join([]string{
		"started: Interrupted",
		"interrupt: awaiting increment",
		"recovered: Interrupted",
		"resumed: Completed count=3",
		"cancelled: Cancelled",
		"history: append-only",
		"",
	}, "\n")
	if string(output) != want {
		t.Fatalf("output:\n%s\nwant:\n%s", output, want)
	}
}
