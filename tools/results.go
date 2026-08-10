package tools

import (
	"time"

	"github.com/looprig/workflows"
)

type artifactResult struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}
type runResult struct {
	RunID              string           `json:"run_id"`
	DefinitionName     string           `json:"definition_name"`
	DefinitionVersion  string           `json:"definition_version"`
	RunStatus          string           `json:"run_status"`
	StatusSummary      string           `json:"status_summary,omitempty"`
	CheckpointRevision uint64           `json:"checkpoint_revision"`
	ActivityCursor     uint64           `json:"activity_cursor"`
	ParentRunID        string           `json:"parent_run_id,omitempty"`
	Artifacts          []artifactResult `json:"artifacts,omitempty"`
	CreatedAt          time.Time        `json:"created_at"`
	UpdatedAt          time.Time        `json:"updated_at"`
}
type cancelResult struct {
	RunID      string `json:"run_id"`
	RunStatus  string `json:"run_status"`
	Idempotent bool   `json:"idempotent"`
}

func newRunResult(run workflows.Run) runResult {
	artifacts := make([]artifactResult, len(run.Artifacts))
	for i, artifact := range run.Artifacts {
		artifacts[i] = artifactResult{artifact.ID, artifact.Kind, artifact.Digest, artifact.Size}
	}
	parent := ""
	if !run.ParentRunID.IsZero() {
		parent = run.ParentRunID.String()
	}
	return runResult{RunID: run.ID.String(), DefinitionName: run.DefinitionName, DefinitionVersion: run.DefinitionVersion,
		RunStatus: string(run.Status), StatusSummary: run.StatusSummary, CheckpointRevision: run.CheckpointRevision,
		ActivityCursor: run.ActivityCursor, ParentRunID: parent, Artifacts: artifacts, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt}
}
