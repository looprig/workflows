package workflows

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/storage"
)

const (
	MaxRunRecordBytes     = 64 << 10
	MaxArtifactReferences = 128
	MaxRunPageSize        = 100
	DefaultRunPageSize    = 50
	MaxInputBytes         = MaxDocumentBytes
)

var (
	ErrNotFound      = errors.New("workflow record not found")
	ErrConflict      = errors.New("workflow record conflict")
	ErrCorruptRecord = errors.New("corrupt workflow record")
)

// NotFoundError reports a session-scoped run or private input that is absent.
type NotFoundError struct {
	Kind      string
	SessionID uuid.UUID
	RunID     uuid.UUID
	Digest    string
}

func (e *NotFoundError) Error() string {
	if e.Kind == "input" {
		return fmt.Sprintf("%s: input %s in session %s", ErrNotFound, e.Digest, e.SessionID)
	}
	return fmt.Sprintf("%s: run %s in session %s", ErrNotFound, e.RunID, e.SessionID)
}

func (e *NotFoundError) Unwrap() error { return ErrNotFound }

// ConflictError reports create/CAS conflicts and rejected lifecycle mutations.
type ConflictError struct {
	SessionID uuid.UUID
	RunID     uuid.UUID
	Expected  uint64
	Actual    uint64
	Reason    string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s: run %s in session %s at revision %d (actual %d): %s", ErrConflict, e.RunID, e.SessionID, e.Expected, e.Actual, e.Reason)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// CorruptRecordError reports malformed or inconsistent durable metadata.
type CorruptRecordError struct {
	Key string
	Err error
}

func (e *CorruptRecordError) Error() string {
	return fmt.Sprintf("%s at %q: %v", ErrCorruptRecord, e.Key, e.Err)
}

func (e *CorruptRecordError) Unwrap() error { return ErrCorruptRecord }

// InputReference identifies schema-validated input bytes held outside KV.
type InputReference struct {
	Digest string `json:"digest"`
	Key    string `json:"key"`
	Size   int64  `json:"size"`
}

// ArtifactReference identifies immutable output or intermediate artifact bytes.
type ArtifactReference struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

// Run is the bounded durable metadata for one session-owned workflow execution.
// Revision is the storage.KV CAS token and is not encoded into the value.
type Run struct {
	SessionID          uuid.UUID           `json:"session_id"`
	ToolExecutionID    uuid.UUID           `json:"tool_execution_id"`
	DefinitionName     string              `json:"definition_name"`
	DefinitionVersion  string              `json:"definition_version"`
	ID                 uuid.UUID           `json:"id"`
	GraphRunID         flow.GraphRunID     `json:"graph_run_id"`
	ParentRunID        uuid.UUID           `json:"parent_run_id,omitzero"`
	Input              InputReference      `json:"input"`
	Status             RunStatus           `json:"status"`
	StatusSummary      string              `json:"status_summary,omitempty"`
	CancelRequested    bool                `json:"cancel_requested,omitempty"`
	CheckpointRevision uint64              `json:"checkpoint_revision"`
	ActivityCursor     uint64              `json:"activity_cursor"`
	LedgerLocator      string              `json:"ledger_locator"`
	Artifacts          []ArtifactReference `json:"artifacts,omitempty"`
	CreatedAt          time.Time           `json:"created_at"`
	UpdatedAt          time.Time           `json:"updated_at"`
	Revision           uint64              `json:"-"`
}

func cloneRun(run Run) Run {
	run.Artifacts = append([]ArtifactReference(nil), run.Artifacts...)
	return run
}

func validateRun(run Run) error {
	if run.SessionID.IsZero() {
		return errors.New("session ID is required")
	}
	if run.ToolExecutionID.IsZero() {
		return errors.New("tool execution ID is required")
	}
	if run.ID.IsZero() {
		return errors.New("workflow run ID is required")
	}
	if uuid.UUID(run.GraphRunID).IsZero() {
		return errors.New("flow graph run ID is required")
	}
	if err := validateComponent("definition name", run.DefinitionName); err != nil {
		return err
	}
	if err := validateComponent("definition version", run.DefinitionVersion); err != nil {
		return err
	}
	if !run.Status.valid() {
		return fmt.Errorf("invalid run status %q", run.Status)
	}
	if len(run.StatusSummary) > MaxStatusSummaryBytes {
		return fmt.Errorf("status summary exceeds %d bytes", MaxStatusSummaryBytes)
	}
	if err := validateInputReference(run.SessionID, run.Input); err != nil {
		return err
	}
	if err := storage.ValidateName(run.LedgerLocator); err != nil {
		return fmt.Errorf("invalid ledger locator: %w", err)
	}
	if err := validateTimestamp("created_at", run.CreatedAt); err != nil {
		return err
	}
	if err := validateTimestamp("updated_at", run.UpdatedAt); err != nil {
		return err
	}
	if run.UpdatedAt.Before(run.CreatedAt) {
		return errors.New("updated_at precedes created_at")
	}
	if len(run.Artifacts) > MaxArtifactReferences {
		return fmt.Errorf("artifact references exceed %d", MaxArtifactReferences)
	}
	seen := make(map[string]struct{}, len(run.Artifacts))
	for i, artifact := range run.Artifacts {
		if err := validateComponent("artifact ID", artifact.ID); err != nil {
			return fmt.Errorf("artifact %d: %w", i, err)
		}
		if err := validateComponent("artifact kind", artifact.Kind); err != nil {
			return fmt.Errorf("artifact %d: %w", i, err)
		}
		if !validDigest(artifact.Digest) || artifact.Size < 0 {
			return fmt.Errorf("artifact %d has invalid digest or size", i)
		}
		if _, duplicate := seen[artifact.ID]; duplicate {
			return fmt.Errorf("duplicate artifact ID %q", artifact.ID)
		}
		seen[artifact.ID] = struct{}{}
	}
	return nil
}

func validateComponent(field, component string) error {
	if strings.Contains(component, "/") {
		return fmt.Errorf("%s must be one canonical path component", field)
	}
	if err := storage.ValidateName(component); err != nil {
		return fmt.Errorf("invalid %s: %w", field, err)
	}
	return nil
}

func validateTimestamp(field string, value time.Time) error {
	if value.IsZero() || value.Location() != time.UTC || value.Year() < 2000 || value.Year() > 9999 {
		return fmt.Errorf("%s must be a bounded UTC timestamp", field)
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func immutableIdentityEqual(current, next Run) bool {
	return current.SessionID == next.SessionID &&
		current.ToolExecutionID == next.ToolExecutionID &&
		current.DefinitionName == next.DefinitionName &&
		current.DefinitionVersion == next.DefinitionVersion &&
		current.ID == next.ID &&
		current.GraphRunID == next.GraphRunID &&
		current.ParentRunID == next.ParentRunID &&
		current.Input == next.Input &&
		current.LedgerLocator == next.LedgerLocator &&
		current.CreatedAt.Equal(next.CreatedAt)
}
