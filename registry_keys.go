package workflows

import (
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
)

func runPrefix(sessionID uuid.UUID) (string, error) {
	if sessionID.IsZero() {
		return "", errors.New("session ID is required")
	}
	prefix := "sessions/" + sessionID.String() + "/workflows/runs/"
	if err := storage.ValidateName(prefix[:len(prefix)-1]); err != nil {
		return "", fmt.Errorf("invalid run prefix: %w", err)
	}
	return prefix, nil
}

func runKey(sessionID, runID uuid.UUID) (string, error) {
	prefix, err := runPrefix(sessionID)
	if err != nil {
		return "", err
	}
	if runID.IsZero() {
		return "", errors.New("workflow run ID is required")
	}
	key := prefix + runID.String()
	if err := storage.ValidateName(key); err != nil {
		return "", fmt.Errorf("invalid run key: %w", err)
	}
	return key, nil
}

func mustRunKey(sessionID, runID uuid.UUID) string {
	key, err := runKey(sessionID, runID)
	if err != nil {
		panic(err)
	}
	return key
}

func inputPrefix(sessionID uuid.UUID) (string, error) {
	if sessionID.IsZero() {
		return "", errors.New("session ID is required")
	}
	prefix := "sessions/" + sessionID.String() + "/workflows/inputs/sha256/"
	if err := storage.ValidateName(prefix[:len(prefix)-1]); err != nil {
		return "", fmt.Errorf("invalid input prefix: %w", err)
	}
	return prefix, nil
}

func inputKey(sessionID uuid.UUID, digest string) (string, error) {
	prefix, err := inputPrefix(sessionID)
	if err != nil {
		return "", err
	}
	if !validDigest(digest) {
		return "", errors.New("input digest must be lowercase SHA-256")
	}
	key := prefix + digest
	if err := storage.ValidateName(key); err != nil {
		return "", fmt.Errorf("invalid input key: %w", err)
	}
	return key, nil
}

func validateInputReference(sessionID uuid.UUID, ref InputReference) error {
	want, err := inputKey(sessionID, ref.Digest)
	if err != nil {
		return err
	}
	if ref.Key != want {
		return errors.New("input reference is outside the session-private digest namespace")
	}
	if ref.Size <= 0 || ref.Size > MaxInputBytes {
		return fmt.Errorf("input reference size must be within 1..%d", MaxInputBytes)
	}
	return nil
}
