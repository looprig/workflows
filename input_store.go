package workflows

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
)

// InputStore persists validated JSON inputs in session-private immutable blobs.
type InputStore struct {
	blobs storage.Blobs
}

func NewInputStore(blobs storage.Blobs) (*InputStore, error) {
	if blobs == nil {
		return nil, errors.New("workflows: input store requires storage.Blobs")
	}
	return &InputStore{blobs: blobs}, nil
}

func (s *InputStore) Put(ctx context.Context, sessionID uuid.UUID, canonicalJSON []byte) (InputReference, error) {
	if s == nil || s.blobs == nil {
		return InputReference{}, errors.New("workflows: input store is not initialized")
	}
	if sessionID.IsZero() {
		return InputReference{}, errors.New("workflows: session ID is required")
	}
	if len(canonicalJSON) == 0 || len(canonicalJSON) > MaxInputBytes {
		return InputReference{}, fmt.Errorf("workflows: input size must be within 1..%d", MaxInputBytes)
	}
	document, err := decodeBoundedJSON(canonicalJSON, MaxInputBytes, MaxDocumentDepth, MaxDocumentProperties)
	if err != nil {
		return InputReference{}, fmt.Errorf("workflows: invalid canonical input JSON: %w", err)
	}
	if _, object := document.(map[string]any); !object {
		return InputReference{}, errors.New("workflows: canonical input JSON must be an object")
	}
	digestBytes := sha256.Sum256(canonicalJSON)
	digest := hex.EncodeToString(digestBytes[:])
	key, err := inputKey(sessionID, digest)
	if err != nil {
		return InputReference{}, err
	}
	owned := bytes.Clone(canonicalJSON)
	if err := s.blobs.Put(ctx, key, bytes.NewReader(owned)); err != nil {
		if errors.As(err, new(*storage.BlobConflictError)) {
			return InputReference{}, &ConflictError{SessionID: sessionID, Reason: "digest-addressed input blob contains different bytes"}
		}
		return InputReference{}, fmt.Errorf("workflows: store private input blob: %w", err)
	}
	return InputReference{Digest: digest, Key: key, Size: int64(len(owned))}, nil
}

func (s *InputStore) Get(ctx context.Context, sessionID uuid.UUID, ref InputReference) ([]byte, error) {
	if s == nil || s.blobs == nil {
		return nil, errors.New("workflows: input store is not initialized")
	}
	prefix, err := inputPrefix(sessionID)
	if err != nil {
		return nil, err
	}
	if !stringsHasPrefixExact(ref.Key, prefix) {
		return nil, &NotFoundError{Kind: "input", SessionID: sessionID, Digest: ref.Digest}
	}
	wantKey, err := inputKey(sessionID, ref.Digest)
	if err != nil || ref.Key != wantKey || ref.Size <= 0 || ref.Size > MaxInputBytes {
		if err == nil {
			err = errors.New("input reference key or size is inconsistent")
		}
		return nil, &CorruptRecordError{Key: ref.Key, Err: err}
	}
	reader, err := s.blobs.Get(ctx, ref.Key)
	if err != nil {
		if errors.As(err, new(*storage.BlobNotFoundError)) {
			return nil, &NotFoundError{Kind: "input", SessionID: sessionID, Digest: ref.Digest}
		}
		return nil, fmt.Errorf("workflows: get private input blob: %w", err)
	}
	value, readErr := io.ReadAll(io.LimitReader(reader, MaxInputBytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, fmt.Errorf("workflows: read private input blob: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("workflows: close private input blob: %w", closeErr)
	}
	if len(value) > MaxInputBytes || int64(len(value)) != ref.Size {
		return nil, &CorruptRecordError{Key: ref.Key, Err: errors.New("input blob size does not match its reference")}
	}
	digestBytes := sha256.Sum256(value)
	if hex.EncodeToString(digestBytes[:]) != ref.Digest {
		return nil, &CorruptRecordError{Key: ref.Key, Err: errors.New("input blob digest does not match its reference")}
	}
	return bytes.Clone(value), nil
}

func stringsHasPrefixExact(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}
