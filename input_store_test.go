package workflows

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestInputStorePutGetUsesPrivateDigestKeyAndCopies(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	store, err := NewInputStore(backend.Blobs)
	if err != nil {
		t.Fatalf("NewInputStore: %v", err)
	}
	sessionID := testUUID(51)
	input := []byte(`{"document_id":"artifact-123","mode":"extract"}`)
	want := bytes.Clone(input)

	ref, err := store.Put(ctx, sessionID, input)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	input[0] = '['
	if len(ref.Digest) != 64 || ref.Size != int64(len(want)) {
		t.Fatalf("Put reference = %#v", ref)
	}
	wantPrefix := "sessions/" + sessionID.String() + "/workflows/inputs/sha256/"
	if !strings.HasPrefix(ref.Key, wantPrefix) || !strings.HasSuffix(ref.Key, ref.Digest) {
		t.Fatalf("private input key = %q, want prefix %q and digest suffix", ref.Key, wantPrefix)
	}
	if err := storage.ValidateName(ref.Key); err != nil {
		t.Fatalf("input key is not storage canonical: %v", err)
	}

	got, err := store.Get(ctx, sessionID, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get = %q, want %q", got, want)
	}
	got[0] = '['
	again, err := store.Get(ctx, sessionID, ref)
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	if !bytes.Equal(again, want) {
		t.Fatalf("Get did not return defensive bytes: %q", again)
	}

	keys, err := backend.Blobs.List(ctx, wantPrefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 || keys[0] != ref.Key {
		t.Fatalf("blob keys = %v, want [%s]", keys, ref.Key)
	}
}

func TestInputStoreIsSessionScopedAndDigestIdempotent(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	store, _ := NewInputStore(backend.Blobs)
	input := []byte(`{"artifact_id":"private-1"}`)
	owner := testUUID(52)
	other := testUUID(53)
	first, err := store.Put(ctx, owner, input)
	if err != nil {
		t.Fatalf("Put first: %v", err)
	}
	second, err := store.Put(ctx, owner, bytes.Clone(input))
	if err != nil {
		t.Fatalf("Put identical: %v", err)
	}
	if first != second {
		t.Fatalf("idempotent Put references differ: %#v != %#v", first, second)
	}
	if _, err := store.Get(ctx, other, first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-session Get error = %v, want not found", err)
	}
}

func TestInputStoreVerifiesReferenceSizeAndDigest(t *testing.T) {
	ctx := context.Background()
	blobs := &mutableBlobs{values: make(map[string][]byte)}
	store, err := NewInputStore(blobs)
	if err != nil {
		t.Fatalf("NewInputStore: %v", err)
	}
	sessionID := testUUID(54)
	ref, err := store.Put(ctx, sessionID, []byte(`{"safe":true}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	wrongSize := ref
	wrongSize.Size++
	if _, err := store.Get(ctx, sessionID, wrongSize); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Get wrong size error = %v, want corrupt record", err)
	}
	blobs.values[ref.Key] = []byte(`{"safe":false}`)
	if _, err := store.Get(ctx, sessionID, ref); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Get wrong digest error = %v, want corrupt record", err)
	}
}

func TestInputStoreRejectsInvalidAndOversizedInputs(t *testing.T) {
	backend := memstore.New()
	store, _ := NewInputStore(backend.Blobs)
	ctx := context.Background()
	if _, err := store.Put(ctx, uuid.UUID{}, []byte(`{}`)); err == nil {
		t.Fatal("Put accepted zero session ID")
	}
	if _, err := store.Put(ctx, testUUID(55), nil); err == nil {
		t.Fatal("Put accepted empty input")
	}
	if _, err := store.Put(ctx, testUUID(55), []byte(`{"x":1} trailing`)); err == nil {
		t.Fatal("Put accepted invalid JSON")
	}
	if _, err := store.Put(ctx, testUUID(55), bytes.Repeat([]byte{'x'}, MaxInputBytes+1)); err == nil {
		t.Fatal("Put accepted oversized input")
	}
	if _, err := store.Get(ctx, testUUID(55), InputReference{Digest: strings.Repeat("a", 64), Key: "other/key", Size: 1}); err == nil {
		t.Fatal("Get accepted a reference outside the session-private prefix")
	}
}

type mutableBlobs struct {
	values map[string][]byte
}

func (b *mutableBlobs) Put(_ context.Context, key string, r io.Reader) error {
	value, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.values[key] = bytes.Clone(value)
	return nil
}

func (b *mutableBlobs) Get(_ context.Context, key string) (io.ReadCloser, error) {
	value, ok := b.values[key]
	if !ok {
		return nil, &storage.BlobNotFoundError{Key: key}
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(value))), nil
}

func (b *mutableBlobs) Delete(_ context.Context, key string) error {
	delete(b.values, key)
	return nil
}

func (b *mutableBlobs) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range b.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

var _ storage.Blobs = (*mutableBlobs)(nil)
