package workflows

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/looprig/core/uuid"
)

var activityNamespace = uuid.MustParse("d5e20f2c-8897-5bc0-9de8-3d65d4ebf053")

type activityIdentity struct {
	SessionID uuid.UUID
	RunID     uuid.UUID
	Kind      activityKind
	Revision  uint64
	VertexID  uuid.UUID
	Ordinal   uint32
}

// stableActivityID derives a deterministic RFC 4122 variant UUID. Version 5
// identifies it as name based; SHA-256 supplies the name digest before the
// UUID's fixed 128-bit representation is selected.
func stableActivityID(identity activityIdentity) uuid.UUID {
	hash := sha256.New()
	writeIdentityField(hash, activityNamespace[:])
	writeIdentityField(hash, identity.SessionID[:])
	writeIdentityField(hash, identity.RunID[:])
	writeIdentityField(hash, []byte(identity.Kind))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], identity.Revision)
	writeIdentityField(hash, number[:])
	writeIdentityField(hash, identity.VertexID[:])
	var ordinal [4]byte
	binary.BigEndian.PutUint32(ordinal[:], identity.Ordinal)
	writeIdentityField(hash, ordinal[:])
	sum := hash.Sum(nil)
	var id uuid.UUID
	copy(id[:], sum[:len(id)])
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

type identityWriter interface{ Write([]byte) (int, error) }

func writeIdentityField(writer identityWriter, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(value)
}
