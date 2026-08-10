package workflows

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
)

func sessionLeaseKey(sessionID uuid.UUID) (string, error) {
	if sessionID.IsZero() {
		return "", errors.New("workflows: session ID is required")
	}
	key := "sessions/" + sessionID.String() + "/workflows/owner"
	if err := storage.ValidateName(key); err != nil {
		return "", fmt.Errorf("workflows: invalid session lease key: %w", err)
	}
	return key, nil
}

func acquireSessionLease(ctx context.Context, leaser storage.Leaser, sessionID uuid.UUID) (storage.Lease, error) {
	key, err := sessionLeaseKey(sessionID)
	if err != nil {
		return nil, err
	}
	lease, err := leaser.Acquire(ctx, key)
	if err != nil {
		var held *storage.LeaseHeldError
		if errors.As(err, &held) {
			return nil, &SessionOwnedError{SessionID: sessionID, HolderEpoch: held.HolderEpoch}
		}
		return nil, fmt.Errorf("workflows: acquire session ownership: %w", err)
	}
	if lease == nil || lease.Lost() == nil {
		return nil, errors.New("workflows: leaser returned an invalid lease")
	}
	return lease, nil
}
