package workflows

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const runRecordVersion = 2

type runRecordEnvelope struct {
	Version int `json:"version"`
	Run     Run `json:"run"`
}

func encodeRunRecord(run Run) ([]byte, error) {
	if err := validateRun(run); err != nil {
		return nil, err
	}
	run = cloneRun(run)
	run.Revision = 0
	raw, err := json.Marshal(runRecordEnvelope{Version: runRecordVersion, Run: run})
	if err != nil {
		return nil, fmt.Errorf("encode run record: %w", err)
	}
	if len(raw) > MaxRunRecordBytes {
		return nil, fmt.Errorf("run record exceeds %d bytes", MaxRunRecordBytes)
	}
	return raw, nil
}

func decodeRunRecord(key string, raw []byte) (Run, error) {
	corrupt := func(err error) (Run, error) {
		return Run{}, &CorruptRecordError{Key: key, Err: err}
	}
	if len(raw) == 0 {
		return corrupt(errors.New("empty record"))
	}
	if len(raw) > MaxRunRecordBytes {
		return corrupt(fmt.Errorf("record exceeds %d bytes", MaxRunRecordBytes))
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope runRecordEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return corrupt(err)
	}
	if err := requireEOF(decoder); err != nil {
		return corrupt(err)
	}
	if envelope.Version != runRecordVersion {
		return corrupt(fmt.Errorf("unsupported record version %d", envelope.Version))
	}
	if err := validateRun(envelope.Run); err != nil {
		return corrupt(err)
	}
	return cloneRun(envelope.Run), nil
}
