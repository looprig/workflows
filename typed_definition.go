package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/looprig/flow/pkg/flow"
)

type StateDecoder[S any] func(json.RawMessage) (S, error)
type ResumeDecoder func(json.RawMessage) (any, error)
type StatusSummarizer[S any] func(S) string

func StrictJSONDecoder[S any](raw json.RawMessage) (S, error) {
	var value S
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	if err := requireEOF(decoder); err != nil {
		return value, err
	}
	return value, nil
}

func TypedResumeDecoder[R any](decoder StateDecoder[R]) ResumeDecoder {
	if decoder == nil {
		return nil
	}
	return func(raw json.RawMessage) (any, error) { return decoder(raw) }
}

type registration struct {
	input  *compiledSchema
	resume *compiledSchema
}

type TypedDefinition[S any] struct {
	metadata      Metadata
	runner        *flow.Runner[S]
	store         flow.CheckpointStore
	stateDecoder  StateDecoder[S]
	resumeDecoder ResumeDecoder
	summarizer    StatusSummarizer[S]
	registration  *registration
}

func NewTypedDefinition[S any](metadata Metadata, runner *flow.Runner[S], store flow.CheckpointStore, stateDecoder StateDecoder[S], resumeDecoder ResumeDecoder, summarizer StatusSummarizer[S]) (*TypedDefinition[S], error) {
	if runner == nil || store == nil || stateDecoder == nil {
		return nil, &InvalidSchemaError{Field: "definition", Err: errors.New("runner, checkpoint store, and state decoder are required")}
	}
	if len(metadata.resumeSchema) > 0 && resumeDecoder == nil {
		return nil, &InvalidSchemaError{Field: "resume decoder", Err: errors.New("required when a resume schema is present")}
	}
	return &TypedDefinition[S]{
		metadata: metadata.clone(), runner: runner, store: store,
		stateDecoder: stateDecoder, resumeDecoder: resumeDecoder, summarizer: summarizer,
	}, nil
}

func (d *TypedDefinition[S]) Metadata() Metadata {
	if d == nil {
		return Metadata{}
	}
	return d.metadata.clone()
}

func (d *TypedDefinition[S]) registeredCopy() (Definition, error) {
	if d == nil {
		return nil, &InvalidSchemaError{Field: "definition", Err: errors.New("nil definition")}
	}
	input, err := compileStrictSchema("input", d.metadata.inputSchema)
	if err != nil {
		return nil, err
	}
	var resume *compiledSchema
	if len(d.metadata.resumeSchema) > 0 {
		resume, err = compileStrictSchema("resume", d.metadata.resumeSchema)
		if err != nil {
			return nil, err
		}
	}
	copy := *d
	copy.metadata = d.metadata.clone()
	copy.registration = &registration{input: input, resume: resume}
	return &copy, nil
}

func (d *TypedDefinition[S]) ValidateInput(raw json.RawMessage) (ValidatedInput, error) {
	if d == nil || d.registration == nil {
		return ValidatedInput{}, invalidInput("input", errors.New("definition is not registered"))
	}
	if err := d.registration.input.validate(raw); err != nil {
		return ValidatedInput{}, invalidInput("input", err)
	}
	state, err := d.stateDecoder(raw)
	if err != nil {
		return ValidatedInput{}, invalidInput("input", err)
	}
	return ValidatedInput{owner: d.registration, value: state}, nil
}

func (d *TypedDefinition[S]) ValidateResume(raw json.RawMessage) (ValidatedResume, error) {
	if d == nil || d.registration == nil || d.registration.resume == nil || d.resumeDecoder == nil {
		return ValidatedResume{}, invalidInput("resume", errors.New("resume is not supported"))
	}
	if err := d.registration.resume.validate(raw); err != nil {
		return ValidatedResume{}, invalidInput("resume", err)
	}
	payload, err := d.resumeDecoder(raw)
	if err != nil {
		return ValidatedResume{}, invalidInput("resume", err)
	}
	return ValidatedResume{owner: d.registration, value: payload}, nil
}

func (d *TypedDefinition[S]) Start(ctx context.Context, input ValidatedInput, opts ...flow.RunOption) (*Result, error) {
	if d == nil || d.registration == nil || input.owner != d.registration {
		return nil, invalidInput("input", errors.New("validation token does not belong to this definition"))
	}
	state, ok := input.value.(S)
	if !ok {
		return nil, invalidInput("input", errors.New("validated state has the wrong Go type"))
	}
	result, err := d.runner.Run(ctx, state, opts...)
	if err != nil {
		return nil, err
	}
	return d.result(result)
}

func (d *TypedDefinition[S]) Resume(ctx context.Context, id flow.GraphRunID, resume ValidatedResume, opts ...flow.RunOption) (*Result, error) {
	if d == nil || d.registration == nil || resume.owner != d.registration {
		return nil, invalidInput("resume", errors.New("validation token does not belong to this definition"))
	}
	result, err := d.runner.Resume(ctx, id, resume.value, opts...)
	if err != nil {
		return nil, err
	}
	return d.result(result)
}

// Adopt continues a durable running Flow checkpoint after a supervisor
// restart. It is intentionally separate from Resume: adoption carries no
// user payload and is only selected after the supervisor observes a running,
// nonterminal checkpoint. Interrupted checkpoints still require ValidateResume
// and an explicit user action.
func (d *TypedDefinition[S]) Adopt(ctx context.Context, id flow.GraphRunID, opts ...flow.RunOption) (*Result, error) {
	result, err := d.runner.Resume(ctx, id, nil, opts...)
	if err != nil {
		return nil, err
	}
	return d.result(result)
}

func (d *TypedDefinition[S]) Get(ctx context.Context, id flow.GraphRunID) (*Result, error) {
	result, err := d.runner.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return d.result(result)
}

func (d *TypedDefinition[S]) History(ctx context.Context, id flow.GraphRunID) ([]flow.GraphRunState, error) {
	checkpoints, err := d.store.History(ctx, id)
	if err != nil {
		return nil, err
	}
	history := make([]flow.GraphRunState, len(checkpoints))
	for i, checkpoint := range checkpoints {
		history[i] = checkpoint.Run
	}
	return history, nil
}

func (d *TypedDefinition[S]) Cancel(ctx context.Context, id flow.GraphRunID, reason string, opts ...flow.RunOption) error {
	return d.runner.Cancel(ctx, id, reason, opts...)
}

func (d *TypedDefinition[S]) result(result *flow.Result[S]) (*Result, error) {
	state, err := json.Marshal(result.State)
	if err != nil {
		return nil, fmt.Errorf("workflows: encode state: %w", err)
	}
	summary := ""
	if d.summarizer != nil {
		summary = boundSummary(d.summarizer(result.State))
	}
	return &Result{Run: result.Run, State: state, Interrupts: result.Interrupts, Halt: result.Halt, Summary: summary}, nil
}

func boundSummary(summary string) string {
	if len(summary) <= MaxStatusSummaryBytes {
		return summary
	}
	summary = summary[:MaxStatusSummaryBytes]
	for !utf8.ValidString(summary) {
		summary = summary[:len(summary)-1]
	}
	return summary
}

func invalidInput(field string, err error) error { return &InvalidInputError{Field: field, Err: err} }

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("trailing JSON value")
	}
	return err
}

var _ Definition = (*TypedDefinition[struct{}])(nil)
