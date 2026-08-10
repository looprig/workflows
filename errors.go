package workflows

import (
	"errors"
	"fmt"
)

var (
	ErrUnknownDefinition   = errors.New("unknown workflow definition")
	ErrDuplicateDefinition = errors.New("duplicate workflow definition")
	ErrInvalidSchema       = errors.New("invalid workflow schema")
	ErrInvalidInput        = errors.New("invalid workflow input")
)

type UnknownDefinitionError struct{ Name, Version string }

func (e *UnknownDefinitionError) Error() string {
	return fmt.Sprintf("%s: %s@%s", ErrUnknownDefinition, e.Name, e.Version)
}
func (e *UnknownDefinitionError) Unwrap() error { return ErrUnknownDefinition }

type DuplicateDefinitionError struct{ Name, Version string }

func (e *DuplicateDefinitionError) Error() string {
	return fmt.Sprintf("%s: %s@%s", ErrDuplicateDefinition, e.Name, e.Version)
}
func (e *DuplicateDefinitionError) Unwrap() error { return ErrDuplicateDefinition }

type InvalidSchemaError struct {
	Field string
	Err   error
}

func (e *InvalidSchemaError) Error() string {
	return fmt.Sprintf("%s (%s): %v", ErrInvalidSchema, e.Field, e.Err)
}
func (e *InvalidSchemaError) Unwrap() error { return ErrInvalidSchema }

type InvalidInputError struct {
	Field string
	Err   error
}

func (e *InvalidInputError) Error() string {
	return fmt.Sprintf("%s (%s): %v", ErrInvalidInput, e.Field, e.Err)
}
func (e *InvalidInputError) Unwrap() error { return ErrInvalidInput }
