package workflows

import (
	"errors"
	"sort"
	"sync"
)

type definitionKey struct{ name, version string }

type Catalog struct {
	mu          sync.RWMutex
	definitions map[definitionKey]Definition
}

func NewCatalog() *Catalog { return &Catalog{definitions: make(map[definitionKey]Definition)} }

func (c *Catalog) Register(definition Definition) error {
	if definition == nil {
		return &InvalidSchemaError{Field: "definition", Err: errors.New("nil definition")}
	}
	registered, err := definition.registeredCopy()
	if err != nil {
		return err
	}
	metadata := registered.Metadata()
	key := definitionKey{name: metadata.Name(), version: metadata.Version()}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.definitions[key]; exists {
		return &DuplicateDefinitionError{Name: key.name, Version: key.version}
	}
	c.definitions[key] = registered
	return nil
}

func (c *Catalog) Resolve(name, version string) (Definition, error) {
	c.mu.RLock()
	definition, ok := c.definitions[definitionKey{name: name, version: version}]
	c.mu.RUnlock()
	if !ok {
		return nil, &UnknownDefinitionError{Name: name, Version: version}
	}
	return definition, nil
}

func (c *Catalog) List() []Metadata {
	c.mu.RLock()
	list := make([]Metadata, 0, len(c.definitions))
	for _, definition := range c.definitions {
		list = append(list, definition.Metadata())
	}
	c.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool {
		if list[i].Name() == list[j].Name() {
			return list[i].Version() < list[j].Version()
		}
		return list[i].Name() < list[j].Name()
	})
	return list
}
