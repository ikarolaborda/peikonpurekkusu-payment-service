package events

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	// ErrSchemaInvalid marks a payload that violates the schema it was framed
	// with — poison: redelivery cannot fix it.
	ErrSchemaInvalid = errors.New("payload violates schema")

	// ErrUnknownSchema marks a frame whose schema id the registry
	// authoritatively does not know — poison: the frame is junk.
	ErrUnknownSchema = errors.New("schema id unknown to the registry")

	// ErrRegistryUnavailable marks a registry outage (network, timeout, 5xx).
	// Transient: the consumer must hold the record and retry, never dead-letter
	// and never commit past it.
	ErrRegistryUnavailable = errors.New("schema registry unavailable")
)

// Validator checks consumed events against the exact schema the producer
// framed them with (the Confluent frame's schema id), fetched from the
// registry and cached compiled forever — registry ids are immutable.
// Formats are deliberately not asserted (mirrors the platform-wide choice).
type Validator struct {
	baseURL string
	client  *http.Client
	mu      sync.RWMutex
	cache   map[int32]*jsonschema.Schema
}

func NewValidator(registryBaseURL string) *Validator {
	return &Validator{
		baseURL: registryBaseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
		cache:   map[int32]*jsonschema.Schema{},
	}
}

// Validate parses the frame and validates the envelope against its declared
// schema. Callers classify the error with errors.Is against the sentinels above.
func (v *Validator) Validate(ctx context.Context, value []byte) error {
	if len(value) < 6 || value[0] != 0 {
		return fmt.Errorf("%w: not a confluent-framed message", ErrSchemaInvalid)
	}
	id := int32(binary.BigEndian.Uint32(value[1:5]))
	schema, err := v.schema(ctx, id)
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(value[5:])))
	if err != nil {
		return fmt.Errorf("%w: envelope parse: %v", ErrSchemaInvalid, err)
	}
	if err := schema.Validate(doc); err != nil {
		return fmt.Errorf("%w (id %d): %v", ErrSchemaInvalid, id, err)
	}
	return nil
}

func (v *Validator) schema(ctx context.Context, id int32) (*jsonschema.Schema, error) {
	v.mu.RLock()
	s, ok := v.cache[id]
	v.mu.RUnlock()
	if ok {
		return s, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/schemas/ids/%d", v.baseURL, id), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	defer resp.Body.Close()
	// Only a definitive answer from the registry itself may condemn the frame;
	// anything else (5xx, proxy noise) is treated as an outage.
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: id %d", ErrUnknownSchema, id)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: registry answered %d", ErrRegistryUnavailable, resp.StatusCode)
	}

	var body struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(body.Schema))
	if err != nil {
		return nil, fmt.Errorf("%w (id %d): registry returned unparseable schema: %v", ErrSchemaInvalid, id, err)
	}
	compiler := jsonschema.NewCompiler()
	name := fmt.Sprintf("registry-%d.json", id)
	if err := compiler.AddResource(name, doc); err != nil {
		return nil, fmt.Errorf("%w (id %d): %v", ErrSchemaInvalid, id, err)
	}
	compiled, err := compiler.Compile(name)
	if err != nil {
		return nil, fmt.Errorf("%w (id %d): %v", ErrSchemaInvalid, id, err)
	}

	v.mu.Lock()
	v.cache[id] = compiled
	v.mu.Unlock()
	return compiled, nil
}
