package responses

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type Request struct {
	Body   []byte
	Model  string
	Stream bool
}

func Parse(body []byte) (*Request, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("invalid JSON request: %w", err)
	}
	model := ""
	if raw, ok := fields["model"]; ok {
		if err := json.Unmarshal(raw, &model); err != nil {
			return nil, fmt.Errorf("model must be a string: %w", err)
		}
	}
	stream := false
	if raw, ok := fields["stream"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &stream); err != nil {
			return nil, fmt.Errorf("stream must be a boolean: %w", err)
		}
	}
	return &Request{Body: append([]byte(nil), body...), Model: model, Stream: stream}, nil
}

func (r *Request) WithModel(model string) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.Body, &fields); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}
	encoded, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	fields["model"] = encoded
	var out bytes.Buffer
	if err := json.NewEncoder(&out).Encode(fields); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (r *Request) SetReasoningEffort(effort string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.Body, &fields); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	var reasoning map[string]json.RawMessage
	if raw := fields["reasoning"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &reasoning); err != nil {
			return fmt.Errorf("reasoning must be an object: %w", err)
		}
	}
	if reasoning == nil {
		reasoning = make(map[string]json.RawMessage)
	}
	encoded, err := json.Marshal(effort)
	if err != nil {
		return err
	}
	reasoning["effort"] = encoded
	fields["reasoning"], err = json.Marshal(reasoning)
	if err != nil {
		return err
	}
	r.Body, err = json.Marshal(fields)
	if err != nil {
		return err
	}
	return nil
}
