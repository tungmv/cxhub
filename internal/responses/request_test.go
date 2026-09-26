package responses

import (
	"encoding/json"
	"testing"
)

func TestSetReasoningEffortPreservesOtherFields(t *testing.T) {
	req, err := Parse([]byte(`{"model":"auto","input":"task","reasoning":{"summary":"auto","effort":"high"},"custom":"kept"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := req.SetReasoningEffort("low"); err != nil {
		t.Fatal(err)
	}
	var body struct {
		Reasoning map[string]string `json:"reasoning"`
		Custom    string            `json:"custom"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body.Reasoning["effort"] != "low" || body.Reasoning["summary"] != "auto" || body.Custom != "kept" {
		t.Fatalf("request body fields not preserved: %s", req.Body)
	}
}
