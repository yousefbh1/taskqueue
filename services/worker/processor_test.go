package main

import (
	"encoding/json"
	"testing"
)

func TestProcessSuccess(t *testing.T) {
	j := Job{Type: "demo", Payload: json.RawMessage(`{"sleep_ms":1}`)}
	if err := process(j); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestProcessFail(t *testing.T) {
	j := Job{Type: "fail", Payload: json.RawMessage(`{}`)}
	if err := process(j); err == nil {
		t.Fatalf("expected failure, got nil")
	}
}
