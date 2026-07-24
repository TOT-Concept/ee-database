// Package framing defines the JSON text frames of the database-sync WebSocket
// protocol (version "1"). Mirrors backend/enricher/services/entity_sync/client_sessions.py.
package framing

import (
	"encoding/json"
	"fmt"
)

const ProtocolVersion = "1"

// === client → server ========================================================

type Hello struct {
	Type    string `json:"type"` // "hello"
	Version string `json:"version"`
	Agent   string `json:"agent"`
}

type Ack struct {
	Type   string `json:"type"` // "ack"
	UpToID int64  `json:"up_to_id"`
}

type ApplyError struct {
	Type    string `json:"type"` // "apply_error"
	DeltaID *int64 `json:"delta_id,omitempty"`
	Message string `json:"message"`
}

type Ping struct {
	Type string `json:"type"` // "ping"
}

// === server → client ========================================================

type Delta struct {
	ID         int64   `json:"id"`
	Kind       string  `json:"kind"` // "data" | "schema"
	Op         string  `json:"op"`   // "upsert" | "delete" | "migrate"
	EntityType *string `json:"entity_type"`
	Revision   *int64  `json:"revision"`
	SQL        string  `json:"sql"`
}

type Batch struct {
	Type    string  `json:"type"` // "batch"
	Deltas  []Delta `json:"deltas"`
	HasMore bool    `json:"has_more"`
}

type Pong struct {
	Type string `json:"type"` // "pong"
}

// Decode parses a server frame by its "type" discriminator.
func Decode(raw []byte) (any, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, fmt.Errorf("frame: %w", err)
	}
	switch head.Type {
	case "batch":
		var f Batch
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, err
		}
		return f, nil
	case "pong":
		return Pong{Type: "pong"}, nil
	default:
		return nil, fmt.Errorf("frame: unknown type %q", head.Type)
	}
}
