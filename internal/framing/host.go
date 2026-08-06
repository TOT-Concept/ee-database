package framing

// Host control-plane frames (issue #65), protocol "1". Mirrors
// backend/enricher/services/entity_sync/transport/host_sessions.py.
//
// The control plane carries assignments and host status only — claimed
// databases open the ordinary per-database data plane (sync.Dial + Pump).

import (
	"encoding/json"
	"fmt"
)

// === host → server ==========================================================

type HostHello struct {
	Type    string `json:"type"` // "hello"
	Version string `json:"version"`
	Agent   string `json:"agent"`
	// Dialect of the host's base DSN — feeds the server's assignment
	// eligibility (a mysql registration never auto-assigns to a postgres host).
	Dialect string `json:"dialect,omitempty"`
}

// Preflight (preflight report) and Ping are shared with the data plane.

// === server → host ==========================================================

// HostRegistration is one database sync assigned to this host.
type HostRegistration struct {
	DatabaseID   string `json:"database_id"`
	Name         string `json:"name"`
	Dialect      string `json:"dialect"`
	DatabaseName string `json:"database_name"` // server-suggested physical name
	// Claimable is false while another client holds the active credential — a
	// live classic pairing is reported, never evicted.
	Claimable           bool `json:"claimable"`
	HasActiveCredential bool `json:"has_active_credential"`
}

// HostAssigned is the reconcile list sent after hello: the full set of
// registrations assigned to this host. Push events are a doorbell; this list
// is the source of truth, so offline-created registrations are never missed.
type HostAssigned struct {
	Type          string             `json:"type"` // "assigned"
	Registrations []HostRegistration `json:"registrations"`
}

type HostRegistrationAssigned struct {
	Type         string           `json:"type"` // "registration_assigned"
	Registration HostRegistration `json:"registration"`
}

type HostRegistrationRevoked struct {
	Type       string `json:"type"` // "registration_revoked"
	DatabaseID string `json:"database_id"`
}

// DecodeHost parses a server control-plane frame by its "type" discriminator.
func DecodeHost(raw []byte) (any, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, fmt.Errorf("host frame: %w", err)
	}
	switch head.Type {
	case "assigned":
		var f HostAssigned
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, err
		}
		return f, nil
	case "registration_assigned":
		var f HostRegistrationAssigned
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, err
		}
		return f, nil
	case "registration_revoked":
		var f HostRegistrationRevoked
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, err
		}
		return f, nil
	case "pong":
		return Pong{Type: "pong"}, nil
	default:
		return nil, fmt.Errorf("host frame: unknown type %q", head.Type)
	}
}
