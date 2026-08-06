package auth

// Managed-host control plane calls (issue #65): host token exchange, the
// claim endpoint, and the host WS URL.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HostExchangeResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	HostID      string `json:"host_id,omitempty"`
	HostName    string `json:"host_name,omitempty"`
}

// HostExchange swaps the host refresh token for a short-lived access token.
func (c *Client) HostExchange(ctx context.Context, refreshToken string) (*HostExchangeResponse, error) {
	u := fmt.Sprintf("%s/api/database-sync/host-exchange?refresh_token=%s",
		c.BaseURL, url.QueryEscape(refreshToken))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("host-exchange: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("host-exchange: server returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out HostExchangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("host-exchange: decode response: %w", err)
	}
	return &out, nil
}

// HostWSURL returns the control-plane WebSocket URL derived from BaseURL.
func (c *Client) HostWSURL() string {
	switch {
	case strings.HasPrefix(c.BaseURL, "https://"):
		return "wss://" + c.BaseURL[len("https://"):] + "/api/database-sync/host-ws"
	case strings.HasPrefix(c.BaseURL, "http://"):
		return "ws://" + c.BaseURL[len("http://"):] + "/api/database-sync/host-ws"
	default:
		return c.BaseURL + "/api/database-sync/host-ws"
	}
}

// ClaimResponse carries everything the host needs to build the local profile.
type ClaimResponse struct {
	RefreshToken          string `json:"refresh_token"`
	ServerURL             string `json:"server_url"`
	DatabaseID            string `json:"database_id"`
	DatabaseName          string `json:"database_name"`
	Dialect               string `json:"dialect"`
	SuggestedDatabaseName string `json:"suggested_database_name"`
}

// ErrNotClaimable marks the claim 409: another client holds the database's
// active credential — reported, never evicted (the hijack guard).
type ErrNotClaimable struct{ Body string }

func (e ErrNotClaimable) Error() string { return "not claimable: " + e.Body }

// Claim mints the per-database credential for a registration assigned to this
// host, authorized by the host access token.
func (c *Client) Claim(ctx context.Context, accessToken, databaseID string) (*ClaimResponse, error) {
	body, _ := json.Marshal(map[string]string{"database_id": databaseID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/api/database-sync/claim", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusConflict {
			return nil, ErrNotClaimable{Body: strings.TrimSpace(string(raw))}
		}
		return nil, fmt.Errorf("claim: server returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("claim: decode response: %w", err)
	}
	return &out, nil
}
