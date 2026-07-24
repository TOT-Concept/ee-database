// Package auth handles communication with the Entity Enricher database-sync API.
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to /api/database-sync/* endpoints on the Entity Enricher server.
type Client struct {
	BaseURL string // e.g. https://entityenricher.ai or http://localhost:18808
	HTTP    *http.Client
}

// New constructs a Client. ws:// and wss:// inputs are rewritten to http(s).
func New(baseURL string) *Client {
	return &Client{
		BaseURL: normalizeBaseURL(baseURL),
		HTTP:    &http.Client{Timeout: 60 * time.Second},
	}
}

func normalizeBaseURL(raw string) string {
	raw = strings.TrimRight(raw, "/")
	switch {
	case strings.HasPrefix(raw, "wss://"):
		return "https://" + raw[len("wss://"):]
	case strings.HasPrefix(raw, "ws://"):
		return "http://" + raw[len("ws://"):]
	default:
		return raw
	}
}

// WSURL returns the data-plane WebSocket URL derived from BaseURL.
func (c *Client) WSURL() string {
	switch {
	case strings.HasPrefix(c.BaseURL, "https://"):
		return "wss://" + c.BaseURL[len("https://"):] + "/api/database-sync/ws"
	case strings.HasPrefix(c.BaseURL, "http://"):
		return "ws://" + c.BaseURL[len("http://"):] + "/api/database-sync/ws"
	default:
		return c.BaseURL + "/api/database-sync/ws"
	}
}

type ExchangeResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	// Paired-database metadata (servers ≥ 2026-07 send it) — lets the CLI key
	// and self-heal its local profile after a token-paste pairing.
	DatabaseID   string `json:"database_id,omitempty"`
	DatabaseName string `json:"database_name,omitempty"`
	Dialect      string `json:"dialect,omitempty"`
}

// Exchange swaps the refresh token for a short-lived access token.
func (c *Client) Exchange(ctx context.Context, refreshToken string) (*ExchangeResponse, error) {
	u := fmt.Sprintf("%s/api/database-sync/exchange?refresh_token=%s",
		c.BaseURL, url.QueryEscape(refreshToken))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("exchange: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("exchange: server returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out ExchangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("exchange: decode response: %w", err)
	}
	return &out, nil
}

// FetchSnapshot downloads the bootstrap .sql snapshot over the credential.
// Returns the SQL text and the delta cursor it embeds.
func (c *Client) FetchSnapshot(ctx context.Context, accessToken string) (string, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.BaseURL+"/api/database-sync/snapshot", nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", 0, fmt.Errorf("snapshot: server returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(body)))
	}
	sqlText, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("snapshot: read: %w", err)
	}
	cursor, _ := strconv.ParseInt(resp.Header.Get("X-EE-Next-Cursor"), 10, 64)
	return string(sqlText), cursor, nil
}

// === Device-code (RFC 8628-style) pairing ==================================

type DeviceCodeResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

type DeviceCodePollResponse struct {
	Status       string `json:"status"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ServerURL    string `json:"server_url,omitempty"`
	DatabaseID   string `json:"database_id,omitempty"`
	DatabaseName string `json:"database_name,omitempty"`
	Dialect      string `json:"dialect,omitempty"`
}

// StartDeviceCode initiates a device-code pairing.
func (c *Client) StartDeviceCode(ctx context.Context, labelHint string) (*DeviceCodeResponse, error) {
	body, _ := json.Marshal(map[string]any{"label_hint": labelHint})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/api/database-sync/device-code", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("device-code: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("device-code: server returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out DeviceCodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("device-code: decode: %w", err)
	}
	return &out, nil
}

// PollDeviceCode polls a pending pairing once; callers branch on resp.Status.
func (c *Client) PollDeviceCode(ctx context.Context, deviceCode string) (*DeviceCodePollResponse, error) {
	body, _ := json.Marshal(map[string]string{"device_code": deviceCode})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/api/database-sync/poll", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poll: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poll: server returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out DeviceCodePollResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("poll: decode: %w", err)
	}
	return &out, nil
}
