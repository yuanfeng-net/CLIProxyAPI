package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	probeTimeout     = 20 * time.Second
	probeConcurrency = 10
	whamUsageURL     = "https://chatgpt.com/backend-api/wham/usage"
)

// UsageInfo stores the parsed quota usage for a credential.
type UsageInfo struct {
	Email            string    `json:"email"`
	PlanType         string    `json:"plan_type"`
	Allowed          bool      `json:"allowed"`
	LimitReached     bool      `json:"limit_reached"`
	UsedPercent      float64   `json:"used_percent"`
	ResetAt          time.Time `json:"reset_at,omitempty"`
	ResetAfterSec    int64     `json:"reset_after_seconds,omitempty"`
	LimitWindowSec   int64     `json:"limit_window_seconds,omitempty"`
	HasCredits       bool      `json:"has_credits"`
	ProbedAt         time.Time `json:"probed_at"`
}

// ProbeAllQuota queries the real quota usage for all Codex credentials via wham/usage API.
// Results are stored in the state cache and used to mark exhausted credentials as cooled down.
func (m *Manager) ProbeAllQuota(ctx context.Context) {
	if m == nil {
		return
	}
	snapshot := m.snapshotAuths()
	if len(snapshot) == 0 {
		return
	}

	var targets []*Auth
	for _, a := range snapshot {
		if a == nil || a.Disabled || a.Status == StatusDisabled {
			continue
		}
		provider := strings.ToLower(strings.TrimSpace(a.Provider))
		if provider != "codex" {
			continue
		}
		targets = append(targets, a)
	}
	if len(targets) == 0 {
		return
	}

	log.Infof("quota probe: querying usage for %d codex credentials...", len(targets))
	start := time.Now()

	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	var okCount, exhaustedCount, errCount int64
	var mu sync.Mutex

	for _, auth := range targets {
		wg.Add(1)
		go func(a *Auth) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			info, err := m.probeCodexUsage(ctx, a)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errCount++
				return
			}
			if info.LimitReached && !info.Allowed {
				exhaustedCount++
			} else {
				okCount++
			}
		}(auth)
	}
	wg.Wait()

	log.Infof("quota probe: done in %s — %d available, %d exhausted, %d errors",
		time.Since(start).Round(time.Millisecond), okCount, exhaustedCount, errCount)
}

// whamUsageResponse mirrors the JSON returned by chatgpt.com/backend-api/wham/usage.
type whamUsageResponse struct {
	Email    string `json:"email"`
	PlanType string `json:"plan_type"`
	RateLimit *struct {
		Allowed       bool `json:"allowed"`
		LimitReached  bool `json:"limit_reached"`
		PrimaryWindow *struct {
			UsedPercent      float64 `json:"used_percent"`
			LimitWindowSec   int64   `json:"limit_window_seconds"`
			ResetAfterSec    int64   `json:"reset_after_seconds"`
			ResetAt          int64   `json:"reset_at"`
		} `json:"primary_window"`
	} `json:"rate_limit"`
	Credits *struct {
		HasCredits bool `json:"has_credits"`
	} `json:"credits"`
}

func (m *Manager) probeCodexUsage(ctx context.Context, auth *Auth) (*UsageInfo, error) {
	if auth == nil {
		return nil, fmt.Errorf("nil auth")
	}

	// Resolve access token.
	token := ""
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["access_token"].(string); ok {
			token = strings.TrimSpace(v)
		}
	}
	if token == "" {
		return nil, fmt.Errorf("no access_token for %s", auth.ID)
	}

	// Resolve chatgpt_account_id from id_token JWT claims or metadata.
	accountID := resolveCodexAccountID(auth)

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, whamUsageURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal")
	if accountID != "" {
		req.Header.Set("Chatgpt-Account-Id", accountID)
	}

	// Use executor's HttpRequest if available for proxy support.
	exec := m.executorFor("codex")
	var resp *http.Response
	if exec != nil {
		resp, err = exec.HttpRequest(probeCtx, auth, req)
	} else {
		resp, err = http.DefaultClient.Do(req)
	}
	if err != nil {
		log.Debugf("quota probe: %s request error: %v", auth.ID, err)
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		log.Debugf("quota probe: %s returned %d", auth.ID, resp.StatusCode)
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var usage whamUsageResponse
	if err = json.Unmarshal(body, &usage); err != nil {
		return nil, fmt.Errorf("parse usage: %w", err)
	}

	now := time.Now()
	info := &UsageInfo{
		Email:    usage.Email,
		PlanType: usage.PlanType,
		ProbedAt: now,
	}
	if usage.RateLimit != nil {
		info.Allowed = usage.RateLimit.Allowed
		info.LimitReached = usage.RateLimit.LimitReached
		if pw := usage.RateLimit.PrimaryWindow; pw != nil {
			info.UsedPercent = pw.UsedPercent
			info.ResetAfterSec = pw.ResetAfterSec
			info.LimitWindowSec = pw.LimitWindowSec
			if pw.ResetAt > 0 {
				info.ResetAt = time.Unix(pw.ResetAt, 0)
			}
		}
	}
	if usage.Credits != nil {
		info.HasCredits = usage.Credits.HasCredits
	}

	// Apply cooldown if quota is exhausted.
	if info.LimitReached && !info.Allowed {
		cooldown := time.Duration(info.ResetAfterSec) * time.Second
		if cooldown <= 0 && !info.ResetAt.IsZero() && info.ResetAt.After(now) {
			cooldown = info.ResetAt.Sub(now)
		}
		if cooldown <= 0 {
			cooldown = 7 * 24 * time.Hour // fallback: 1 week
		}
		m.MarkResult(ctx, Result{
			AuthID:     auth.ID,
			Provider:   "codex",
			Success:    false,
			RetryAfter: &cooldown,
			Error: &Error{
				Code:       "usage_limit_reached",
				Message:    fmt.Sprintf("quota exhausted (%.0f%% used), resets at %s", info.UsedPercent, info.ResetAt.Format(time.RFC3339)),
				HTTPStatus: http.StatusTooManyRequests,
			},
		})
		log.Debugf("quota probe: %s exhausted (%.0f%%), resets %s", auth.ID, info.UsedPercent, info.ResetAt.Format("2006-01-02 15:04"))
	} else {
		// Clear any stale cooldown.
		m.MarkResult(ctx, Result{
			AuthID:   auth.ID,
			Provider: "codex",
			Success:  true,
		})
	}

	// Store usage info in state cache for API response enrichment.
	if m.stateCache != nil {
		m.stateCache.UpdateUsageInfo(auth.ID, info)
	}

	return info, nil
}

func resolveCodexAccountID(auth *Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	// Try id_token JWT claims first (parsed by synthesizer).
	if auth.Attributes != nil {
		// The management panel sends Chatgpt-Account-Id from id_token claims.
		// We can extract it from metadata if available.
	}
	if idToken, ok := auth.Metadata["id_token"].(string); ok && idToken != "" {
		// Parse JWT without verification to extract chatgpt_account_id.
		parts := strings.Split(idToken, ".")
		if len(parts) >= 2 {
			payload := parts[1]
			// Pad base64
			switch len(payload) % 4 {
			case 2:
				payload += "=="
			case 3:
				payload += "="
			}
			if decoded, err := base64Decode(payload); err == nil {
				var claims map[string]any
				if json.Unmarshal(decoded, &claims) == nil {
					if info, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
						if aid, ok := info["chatgpt_account_id"].(string); ok {
							return strings.TrimSpace(aid)
						}
					}
				}
			}
		}
	}
	return ""
}

func base64Decode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
