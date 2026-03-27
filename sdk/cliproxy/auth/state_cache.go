package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	runtimeStateCacheFile  = ".runtime-state.json"
	stateCacheSaveDebounce = 3 * time.Second
)

// cachedAuthState stores the minimal runtime state needed to survive restarts.
type cachedAuthState struct {
	Unavailable    bool                  `json:"unavailable,omitempty"`
	NextRetryAfter time.Time             `json:"next_retry_after,omitempty"`
	Quota          QuotaState            `json:"quota,omitempty"`
	ModelStates    map[string]*ModelState `json:"model_states,omitempty"`
	LastError      *Error                `json:"last_error,omitempty"`
	Status         Status                `json:"status,omitempty"`
	StatusMessage  string                `json:"status_message,omitempty"`
	Usage          json.RawMessage       `json:"usage,omitempty"`
}

// StateStoreBackend abstracts the persistence layer for runtime state.
type StateStoreBackend interface {
	LoadAll(ctx context.Context) (map[string]*cachedAuthState, error)
	SaveAll(ctx context.Context, states map[string]*cachedAuthState) error
}

// runtimeStateCache persists auth cooldown/quota state across restarts.
type runtimeStateCache struct {
	mu      sync.RWMutex
	backend StateStoreBackend
	states  map[string]*cachedAuthState
	dirty   bool
	saveCh  chan struct{}
	stopCh  chan struct{}
}

func newRuntimeStateCache(backend StateStoreBackend) *runtimeStateCache {
	c := &runtimeStateCache{
		backend: backend,
		states:  make(map[string]*cachedAuthState),
		saveCh:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	c.load()
	go c.saveLoop()
	return c
}

func (c *runtimeStateCache) load() {
	if c.backend == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	states, err := c.backend.LoadAll(ctx)
	if err != nil {
		log.Warnf("runtime state cache: load error: %v", err)
		return
	}
	now := time.Now()
	for id, state := range states {
		if state == nil || !hasActiveState(state, now) {
			delete(states, id)
		}
	}
	c.mu.Lock()
	c.states = states
	c.mu.Unlock()
	log.Infof("runtime state cache: restored %d auth cooldown states", len(states))
}

func hasActiveState(state *cachedAuthState, now time.Time) bool {
	if state == nil {
		return false
	}
	// Keep entries that have usage info regardless of cooldown.
	if len(state.Usage) > 0 {
		return true
	}
	if state.Unavailable && !state.NextRetryAfter.IsZero() && state.NextRetryAfter.After(now) {
		return true
	}
	if state.Quota.Exceeded && !state.Quota.NextRecoverAt.IsZero() && state.Quota.NextRecoverAt.After(now) {
		return true
	}
	for _, ms := range state.ModelStates {
		if ms == nil {
			continue
		}
		if ms.Unavailable && !ms.NextRetryAfter.IsZero() && ms.NextRetryAfter.After(now) {
			return true
		}
		if ms.Quota.Exceeded && !ms.Quota.NextRecoverAt.IsZero() && ms.Quota.NextRecoverAt.After(now) {
			return true
		}
	}
	return false
}

func (c *runtimeStateCache) Get(id string) *cachedAuthState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.states[id]
}

func (c *runtimeStateCache) Update(id string, auth *Auth) {
	if auth == nil || id == "" {
		return
	}
	state := &cachedAuthState{
		Unavailable:    auth.Unavailable,
		NextRetryAfter: auth.NextRetryAfter,
		Quota:          auth.Quota,
		LastError:      auth.LastError,
		Status:         auth.Status,
		StatusMessage:  auth.StatusMessage,
	}
	if len(auth.ModelStates) > 0 {
		state.ModelStates = make(map[string]*ModelState, len(auth.ModelStates))
		for k, v := range auth.ModelStates {
			if v != nil {
				state.ModelStates[k] = v.Clone()
			}
		}
	}
	c.mu.Lock()
	c.states[id] = state
	c.dirty = true
	c.mu.Unlock()
	select {
	case c.saveCh <- struct{}{}:
	default:
	}
}

func (c *runtimeStateCache) Remove(id string) {
	c.mu.Lock()
	if _, ok := c.states[id]; ok {
		delete(c.states, id)
		c.dirty = true
	}
	c.mu.Unlock()
}

// UpdateUsageInfo stores quota usage info for an auth credential.
func (c *runtimeStateCache) UpdateUsageInfo(id string, info interface{}) {
	if id == "" || info == nil {
		return
	}
	data, err := json.Marshal(info)
	if err != nil {
		return
	}
	c.mu.Lock()
	state, ok := c.states[id]
	if !ok {
		state = &cachedAuthState{}
		c.states[id] = state
	}
	state.Usage = data
	c.dirty = true
	c.mu.Unlock()
	select {
	case c.saveCh <- struct{}{}:
	default:
	}
}

// GetUsageInfo returns the stored usage info for an auth credential.
func (c *runtimeStateCache) GetUsageInfo(id string) json.RawMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if state, ok := c.states[id]; ok && state != nil {
		return state.Usage
	}
	return nil
}

func (c *runtimeStateCache) saveLoop() {
	timer := time.NewTimer(stateCacheSaveDebounce)
	timer.Stop()
	for {
		select {
		case <-c.saveCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(stateCacheSaveDebounce)
		case <-timer.C:
			c.flush()
		case <-c.stopCh:
			timer.Stop()
			c.flush()
			return
		}
	}
}

func (c *runtimeStateCache) flush() {
	if c.backend == nil {
		return
	}
	c.mu.Lock()
	if !c.dirty {
		c.mu.Unlock()
		return
	}
	now := time.Now()
	for id, state := range c.states {
		if !hasActiveState(state, now) {
			delete(c.states, id)
		}
	}
	snapshot := make(map[string]*cachedAuthState, len(c.states))
	for k, v := range c.states {
		snapshot[k] = v
	}
	c.dirty = false
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.backend.SaveAll(ctx, snapshot); err != nil {
		log.Warnf("runtime state cache: save error: %v", err)
	}
}

func (c *runtimeStateCache) Stop() {
	close(c.stopCh)
}

// ApplyTo merges cached runtime state into an Auth object (only non-expired state).
func (c *runtimeStateCache) ApplyTo(auth *Auth) {
	if auth == nil || auth.ID == "" {
		return
	}
	cached := c.Get(auth.ID)
	if cached == nil {
		return
	}
	now := time.Now()
	if cached.Unavailable && !cached.NextRetryAfter.IsZero() && cached.NextRetryAfter.After(now) {
		auth.Unavailable = true
		auth.NextRetryAfter = cached.NextRetryAfter
	}
	if cached.Quota.Exceeded && !cached.Quota.NextRecoverAt.IsZero() && cached.Quota.NextRecoverAt.After(now) {
		auth.Quota = cached.Quota
	}
	if cached.LastError != nil {
		auth.LastError = cloneError(cached.LastError)
	}
	if cached.Status == StatusError {
		auth.Status = cached.Status
		auth.StatusMessage = cached.StatusMessage
	}
	if len(cached.ModelStates) > 0 {
		if auth.ModelStates == nil {
			auth.ModelStates = make(map[string]*ModelState, len(cached.ModelStates))
		}
		for model, ms := range cached.ModelStates {
			if ms == nil {
				continue
			}
			if ms.Unavailable && !ms.NextRetryAfter.IsZero() && ms.NextRetryAfter.After(now) {
				auth.ModelStates[model] = ms.Clone()
			}
		}
	}
}

// --- File-based backend (default) ---

type fileStateBackend struct {
	dir string
}

// NewFileStateBackend creates a file-based state persistence backend.
func NewFileStateBackend(dir string) StateStoreBackend {
	return &fileStateBackend{dir: dir}
}

func (f *fileStateBackend) LoadAll(_ context.Context) (map[string]*cachedAuthState, error) {
	data, err := os.ReadFile(filepath.Join(f.dir, runtimeStateCacheFile))
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]*cachedAuthState), nil
		}
		return nil, err
	}
	var states map[string]*cachedAuthState
	if err = json.Unmarshal(data, &states); err != nil {
		return make(map[string]*cachedAuthState), nil
	}
	return states, nil
}

func (f *fileStateBackend) SaveAll(_ context.Context, states map[string]*cachedAuthState) error {
	data, err := json.Marshal(states)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(f.dir, 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(f.dir, runtimeStateCacheFile+".tmp")
	target := filepath.Join(f.dir, runtimeStateCacheFile)
	if err = os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err = os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
