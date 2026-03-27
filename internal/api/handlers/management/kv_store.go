package management

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// KVBackend abstracts key-value persistence for panel settings.
type KVBackend interface {
	Get(ctx context.Context, key string) (json.RawMessage, error)
	Set(ctx context.Context, key string, value json.RawMessage) error
	Delete(ctx context.Context, key string) error
	ListKeys(ctx context.Context) ([]string, error)
}

// --- PostgreSQL backend ---

type pgKVBackend struct {
	db    *sql.DB
	table string
}

// NewPgKVBackend creates a PostgreSQL-backed KV store. Table is auto-created.
func NewPgKVBackend(db *sql.DB, schema string) (KVBackend, error) {
	if db == nil {
		return nil, fmt.Errorf("kv: db is nil")
	}
	table := kvQuote("panel_settings")
	if s := strings.TrimSpace(schema); s != "" {
		table = kvQuote(s) + "." + kvQuote("panel_settings")
	}
	b := &pgKVBackend{db: db, table: table}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key   TEXT PRIMARY KEY,
			value JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, b.table)
	if _, err := b.db.ExecContext(ctx, query); err != nil {
		return nil, fmt.Errorf("kv: create table: %w", err)
	}
	return b, nil
}

func (b *pgKVBackend) Get(ctx context.Context, key string) (json.RawMessage, error) {
	var data []byte
	err := b.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT value FROM %s WHERE key = $1", b.table), key).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func (b *pgKVBackend) Set(ctx context.Context, key string, value json.RawMessage) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (key, value, updated_at) VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
	`, b.table)
	_, err := b.db.ExecContext(ctx, query, key, []byte(value))
	return err
}

func (b *pgKVBackend) Delete(ctx context.Context, key string) error {
	_, err := b.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE key = $1", b.table), key)
	return err
}

func (b *pgKVBackend) ListKeys(ctx context.Context) ([]string, error) {
	rows, err := b.db.QueryContext(ctx,
		fmt.Sprintf("SELECT key FROM %s ORDER BY key", b.table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err = rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// --- In-memory backend (fallback when no PG) ---

type memKVBackend struct {
	mu   sync.RWMutex
	data map[string]json.RawMessage
}

// NewMemKVBackend creates an in-memory KV store (lost on restart).
func NewMemKVBackend() KVBackend {
	return &memKVBackend{data: make(map[string]json.RawMessage)}
}

func (b *memKVBackend) Get(_ context.Context, key string) (json.RawMessage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.data[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (b *memKVBackend) Set(_ context.Context, key string, value json.RawMessage) error {
	b.mu.Lock()
	b.data[key] = value
	b.mu.Unlock()
	return nil
}

func (b *memKVBackend) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.data, key)
	b.mu.Unlock()
	return nil
}

func (b *memKVBackend) ListKeys(_ context.Context) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := make([]string, 0, len(b.data))
	for k := range b.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func kvQuote(id string) string {
	return "\"" + strings.ReplaceAll(id, "\"", "\"\"") + "\""
}

// --- HTTP handlers ---

// GetKV returns a stored value by key.
func (h *Handler) GetKV(c *gin.Context) {
	key := strings.TrimSpace(c.Query("key"))
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	backend := h.kvBackend()
	if backend == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kv store unavailable"})
		return
	}
	value, err := backend.Get(c.Request.Context(), key)
	if err != nil {
		log.Debugf("kv get error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read failed"})
		return
	}
	if value == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.Data(http.StatusOK, "application/json", value)
}

// PutKV stores a value by key.
func (h *Handler) PutKV(c *gin.Context) {
	key := strings.TrimSpace(c.Query("key"))
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	backend := h.kvBackend()
	if backend == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kv store unavailable"})
		return
	}
	data, err := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20)) // 1MB limit
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body failed"})
		return
	}
	if !json.Valid(data) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if err = backend.Set(c.Request.Context(), key, json.RawMessage(data)); err != nil {
		log.Debugf("kv set error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// DeleteKV removes a stored value by key.
func (h *Handler) DeleteKV(c *gin.Context) {
	key := strings.TrimSpace(c.Query("key"))
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	backend := h.kvBackend()
	if backend == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kv store unavailable"})
		return
	}
	if err := backend.Delete(c.Request.Context(), key); err != nil {
		log.Debugf("kv delete error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ListKVKeys returns all stored keys.
func (h *Handler) ListKVKeys(c *gin.Context) {
	backend := h.kvBackend()
	if backend == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kv store unavailable"})
		return
	}
	keys, err := backend.ListKeys(c.Request.Context())
	if err != nil {
		log.Debugf("kv list error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"keys": keys})
}
