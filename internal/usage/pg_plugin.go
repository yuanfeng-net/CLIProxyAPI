package usage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// PgUsagePlugin persists usage records to PostgreSQL.
type PgUsagePlugin struct {
	db    *sql.DB
	table string
}

// NewPgUsagePlugin creates a usage plugin that writes records to PostgreSQL.
// The table is created automatically if it does not exist.
func NewPgUsagePlugin(db *sql.DB, schema string) (*PgUsagePlugin, error) {
	if db == nil {
		return nil, fmt.Errorf("pg usage plugin: db is nil")
	}
	table := pgUsageQuote("usage_log")
	if s := strings.TrimSpace(schema); s != "" {
		table = pgUsageQuote(s) + "." + pgUsageQuote("usage_log")
	}
	p := &PgUsagePlugin{db: db, table: table}
	if err := p.ensureTable(context.Background()); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *PgUsagePlugin) ensureTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id            BIGSERIAL PRIMARY KEY,
			provider      TEXT NOT NULL DEFAULT '',
			model         TEXT NOT NULL DEFAULT '',
			api_key       TEXT NOT NULL DEFAULT '',
			auth_id       TEXT NOT NULL DEFAULT '',
			auth_index    TEXT NOT NULL DEFAULT '',
			source        TEXT NOT NULL DEFAULT '',
			requested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			latency_ms    BIGINT NOT NULL DEFAULT 0,
			failed        BOOLEAN NOT NULL DEFAULT FALSE,
			input_tokens  BIGINT NOT NULL DEFAULT 0,
			output_tokens BIGINT NOT NULL DEFAULT 0,
			reasoning_tokens BIGINT NOT NULL DEFAULT 0,
			cached_tokens BIGINT NOT NULL DEFAULT 0,
			total_tokens  BIGINT NOT NULL DEFAULT 0
		)
	`, p.table)
	if _, err := p.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("pg usage plugin: create table: %w", err)
	}
	// Index for common queries.
	idxName := "idx_usage_log_requested_at"
	idxQuery := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS %s ON %s (requested_at DESC)",
		pgUsageQuote(idxName), p.table)
	if _, err := p.db.ExecContext(ctx, idxQuery); err != nil {
		log.Debugf("pg usage plugin: create index: %v", err)
	}
	return nil
}

// HandleUsage implements coreusage.Plugin — inserts a usage record into PostgreSQL.
func (p *PgUsagePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil || p.db == nil {
		return
	}
	ts := record.RequestedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	latencyMs := int64(0)
	if record.Latency > 0 {
		latencyMs = record.Latency.Milliseconds()
	}
	total := record.Detail.TotalTokens
	if total == 0 {
		total = record.Detail.InputTokens + record.Detail.OutputTokens + record.Detail.ReasoningTokens
	}

	query := fmt.Sprintf(`
		INSERT INTO %s
			(provider, model, api_key, auth_id, auth_index, source,
			 requested_at, latency_ms, failed,
			 input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`, p.table)

	if _, err := p.db.ExecContext(ctx, query,
		record.Provider,
		record.Model,
		record.APIKey,
		record.AuthID,
		record.AuthIndex,
		record.Source,
		ts,
		latencyMs,
		record.Failed,
		record.Detail.InputTokens,
		record.Detail.OutputTokens,
		record.Detail.ReasoningTokens,
		record.Detail.CachedTokens,
		total,
	); err != nil {
		log.Debugf("pg usage plugin: insert error: %v", err)
	}
}

func pgUsageQuote(identifier string) string {
	return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
}

// RestoreToStats loads historical usage records from PostgreSQL into the in-memory statistics store.
// Call this once at startup before serving requests.
func (p *PgUsagePlugin) RestoreToStats(stats *RequestStatistics) error {
	if p == nil || p.db == nil || stats == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
		SELECT api_key, model, requested_at, latency_ms, source, auth_index, failed,
		       input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
		FROM %s ORDER BY requested_at ASC
	`, p.table)

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("pg usage restore: query: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			apiKey           string
			model            string
			requestedAt      time.Time
			latencyMs        int64
			source           string
			authIndex        string
			failed           bool
			inputTokens      int64
			outputTokens     int64
			reasoningTokens  int64
			cachedTokens     int64
			totalTokens      int64
		)
		if err = rows.Scan(&apiKey, &model, &requestedAt, &latencyMs, &source, &authIndex, &failed,
			&inputTokens, &outputTokens, &reasoningTokens, &cachedTokens, &totalTokens); err != nil {
			continue
		}
		stats.Record(ctx, coreusage.Record{
			APIKey:      apiKey,
			Model:       model,
			RequestedAt: requestedAt,
			Latency:     time.Duration(latencyMs) * time.Millisecond,
			Source:      source,
			AuthIndex:   authIndex,
			Failed:      failed,
			Detail: coreusage.Detail{
				InputTokens:     inputTokens,
				OutputTokens:    outputTokens,
				ReasoningTokens: reasoningTokens,
				CachedTokens:    cachedTokens,
				TotalTokens:     totalTokens,
			},
		})
		count++
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("pg usage restore: iterate: %w", err)
	}
	log.Infof("pg usage restore: loaded %d historical records into memory", count)
	return nil
}
