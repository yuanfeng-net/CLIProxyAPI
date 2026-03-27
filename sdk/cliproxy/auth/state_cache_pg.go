package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// PgStateBackend persists runtime cooldown/quota state in PostgreSQL.
type PgStateBackend struct {
	db    *sql.DB
	table string
}

// NewPgStateBackend creates a PostgreSQL-backed state persistence backend.
// The table is created automatically if it does not exist.
func NewPgStateBackend(db *sql.DB, schema string) (*PgStateBackend, error) {
	if db == nil {
		return nil, fmt.Errorf("pg state backend: db is nil")
	}
	table := pgQuote("runtime_state")
	if s := strings.TrimSpace(schema); s != "" {
		table = pgQuote(s) + "." + pgQuote("runtime_state")
	}
	b := &PgStateBackend{db: db, table: table}
	if err := b.ensureTable(context.Background()); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *PgStateBackend) ensureTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			auth_id   TEXT PRIMARY KEY,
			state     JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, b.table)
	_, err := b.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("pg state backend: create table: %w", err)
	}
	return nil
}

// LoadAll reads all runtime state records from PostgreSQL.
func (b *PgStateBackend) LoadAll(ctx context.Context) (map[string]*cachedAuthState, error) {
	query := fmt.Sprintf("SELECT auth_id, state FROM %s", b.table)
	rows, err := b.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("pg state backend: load: %w", err)
	}
	defer rows.Close()

	states := make(map[string]*cachedAuthState)
	for rows.Next() {
		var id string
		var data []byte
		if err = rows.Scan(&id, &data); err != nil {
			return nil, fmt.Errorf("pg state backend: scan: %w", err)
		}
		var state cachedAuthState
		if err = json.Unmarshal(data, &state); err != nil {
			continue
		}
		states[id] = &state
	}
	return states, rows.Err()
}

// SaveAll replaces all runtime state records in PostgreSQL using a single transaction.
func (b *PgStateBackend) SaveAll(ctx context.Context, states map[string]*cachedAuthState) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pg state backend: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Truncate and re-insert is simpler and fine for the expected scale (~hundreds of rows).
	if _, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", b.table)); err != nil {
		return fmt.Errorf("pg state backend: truncate: %w", err)
	}

	if len(states) == 0 {
		return tx.Commit()
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (auth_id, state, updated_at) VALUES ($1, $2, NOW())", b.table))
	if err != nil {
		return fmt.Errorf("pg state backend: prepare: %w", err)
	}
	defer stmt.Close()

	for id, state := range states {
		data, errMarshal := json.Marshal(state)
		if errMarshal != nil {
			continue
		}
		if _, err = stmt.ExecContext(ctx, id, data); err != nil {
			return fmt.Errorf("pg state backend: insert %s: %w", id, err)
		}
	}
	return tx.Commit()
}

func pgQuote(identifier string) string {
	return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
}
