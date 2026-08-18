// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// execer lets the write helpers below run against either the pool directly
// or a transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// insertEventIfNew is a no-op if event_id already exists (unique
// constraint), and reports whether it actually inserted a row.
func insertEventIfNew(ctx context.Context, ex execer, e Event) (bool, error) {
	tag, err := ex.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// InsertEvent stores the raw delivery. Ingestion itself goes through
// IngestEvent below; this stays around for tests that only touch events.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	_, err := insertEventIfNew(ctx, s.pool, e)
	return err
}

func upsertCall(ctx context.Context, ex execer, e Event) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// UpsertCall creates or refreshes the call record for this event.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	return upsertCall(ctx, s.pool, e)
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

func incrementAccountStats(ctx context.Context, ex execer, accountID string, durationSec int) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// IncrementAccountStats folds one completed call into the durable aggregate.
func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	return incrementAccountStats(ctx, s.pool, accountID, durationSec)
}

// IngestEvent records the event, its call, and the account totals in one
// transaction. inserted is false for a redelivery, so the caller can skip
// firing side effects (cache update, recording processing) twice.
func (s *Store) IngestEvent(ctx context.Context, e Event) (inserted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	inserted, err = insertEventIfNew(ctx, tx, e)
	if err != nil || !inserted {
		return false, err
	}
	if err := upsertCall(ctx, tx, e); err != nil {
		return false, err
	}
	if err := incrementAccountStats(ctx, tx, e.AccountID, e.DurationSec); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}
