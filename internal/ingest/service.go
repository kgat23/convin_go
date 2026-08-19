// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingTimeout bounds how long a background recording job may run,
// independent of any request.
const recordingTimeout = 30 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// IngestEvent does the insert/upsert/increment as one transaction guarded
// by a unique constraint on event_id, so a redelivery is a no-op rather than
// a double-count. We used to check EventExists before inserting, but that
// was two separate round trips -- concurrent redeliveries could both pass
// the check before either had written anything.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.startRecordingProcessing(rec)
	}

	return nil
}

// startRecordingProcessing runs processRecording in the background with its
// own context, not the caller's -- net/http cancels the request context as
// soon as the handler returns, which is right away here since this is
// fire-and-forget. That used to be why MarkRecordingProcessed failed
// (context.Canceled) and why the error never showed up anywhere: it was
// just dropped.
func (s *Service) startRecordingProcessing(rec store.Event) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), recordingTimeout)
		defer cancel()

		if err := s.processRecording(ctx, rec); err != nil {
			s.log.Error("process recording failed",
				"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
		}
	}()
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
