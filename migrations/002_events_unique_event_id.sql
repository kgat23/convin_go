-- event_id was only indexed, not unique, so nothing stopped two
-- redeliveries racing past the app-level dedup check and landing as
-- separate rows.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
