# SOLUTION.md

## What was broken, and why

**Duplicate records / inflated call counts.** `events.event_id` was only
indexed, not unique, and `Ingest` checked `EventExists` before a separate
`InsertEvent` call. That's a check-then-act race: two redeliveries arriving
close together could both pass the "not seen yet" check before either had
written a row, so both landed as separate `events`/`calls` rows and
`account_stats` got incremented twice for one call.

**Recordings never marked processed, nothing in the logs.** `processRecording`
ran in a goroutine that reused the incoming HTTP request's `context.Context`.
`net/http` cancels that context the instant the handler returns, which
happens almost immediately since the goroutine is fire-and-forget. The
background write to `calls.recording_processed` was therefore running against
an already-cancelled context nearly every time, failing with
`context.Canceled` — and that error was caught and discarded
(`// TODO: handle`), so nothing was ever logged.

**Work lost on deploy.** `srv.Shutdown` in `main.go` only waits for HTTP
handler functions to return, not for goroutines a handler spawned and left
running. `Ingest` returns as soon as it kicks off recording processing, so
`Shutdown` could see zero active handlers and return almost instantly while a
`processRecording` goroutine was still mid-flight. The deferred
`store.Close()`/`redis.Close()` calls then ran immediately after, and the job
was simply abandoned.

## Dedup strategy

I put a `UNIQUE` constraint on `events.event_id` and made `IngestEvent` do the
insert, call upsert, and stats increment as one Postgres transaction, using
`INSERT ... ON CONFLICT (event_id) DO NOTHING` to detect a redelivery in a
single round trip. I considered a Redis `SETNX` lock in front of Postgres
instead, but Redis here has no persistence guarantee configured — it's a
cache, and a dropped idempotency key would let a duplicate back in. Postgres
already has to be the source of truth for the event, the call, and the stats;
adding a second system just to decide "have I seen this" would be one more
thing that can disagree with the database. The transaction also means a
redelivery can never leave the event stored without its call/stats
counterpart, which check-then-act plus separate writes could.

## At 10,000 webhooks/sec

The Postgres round trip on every request, including duplicates, would become
the bottleneck first. I'd add a Redis-backed fast path — check a
`SETNX event:{id}` before touching Postgres at all, so obvious repeats never
reach the database; Postgres's unique constraint stays as the real
correctness guarantee behind it. I'd also stop spawning an unbounded goroutine
per recording and put a bounded worker pool (or a real queue) in front of
that work, since one goroutine per request doesn't degrade gracefully under
load. `account_stats` would move from a per-event `UPDATE` to a batched or
periodically-flushed increment rather than one write per call. And I'd
partition `events` by time, since it's an append-mostly table that will
otherwise just grow forever under an index that has to stay fast.
