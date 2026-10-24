# 3. Desired state and reconciliation

## Context

Pipelines are managed through an HTTP API, but streams run in a separate worker
process. The API must not call the worker directly: the worker may be restarting,
there may be several of them later, and a request should not hang while a replication
slot is being dropped.

## Decision

- `Pipeline.status` is the **desired** state set by an operator: `active`, `paused`,
  `failed` or `deleting`. Allowed transitions are enforced by the aggregate.
- What actually happens is **runtime** state (`starting`, `streaming`, `retrying`, slot
  lag, last error), reported by the worker into a separate table.
- The worker's `Supervisor` periodically compares the two and acts: starts streams for
  active pipelines, stops paused ones, drops the slot and publication of deleted ones
  and only then removes the row.
- Errors that retrying cannot fix (`FatalPipelineError`: missing table, unknown secret)
  move the pipeline to `failed`. Everything else is retried with exponential backoff.

## Consequences

- API calls are fast and never touch the source database.
- Changes take effect within one poll interval (2 seconds by default).
- A single worker is assumed for now; with several workers each would try to start every
  pipeline, and only the slot's "one active consumer" rule would prevent duplicates.
  Locking is planned for stage 6.
