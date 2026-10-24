# 4. Initial snapshot through an exported slot snapshot

## Context

A new pipeline should deliver the rows that already exist, not only changes made after
it started. The copy and the stream have to meet exactly: if the copy is taken after
the slot was created, rows changed in between are delivered twice in different states;
if it is taken before, changes made in between are lost.

## Decision

Create the replication slot with `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput
EXPORT_SNAPSHOT`. PostgreSQL returns the slot's consistent point and the name of a
database snapshot that matches it. Tables are read in a `REPEATABLE READ` transaction
that imports this snapshot (`SET TRANSACTION SNAPSHOT`), then streaming starts from the
consistent point.

- Rows are sent as `r` events with the consistent point as their LSN.
- Values are selected as `::text` and decoded by the same code as streamed values, so a
  row looks the same whether it came from the snapshot or from the WAL.
- The connection that created the slot stays open and idle during the copy; the exported
  snapshot dies with it.
- After the copy is delivered, the consistent point becomes the checkpoint and the
  pipeline is marked `snapshot_completed`.
- An interrupted snapshot is not resumed. The next attempt drops the slot and starts
  over, which redelivers rows but never loses any (at-least-once, as everywhere else).

## Consequences

- The whole copy runs in one long transaction. On a busy source this holds back vacuum
  for the duration of the snapshot.
- Restarting a large snapshot from scratch can be slow. Chunked, resumable snapshots
  (watermark-based, like Debezium's incremental snapshots) are possible later.
- Pipelines can opt out with `initial_snapshot: false`.
