# telescreen

> Big Brother is watching your WAL.

Lightweight change data capture for PostgreSQL written in Python. Telescreen reads the
write-ahead log through native logical replication (`pgoutput`), turns row changes into
structured events and publishes them to Kafka or NATS JetStream.

**Status:** work in progress. See [roadmap](#roadmap).

## How it works

```
operator ──▶ telescreen-web (HTTP API, status pages)
                 │ desired state
                 ▼
             metadata DB: pipelines, checkpoints, runtime status
                 ▲ reconcile every 2 s
                 │
             telescreen-worker: Supervisor ─▶ PipelineRunner per pipeline

PostgreSQL ──pgoutput──▶ decoder ──▶ ChangeCapture ──▶ sink (Kafka)
    ▲                                      │
    └──── flush LSN (only after broker ack)┘
```

- Telescreen creates a **publication** for the configured tables and a **logical
  replication slot** with the built-in `pgoutput` plugin. No extensions are required.
- The `pgoutput` binary protocol is decoded by telescreen itself
  ([`pgoutput.py`](src/telescreen/adapters/postgres/pgoutput.py)).
- Delivery is **at-least-once**. A WAL position is acknowledged to PostgreSQL only after
  every event of the transactions up to it has been accepted by the broker. A crash means
  redelivery, never loss.
- Events are keyed by the primary key, so all changes of one row land in the same Kafka
  partition in commit order.

## Event format

Topic: `<topic_prefix>.<schema>.<table>`. Key: the primary key as JSON. Value:

```json
{
  "op": "u",
  "source": {
    "pipeline": "shop",
    "schema": "public",
    "table": "orders",
    "lsn": "0/1A2B3C4",
    "xid": 754,
    "committed_at": "2026-10-24T12:00:00.123456+00:00"
  },
  "key": {"id": 1},
  "before": {"id": 1, "status": "new", "total": "19.84"},
  "after": {"id": 1, "status": "paid", "total": "19.84"}
}
```

| `op` | Meaning  | `before`                         | `after` |
|------|----------|----------------------------------|---------|
| `c`  | insert   | `null`                           | row     |
| `u`  | update   | row, with `REPLICA IDENTITY FULL` | row     |
| `d`  | delete   | row, with `REPLICA IDENTITY FULL` | `null`  |
| `t`  | truncate | `null`                           | `null`  |
| `r`  | snapshot | `null`                           | row     |

Values keep their JSON type where it is unambiguous (booleans, integers, floats, `json`
and `jsonb`). Everything else, including `numeric` and timestamps, is passed as the string
PostgreSQL produced, so no precision is lost.

## Running

```bash
docker compose up -d
cp .env.example .env
uv run alembic upgrade head
uv run telescreen-web       # http://localhost:8000, API docs at /docs
uv run telescreen-worker    # in another terminal
```

Create a pipeline on the status page, through the API, or import a YAML file:

```bash
curl -X POST localhost:8000/api/pipelines -H 'Content-Type: application/json'   -d '{"name": "shop", "dsn": "env:SHOP_DSN", "tables": ["public.customers", "public.orders"]}'

uv run telescreen-import pipelines.example.yaml
```

Then change something in the demo database and watch the topics in Kafka UI:

```bash
docker compose exec postgres psql -U postgres -d shop -c "UPDATE orders SET status = 'shipped' WHERE id = 1"
```

## Initial snapshot

By default a new pipeline first copies every existing row (`"op": "r"`) and then streams
changes. The copy is read inside the snapshot that PostgreSQL exports when the
replication slot is created, so it lines up exactly with the start of the stream:
nothing is lost and nothing is read twice ([ADR 4](docs/adr/0004-initial-snapshot.md)).

Snapshot events carry the slot's consistent point as `lsn` and have no `xid` or
`committed_at`. To stream changes only, create the pipeline with
`"initial_snapshot": false`.

## Managing pipelines

| Method   | Path                           | Effect                                           |
|----------|--------------------------------|--------------------------------------------------|
| `GET`    | `/api/pipelines`               | All pipelines with runtime status and checkpoint |
| `POST`   | `/api/pipelines`               | Create a pipeline                                |
| `GET`    | `/api/pipelines/{name}`        | One pipeline                                     |
| `POST`   | `/api/pipelines/{name}/pause`  | Stop streaming, keep the slot                    |
| `POST`   | `/api/pipelines/{name}/resume` | Continue from the last acknowledged position     |
| `POST`   | `/api/pipelines/{name}/retry`  | Restart a failed pipeline                        |
| `DELETE` | `/api/pipelines/{name}`        | Drop the slot and publication, then remove       |

The API only records the desired state. The worker picks it up within a couple of
seconds ([ADR 3](docs/adr/0003-desired-state-and-reconciliation.md)). Errors that
retrying cannot fix, such as a missing table or an unset secret, move the pipeline to
`failed`; everything else is retried with exponential backoff.

A paused pipeline keeps its replication slot, so PostgreSQL retains WAL until it is
resumed. The status page shows how much.

## Things to know

- **`before` needs `REPLICA IDENTITY FULL`.** With the default identity PostgreSQL sends
  only the key of a deleted row and no old image for updates.
- **Unchanged TOAST values.** Large values that did not change in an update are not sent
  by PostgreSQL. Telescreen restores them from the old row image when there is one, and
  lists them in `"unchanged"` otherwise.
- **An abandoned slot keeps WAL forever.** Drop slots of pipelines you remove and set
  `max_slot_wal_keep_size` on the server.

## Roadmap

- [x] Stage 0 — project skeleton, tooling, local infrastructure
- [x] Stage 1 — `pgoutput` decoder, streaming to Kafka, at-least-once delivery
- [x] Stage 2 — pipeline metadata in PostgreSQL, HTTP API, status page
- [x] Stage 3 — initial snapshot with a gap-free handoff to streaming
- [ ] Stage 4 — column filters, PII masking, topic routing, schema tracking
- [ ] Stage 5 — NATS JetStream sink
- [ ] Stage 6 — metrics and integration tests

## Development

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker.

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy
```

Integration tests run against the compose PostgreSQL when `TELESCREEN_TEST_SOURCE_DSN`
and `TELESCREEN_TEST_DATABASE_URL` are set (see `.env.example`) and are skipped otherwise.

Architecture decisions are recorded in [docs/adr](docs/adr).

| Service    | Address                                     |
|------------|---------------------------------------------|
| PostgreSQL | `localhost:5433` (user/password `postgres`) |
| Kafka      | `localhost:9094`                            |
| Kafka UI   | http://localhost:8081                       |

The PostgreSQL container hosts `telescreen` for the service's own metadata, `shop`, a demo
source with a few tables to capture, and `telescreen_test` for integration tests.

## License

MIT
