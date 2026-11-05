# Records core

The first slice deliberately keeps the layers separate:

```text
SQLite projection  <- fast local reads and search
op log             <- durable, device-scoped, idempotent sync
transport          <- SSH today, Unibus next
schema             <- namespaced record meaning
book               <- ownership and future encryption boundary
```

## Op

```json
{
  "op_id": "op-...",
  "book_id": "personal/avery/contacts",
  "device_id": "uuid",
  "seq": 1,
  "hlc": 1793838952614,
  "kind": "contact.put",
  "entity_id": "contact-...",
  "body": { "...": "schema payload" }
}
```

`op_id` is a BLAKE3 digest of the envelope and body. `(book_id, device_id, seq)`
is unique. Re-delivery is a no-op.

## Sync

Each side publishes a vector:

```json
{"book_id":"personal/avery/contacts","devices":{"<device>":591}}
```

The peer sends ops with `seq > vector[device]`. The receiver verifies the op id,
applies the projection, and appends the op. No global order is assumed.

## Why SQLite

SQLite is the local materialized view and index, not the sync authority. It
gives instant list/search and survives restart. PostgreSQL 17 will use the same
logical schema for always-on nodes; it will be a backend, not the sync protocol.

## What is deferred

- encryption and key wrapping
- multi-field merge/conflict retention
- team/company book grants
- share projections and tickets
- CardDAV/CalDAV
- Unibus provider/consumer transport
- calendar recurrence and alarms
