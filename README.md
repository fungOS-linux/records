# fungOS records

**Local-first PIM records for fungOS.** The first slice implements contacts,
namespaced books, a signed-by-transport op log, vCard import/export, and
device-to-device sync.

Status: **first working slice, 2026-11-04.** This is not the final sharing or
encryption model.

## What works

- SQLite materialized view over a per-device append-only op log
- namespaced books, e.g. `personal/avery/contacts`
- built-in schema `fungos.contacts/contact@1`
- macOS Contacts vCard import
- vCard export
- list/search/stat/vector/dump/apply
- SSH-based device sync

The current transport is SSH because it gets real contacts syncing today. The
store and op format are transport-neutral; Unibus is the next adapter.

## Import the macOS address book

```sh
swift scripts/export-macos-contacts.swift > /tmp/contacts.vcf

records --root ~/.local/share/fungos/records init
records --root ~/.local/share/fungos/records import-vcf \
  --book personal/avery/contacts \
  /tmp/contacts.vcf
```

## Inspect

```sh
records --root ~/.local/share/fungos/records list \
  --book personal/avery/contacts --limit 20

records --root ~/.local/share/fungos/records list \
  --book personal/avery/contacts --search acme --json
```

## Sync

On the remote node, install `records` and initialise the same root:

```sh
records --root ~/.local/share/fungos/records init
```

From the source device:

```sh
records --root ~/.local/share/fungos/records sync \
  --book personal/avery/contacts \
  --ssh studio
```

`sync` exchanges vectors first, then pulls and pushes the missing signed ops.
It is idempotent: duplicate `op_id`s are ignored.

## Data layout

```text
~/.local/share/fungos/records/
  records.sqlite3
  device_id
  keys/              reserved for book-key wrapping
```

The SQLite database is a local index and projection. The durable sync data is
the op log in `ops`, scoped by `book_id`.

## Book and schema namespacing

```text
book:    personal/avery/contacts
schema:  fungos.contacts/contact
version: 1
```

A record has exactly one home book. Cross-book links and team sharing are the
next design slice; this implementation does not yet expose them.

## Roadmap

1. Unibus transport adapter (replace SSH).
2. Encrypted book keys and device wrapping.
3. `recordsd` native service with Unibus provider/consumer contracts.
4. Team/company books and explicit share projections.
5. Calendar adapter with time zones, recurrence, and alarms.
6. CardDAV/CalDAV front doors.
7. PostgreSQL 17 backend for always-on nodes.

## License

MIT OR Apache-2.0.
