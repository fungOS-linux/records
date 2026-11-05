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
- iCalendar import/export for events, with raw VEVENT/VTIMEZONE preservation
- RSS/Atom ingest into searchable records from a host-side feed list
- list/search/stat/vector/dump/apply
- generic schema-tagged entries plus the built-in contacts schema
- materialized changes and revision/origin merge
- Unibus device sync with a long-running provider
- SSH sync as a bootstrap/fallback transport

## Import the macOS address book

```sh
swift scripts/export-macos-contacts.swift > /tmp/contacts.vcf

records --root ~/.local/share/fungos/records init
records --root ~/.local/share/fungos/records import-vcf \
  --book personal/avery/contacts \
  /tmp/contacts.vcf
```

## Import a calendar

```sh
records --root ~/.local/share/fungos/records import-ics \
  --book personal/avery/calendar \
  /tmp/calendar.ics

records --root ~/.local/share/fungos/records list \
  --book personal/avery/calendar
```

`import-ics` stores events under `fungos.calendar/event@1`. The first slice
keeps the raw VEVENT, referenced VTIMEZONE blocks, timezone id, all-day flag,
organizer, attendees, categories, recurrence rule, and basic timestamps. It
does not yet expand recurrence or resolve timezone transitions for display.
Export round-trips the stored raw event components:

```sh
records --root ~/.local/share/fungos/records export-ics \
  --book personal/avery/calendar --output /tmp/calendar-out.ics
```

## RSS and Atom

Fetch one feed:

```sh
records --root ~/.local/share/fungos/records import-rss \
  --book personal/avery/feeds \
  --feed https://blog.rust-lang.org/feed.xml
```

Or keep the list in a dotfile and sync all of them:

```sh
records --root ~/.local/share/fungos/records sync-feeds \
  --config ~/.config/fungos/feeds.toml
```

```toml
default_book = "personal/avery/feeds"

[[feeds]]
url = "https://blog.rust-lang.org/feed.xml"
title = "Rust Blog"
limit = 100
```

Items use schema `fungos.feeds/item@1` with title, link, summary, content,
authors, categories, published/updated timestamps, feed title, feed URL, and a
stable UID. Re-importing updates the same record. Because feed items are normal
records, they sync through the existing op log and are visible to
`soil:records` and `soil:search`.

Fetching is deliberately host-side. A Spore has no ambient network capability;
a reader Spore should import `soil:records` for items and `soil:appdata` for its
private display settings. The feed list above is host config, not AppData,
because the host performs the fetch. See [`examples/feeds.toml`](examples/feeds.toml).

## Inspect

```sh
records --root ~/.local/share/fungos/records list \
  --book personal/avery/contacts --limit 20

records --root ~/.local/share/fungos/records list \
  --book personal/avery/contacts --search acme --json
```

## Unibus sync

Run a provider on an always-on node:

```sh
records --root ~/.local/share/fungos/records serve \
  --config ~/.config/unibus/records-studio.json
```

From the device:

```sh
records --root ~/.local/share/fungos/records unibus-sync \
  --book personal/avery/contacts \
  --config ~/.config/unibus/records-macbook.json \
  --peer records.studio
```

Protocol kinds:

```text
records.sync.pull.v1   consumer -> provider
records.sync.ops.v1    provider -> consumer
records.sync.push.v1   consumer -> provider
records.sync.ack.v1    provider -> consumer
records.sync.error.v1  provider -> consumer
```

The provider exchanges vectors, returns ops missing from the caller, and accepts
ops the caller has that the provider lacks. `op_id` makes re-delivery
idempotent.

### SSH fallback

```sh
records --root ~/.local/share/fungos/records sync \
  --book personal/avery/contacts \
  --ssh studio
```

Both transports use the same store and op format.

## Data layout

```text
~/.local/share/fungos/records/
  records.sqlite3
  device_id
  keys/              reserved for book-key wrapping
```

The SQLite database is a local index and projection. The durable sync data is
the op log in `ops`, scoped by `book_id`.

## Library and Spore host

The store is also a library:

```rust
use fungos_records::{Store, RecordEntry};

let mut store = Store::open(root, &root.join("device_id"))?;
store.put_record(book, id, "fungos.mail/message@1", 1, fields)?;

store.apply_entry(book, RecordEntry {
    entity_id: id,
    schema_id: "fungos.mail/message@1",
    schema_version: 1,
    fields,
    deleted: false,
    revision: remote_hlc,
    origin: remote_device,
})?;
```

`Store::scan_records(book, cursor, limit)` pages live records without loading
the whole book. `Store::scan_changes(book, cursor, limit)` returns a materialized
change feed including tombstones. `crates/soil-records-host` in Soil exposes this through
`soil:records`; `crates/soil-sdk-records` is the guest side.

## Shared peer session

The records sync protocol is transport-neutral. The same `records.sync.*` kinds
can run over the TCP hub adapter or an already-authorized Unibus peer session:

```rust
let outcome = records::sync_peer_session(
    &mut peer_session,
    &mut store,
    "personal/avery/contacts",
    &records::PeerEndpoint {
        installation_id: "home".into(),
        producer_id: "records.macbook".into(),
    },
).await?;
```

`serve_peer_session` handles `pull`, `ops`, `push`, `ack`, and typed errors on
the other side. The session layer owns identity, grants, expiry, encryption, and
transport choice. This is the same `unibus/peer/1` session other application
kinds use; records does not define a second Iroh or WiFi transport.

A Spore never imports this API. The Soil/records host owns the session and
exposes only `soil:records`.

## Book and schema namespacing

```text
book:    personal/avery/contacts
schema:  fungos.contacts/contact
version: 1
```

A record has exactly one home book. Cross-book links and team sharing are the
next design slice; this implementation does not yet expose them.

## Roadmap

1. Encrypted book keys and device wrapping.
2. Team/company books and explicit share projections.
3. Calendar adapter with time zones, recurrence, and alarms.
4. CardDAV/CalDAV front doors.
5. PostgreSQL 17 backend for always-on nodes.
6. Records schema packs and Lua policy.

The current Unibus frames are authenticated by producer tokens but are not
end-to-end encrypted. Do not treat this slice as the final privacy boundary.

## License

MIT OR Apache-2.0.
