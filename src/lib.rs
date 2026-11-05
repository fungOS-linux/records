//! First local-first records slice for fungOS PIM.
//!
//! This implementation is intentionally narrow: a SQLite materialized view over
//! a signed-by-transport (SSH for now) per-device op log, plus vCard import and
//! export. The sync transport is a separate command so Unibus can replace it
//! without changing the store.

use std::{
    collections::BTreeMap,
    fs,
    io::{self, BufRead, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow, bail};
use clap::{Parser, Subcommand};
use ical_vcard::{Contentline, Parser as VCardParser};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value as SqlValue};
use serde::{Deserialize, Serialize};
use unibus::client::{Client, ClientConfig};
use unibus::{Envelope, Message, PeerSession};
use uuid::Uuid;

pub const CONTACT_SCHEMA: &str = "fungos.contacts/contact";
pub const CALENDAR_SCHEMA: &str = "fungos.calendar/event";
pub const FEED_ITEM_SCHEMA: &str = "fungos.feeds/item";
const CONTACT_VERSION: i64 = 1;
const CALENDAR_VERSION: i64 = 1;
const FEED_ITEM_VERSION: i64 = 1;
const MAX_FEED_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_FEED_LIMIT: usize = 250;
const FEED_TIMEOUT: Duration = Duration::from_secs(20);
const PULL_KIND: &str = "records.sync.pull.v1";
const OPS_KIND: &str = "records.sync.ops.v1";
const PUSH_KIND: &str = "records.sync.push.v1";
const ACK_KIND: &str = "records.sync.ack.v1";
const ERROR_KIND: &str = "records.sync.error.v1";
const SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser)]
#[command(version, about = "Local-first fungOS records")]
struct Cli {
    #[arg(long, global = true, default_value = "~/.local/share/fungos/records")]
    root: String,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Init,
    ImportVcf {
        #[arg(long)]
        book: String,
        file: PathBuf,
    },
    ExportVcf {
        #[arg(long)]
        book: String,
        #[arg(long)]
        output: PathBuf,
    },
    ImportIcs {
        #[arg(long)]
        book: String,
        file: PathBuf,
    },
    ExportIcs {
        #[arg(long)]
        book: String,
        #[arg(long)]
        output: PathBuf,
    },
    ImportRss {
        #[arg(long)]
        book: String,
        /// HTTP(S) URL or a local file path.
        #[arg(long)]
        feed: String,
        #[arg(long, default_value_t = DEFAULT_FEED_LIMIT)]
        limit: usize,
    },
    SyncFeeds {
        #[arg(long, default_value = "~/.config/fungos/feeds.toml")]
        config: PathBuf,
    },
    List {
        #[arg(long)]
        book: String,
        #[arg(long)]
        search: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    Stat {
        #[arg(long)]
        book: String,
        id: String,
    },
    Vector {
        #[arg(long)]
        book: String,
    },
    Dump {
        #[arg(long)]
        book: String,
        #[arg(long)]
        since: Option<String>,
    },
    Apply {
        #[arg(long)]
        book: String,
    },
    Sync {
        #[arg(long)]
        book: String,
        #[arg(long)]
        ssh: String,
        #[arg(long, default_value = "~/.local/bin/records")]
        remote_binary: String,
        #[arg(long, default_value = "~/.local/share/fungos/records")]
        remote_root: String,
    },
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    UnibusSync {
        #[arg(long)]
        book: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        peer: String,
    },
}

pub fn run_cli() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let root = expand_home(&cli.root);
    let device_path = root.join("device_id");
    let mut store = Store::open(&root, &device_path)?;
    match cli.command {
        Cmd::Init => {
            println!("{}", store.root.display());
        }
        Cmd::ImportVcf { book, file } => {
            let count = store.import_vcf(&book, &file)?;
            println!("imported {count} contacts into {book}");
        }
        Cmd::ExportVcf { book, output } => {
            let count = store.export_vcf(&book, &output)?;
            println!("exported {count} contacts to {}", output.display());
        }
        Cmd::ImportIcs { book, file } => {
            let count = store.import_ics(&book, &file)?;
            println!("imported {count} events into {book}");
        }
        Cmd::ExportIcs { book, output } => {
            let count = store.export_ics(&book, &output)?;
            println!("exported {count} events to {}", output.display());
        }
        Cmd::ImportRss { book, feed, limit } => {
            let count = store.import_rss(&book, &feed, limit)?;
            println!("imported {count} feed items into {book}");
        }
        Cmd::SyncFeeds { config } => {
            let config = expand_home_path(&config);
            let count = store.sync_feeds(&config)?;
            println!("imported {count} feed items from {}", config.display());
        }
        Cmd::List {
            book,
            search,
            limit,
            json,
        } => {
            let records = store.list(&book, search.as_deref(), limit)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&records)?);
            } else {
                for record in records {
                    match record.schema_id.as_str() {
                        CONTACT_SCHEMA => {
                            let contact: Contact = serde_json::from_value(record.fields)?;
                            println!(
                                "{}\t{}\t{}",
                                record.entity_id,
                                contact.display_name,
                                contact
                                    .emails
                                    .first()
                                    .map(|e| e.value.as_str())
                                    .unwrap_or("")
                            );
                        }
                        CALENDAR_SCHEMA => {
                            let event: CalendarEvent = serde_json::from_value(record.fields)?;
                            println!(
                                "{}\t{}\t{}\t{}",
                                record.entity_id, event.start, event.end, event.summary
                            );
                        }
                        FEED_ITEM_SCHEMA => println!(
                            "{}\t{}\t{}\t{}",
                            record.entity_id,
                            record.fields["published"].as_str().unwrap_or(""),
                            record.fields["title"].as_str().unwrap_or(""),
                            record.fields["link"].as_str().unwrap_or("")
                        ),
                        _ => println!(
                            "{}\t{}\t{}",
                            record.entity_id,
                            record.schema_id,
                            serde_json::to_string(&record.fields)?
                        ),
                    }
                }
            }
        }
        Cmd::Stat { book, id } => {
            let record = store.get(&book, &id)?;
            println!("{}", serde_json::to_string_pretty(&record)?);
        }
        Cmd::Vector { book } => {
            println!("{}", serde_json::to_string(&store.vector(&book)?)?);
        }
        Cmd::Dump { book, since } => {
            let since: Option<Vector> = since
                .as_deref()
                .map(serde_json::from_str::<Option<Vector>>)
                .transpose()
                .context("parse --since vector JSON")?
                .flatten();
            for op in store.dump(&book, since.as_ref())? {
                println!("{}", serde_json::to_string(&op)?);
            }
        }
        Cmd::Apply { book } => {
            let stdin = io::stdin();
            let mut count = 0usize;
            for line in stdin.lock().lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let op: Op = serde_json::from_str(&line).context("parse op")?;
                if op.book_id != book {
                    bail!("op belongs to {}, not {book}", op.book_id);
                }
                store.apply_op(&op)?;
                count += 1;
            }
            println!("applied {count} ops");
        }
        Cmd::Sync {
            book,
            ssh,
            remote_binary,
            remote_root,
        } => {
            let remote = Remote::new(&ssh, &remote_binary, &remote_root);
            let remote_vector = remote.vector(&book)?;
            let local_vector = store.vector(&book)?;
            let pull = remote.dump(&book, Some(&local_vector))?;
            let mut pulled = 0usize;
            for op in pull {
                store.apply_op(&op)?;
                pulled += 1;
            }
            let push = store.dump(&book, Some(&remote_vector))?;
            let pushed = push.len();
            remote.apply(&book, &push)?;
            println!(
                "sync {book}: pulled {pulled}, pushed {pushed} (local {} remote {})",
                vector_string(&store.vector(&book)?),
                vector_string(&remote_vector)
            );
        }
        Cmd::Serve { config } => {
            serve_unibus(&mut store, &config)?;
        }
        Cmd::UnibusSync { book, config, peer } => {
            sync_unibus(&mut store, &book, &config, &peer)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Vector {
    pub book_id: String,
    pub devices: BTreeMap<String, u64>,
}

fn vector_string(vector: &Vector) -> String {
    vector
        .devices
        .iter()
        .map(|(device, seq)| format!("{device}={seq}"))
        .collect::<Vec<_>>()
        .join(",")
}

struct Remote {
    host: String,
    binary: String,
    root: String,
}

impl Remote {
    fn new(host: &str, binary: &str, root: &str) -> Self {
        Self {
            host: host.into(),
            binary: binary.into(),
            root: root.into(),
        }
    }

    fn command(&self, args: &[&str]) -> anyhow::Result<std::process::Child> {
        let mut command = Command::new("ssh");
        command.arg("-o").arg("BatchMode=yes");
        command.arg(&self.host);
        command.arg(self.remote_line(args));
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("start ssh {}", self.host))
    }

    fn remote_line(&self, args: &[&str]) -> String {
        let mut line = format!("{} --root {}", self.binary, self.root);
        for arg in args {
            line.push(' ');
            line.push_str(&shell_quote(arg));
        }
        line
    }

    pub fn vector(&self, book: &str) -> anyhow::Result<Vector> {
        let output = Command::new("ssh")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg(&self.host)
            .arg(self.remote_line(&["vector", "--book", book]))
            .output()
            .with_context(|| format!("ssh {} vector", self.host))?;
        if !output.status.success() {
            bail!("remote vector failed");
        }
        serde_json::from_slice(&output.stdout).context("parse remote vector")
    }

    pub fn dump(&self, book: &str, since: Option<&Vector>) -> anyhow::Result<Vec<Op>> {
        let since = since
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_else(|| "null".into());
        let output = Command::new("ssh")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg(&self.host)
            .arg(self.remote_line(&["dump", "--book", book, "--since", &since]))
            .output()
            .with_context(|| format!("ssh {} dump", self.host))?;
        if !output.status.success() {
            bail!("remote dump failed");
        }
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).context("parse remote op"))
            .collect()
    }

    fn apply(&self, book: &str, ops: &[Op]) -> anyhow::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut child = self.command(&["apply", "--book", book])?;
        {
            let stdin = child.stdin.as_mut().ok_or_else(|| anyhow!("no stdin"))?;
            for op in ops {
                serde_json::to_writer(&mut *stdin, op)?;
                stdin.write_all(b"\n")?;
            }
        }
        let status = child.wait()?;
        if !status.success() {
            bail!("remote apply failed");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Op {
    pub op_id: String,
    pub book_id: String,
    pub device_id: String,
    pub seq: u64,
    pub hlc: u64,
    pub kind: String,
    pub entity_id: String,
    pub body: serde_json::Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub book_id: String,
    pub entity_id: String,
    pub schema_id: String,
    pub schema_version: i64,
    pub revision: String,
    pub fields: serde_json::Value,
    pub updated_hlc: u64,
    pub deleted: bool,
    pub origin: String,
}

#[derive(Clone, Serialize, Deserialize)]
/// One page of a cross-namespace query, newest first.
pub struct QueryPage {
    pub records: Vec<Record>,
    pub next: Option<String>,
}

/// Cheap watermark used by host-side live refresh.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub revision: u64,
    pub records: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Contact {
    pub display_name: String,
    pub given_name: String,
    pub family_name: String,
    pub organization: String,
    pub title: String,
    pub emails: Vec<LabeledValue>,
    pub phones: Vec<LabeledValue>,
    pub addresses: Vec<LabeledValue>,
    pub urls: Vec<LabeledValue>,
    pub note: String,
    pub birthday: String,
    pub uid: String,
    pub created: String,
    pub last_modified: String,
    pub raw_vcard: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LabeledValue {
    label: String,
    value: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CalendarEvent {
    pub summary: String,
    pub description: String,
    pub location: String,
    pub status: String,
    pub uid: String,
    pub start: String,
    pub end: String,
    pub duration: String,
    pub all_day: bool,
    pub timezone: String,
    pub rrule: String,
    pub categories: Vec<String>,
    pub organizer: CalendarAttendee,
    pub attendees: Vec<CalendarAttendee>,
    pub url: String,
    pub created: String,
    pub last_modified: String,
    pub sequence: String,
    pub raw_ics: String,
    pub raw_timezones: Vec<String>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CalendarAttendee {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
struct FeedsConfig {
    #[serde(default)]
    default_book: Option<String>,
    feeds: Vec<FeedConfig>,
}

#[derive(Debug, Deserialize)]
struct FeedConfig {
    url: String,
    #[serde(default)]
    book: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default = "default_feed_limit")]
    limit: usize,
}

fn default_feed_limit() -> usize {
    DEFAULT_FEED_LIMIT
}

/// One materialized entry accepted from a peer.
pub struct RecordEntry<'a> {
    pub entity_id: &'a str,
    pub schema_id: &'a str,
    pub schema_version: i64,
    pub fields: serde_json::Value,
    pub deleted: bool,
    pub revision: u64,
    pub origin: &'a str,
}

pub struct Store {
    root: PathBuf,
    pub db: Connection,
    pub device_id: String,
}

impl Store {
    pub fn open(root: &Path, device_path: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(root)?;
        fs::create_dir_all(root.join("keys"))?;
        let db = Connection::open(root.join("records.sqlite3"))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=5000;
             CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS ops(
               op_id TEXT PRIMARY KEY,
               book_id TEXT NOT NULL,
               device_id TEXT NOT NULL,
               seq INTEGER NOT NULL,
               hlc INTEGER NOT NULL,
               kind TEXT NOT NULL,
               entity_id TEXT NOT NULL,
               body TEXT NOT NULL,
               UNIQUE(book_id, device_id, seq)
             );
             CREATE TABLE IF NOT EXISTS records(
               book_id TEXT NOT NULL,
               entity_id TEXT NOT NULL,
               schema_id TEXT NOT NULL,
               schema_version INTEGER NOT NULL,
               revision TEXT NOT NULL,
               fields TEXT NOT NULL,
               updated_hlc INTEGER NOT NULL,
               deleted INTEGER NOT NULL DEFAULT 0,
               origin TEXT NOT NULL DEFAULT '',
               PRIMARY KEY(book_id, entity_id)
             );
             CREATE INDEX IF NOT EXISTS records_book ON records(book_id);
             CREATE TABLE IF NOT EXISTS sync_peers(
               book_id TEXT NOT NULL,
               peer_id TEXT NOT NULL,
               PRIMARY KEY(book_id, peer_id)
             );
             CREATE TABLE IF NOT EXISTS outbox(
               book_id TEXT NOT NULL,
               peer_id TEXT NOT NULL,
               op_id TEXT NOT NULL,
               enqueued_hlc INTEGER NOT NULL,
               attempts INTEGER NOT NULL DEFAULT 0,
               next_attempt_hlc INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(book_id, peer_id, op_id)
             );
             CREATE INDEX IF NOT EXISTS outbox_due ON outbox(book_id, peer_id, next_attempt_hlc);
            ",
        )?;
        let device_id = if device_path.exists() {
            fs::read_to_string(device_path)?.trim().to_string()
        } else {
            let id = Uuid::new_v4().to_string();
            fs::write(device_path, format!("{id}\n"))?;
            id
        };
        let has_deleted: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('records') WHERE name='deleted')",
            [],
            |row| row.get(0),
        )?;
        if !has_deleted {
            db.execute(
                "ALTER TABLE records ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        let has_origin: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('records') WHERE name='origin')",
            [],
            |row| row.get(0),
        )?;
        if !has_origin {
            db.execute(
                "ALTER TABLE records ADD COLUMN origin TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        let store = Self {
            root: root.to_path_buf(),
            db,
            device_id,
        };
        store.db.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('device_id',?1)",
            params![store.device_id],
        )?;
        Ok(store)
    }

    /// Configure the peers that receive local writes for one book.
    ///
    /// This is host policy, not a Spore-visible option. New local ops are
    /// inserted into a durable per-peer outbox in the same SQLite transaction.
    pub fn set_sync_peers(&self, book: &str, peers: &[String]) -> anyhow::Result<()> {
        let tx = self.db.unchecked_transaction()?;
        tx.execute("DELETE FROM sync_peers WHERE book_id=?1", params![book])?;
        for peer in peers {
            if peer.trim().is_empty() {
                continue;
            }
            tx.execute(
                "INSERT OR IGNORE INTO sync_peers(book_id,peer_id) VALUES(?1,?2)",
                params![book, peer],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn sync_peers(&self, book: &str) -> anyhow::Result<Vec<String>> {
        let mut statement = self
            .db
            .prepare("SELECT peer_id FROM sync_peers WHERE book_id=?1 ORDER BY peer_id")?;
        let rows = statement.query_map(params![book], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn pending_outbox(
        &self,
        book: &str,
        peer: &str,
        now_hlc: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<Op>> {
        let mut statement = self.db.prepare(
            "SELECT o.op_id,o.book_id,o.device_id,o.seq,o.hlc,o.kind,o.entity_id,o.body
             FROM outbox x JOIN ops o ON o.op_id=x.op_id
             WHERE x.book_id=?1 AND x.peer_id=?2 AND x.next_attempt_hlc<=?3
             ORDER BY o.hlc,o.device_id,o.seq LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![
                book,
                peer,
                now_hlc.min(i64::MAX as u64) as i64,
                limit as i64
            ],
            row_to_op,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn outbox_len(&self, book: &str, peer: &str) -> anyhow::Result<u64> {
        let count: i64 = self.db.query_row(
            "SELECT COUNT(*) FROM outbox WHERE book_id=?1 AND peer_id=?2",
            params![book, peer],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Remove delivered ops. A lost ack is repaired by the next vector exchange;
    /// applying an op twice is already idempotent.
    pub fn ack_outbox(&self, book: &str, peer: &str, ops: &[Op]) -> anyhow::Result<()> {
        let tx = self.db.unchecked_transaction()?;
        for op in ops {
            tx.execute(
                "DELETE FROM outbox WHERE book_id=?1 AND peer_id=?2 AND op_id=?3",
                params![book, peer, op.op_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Keep failed ops durable and back them off without blocking the local
    /// write path or the rest of the outbox.
    pub fn defer_outbox(
        &self,
        book: &str,
        peer: &str,
        ops: &[Op],
        next_attempt_hlc: u64,
    ) -> anyhow::Result<()> {
        let tx = self.db.unchecked_transaction()?;
        for op in ops {
            tx.execute(
                "UPDATE outbox
                 SET attempts=attempts+1, next_attempt_hlc=?4
                 WHERE book_id=?1 AND peer_id=?2 AND op_id=?3",
                params![book, peer, op.op_id, next_attempt_hlc as i64],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn next_seq(&self, book: &str) -> anyhow::Result<u64> {
        self.next_seq_for(book, &self.device_id)
    }

    fn next_seq_for(&self, book: &str, device_id: &str) -> anyhow::Result<u64> {
        let current: Option<i64> = self
            .db
            .query_row(
                "SELECT seq FROM ops WHERE book_id=?1 AND device_id=?2 ORDER BY seq DESC LIMIT 1",
                params![book, device_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(current.unwrap_or(0) as u64 + 1)
    }

    pub fn append(
        &mut self,
        book: &str,
        entity_id: &str,
        kind: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<Op> {
        let device_id = self.device_id.clone();
        self.append_as(book, &device_id, entity_id, kind, body, now_ms())
    }

    /// Create or replace a generic schema-tagged record.
    pub fn put_record(
        &mut self,
        book: &str,
        entity_id: &str,
        schema_id: &str,
        schema_version: i64,
        fields: serde_json::Value,
    ) -> anyhow::Result<Op> {
        self.append(
            book,
            entity_id,
            "record.put",
            serde_json::json!({
                "schema_id": schema_id,
                "schema_version": schema_version,
                "fields": fields,
            }),
        )
    }

    /// Write a generic tombstone.
    pub fn delete_record(
        &mut self,
        book: &str,
        entity_id: &str,
        schema_id: &str,
        schema_version: i64,
    ) -> anyhow::Result<Op> {
        self.append(
            book,
            entity_id,
            "record.delete",
            serde_json::json!({
                "schema_id": schema_id,
                "schema_version": schema_version,
            }),
        )
    }

    fn append_as(
        &mut self,
        book: &str,
        device_id: &str,
        entity_id: &str,
        kind: &str,
        body: serde_json::Value,
        hlc: u64,
    ) -> anyhow::Result<Op> {
        let seq = self.next_seq_for(book, device_id)?;
        let mut op = Op {
            op_id: String::new(),
            book_id: book.into(),
            device_id: device_id.into(),
            seq,
            hlc,
            kind: kind.into(),
            entity_id: entity_id.into(),
            body,
        };
        op.op_id = op_id(&op)?;
        let tx = self.db.unchecked_transaction()?;
        apply_projection(&tx, &op)?;
        let body = serde_json::to_string(&op.body)?;
        tx.execute(
            "INSERT INTO ops(op_id,book_id,device_id,seq,hlc,kind,entity_id,body)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                op.op_id,
                op.book_id,
                op.device_id,
                op.seq as i64,
                op.hlc as i64,
                op.kind,
                op.entity_id,
                body
            ],
        )?;
        // Only local device writes enter the outbox. Remote apply never echoes
        // an op back to the peer it came from.
        if op.device_id == self.device_id {
            tx.execute(
                "INSERT OR IGNORE INTO outbox(book_id,peer_id,op_id,enqueued_hlc)
                 SELECT ?1, peer_id, ?2, ?3 FROM sync_peers WHERE book_id=?1",
                params![op.book_id, op.op_id, op.hlc as i64],
            )?;
        }
        tx.commit()?;
        Ok(op)
    }

    /// Apply one materialized remote entry with last-writer-wins semantics.
    ///
    /// The entry revision is the remote HLC; equal revisions are ordered by
    /// origin. Returns `false` when the entry was already present or lost.
    pub fn apply_entry(&mut self, book: &str, entry: RecordEntry<'_>) -> anyhow::Result<bool> {
        let origin = if entry.origin.is_empty() {
            "remote"
        } else {
            entry.origin
        };
        if let Some(current) = self.get_optional(book, entry.entity_id)? {
            if entry.revision < current.updated_hlc {
                return Ok(false);
            }
            if entry.revision == current.updated_hlc && origin <= current.origin.as_str() {
                return Ok(false);
            }
        }
        let body = if entry.deleted {
            serde_json::json!({
                "schema_id": entry.schema_id,
                "schema_version": entry.schema_version,
            })
        } else {
            serde_json::json!({
                "schema_id": entry.schema_id,
                "schema_version": entry.schema_version,
                "fields": entry.fields,
            })
        };
        let kind = if entry.deleted {
            "record.delete"
        } else {
            "record.put"
        };
        self.append_as(book, origin, entry.entity_id, kind, body, entry.revision)?;
        Ok(true)
    }

    /// Materialized change feed, including tombstones, ordered by
    /// `(updated_hlc, entity_id)`. The cursor is the last returned pair.
    pub fn scan_changes(
        &self,
        book: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let (hlc, entity) = cursor
            .map(parse_cursor)
            .transpose()?
            .unwrap_or((0, String::new()));
        let mut statement = self.db.prepare(
            "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
             FROM records
             WHERE book_id=?1 AND (updated_hlc > ?2 OR (updated_hlc = ?2 AND entity_id > ?3))
             ORDER BY updated_hlc, entity_id LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![book, hlc as i64, entity, limit as i64],
            row_to_record,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn get_optional(&self, book: &str, id: &str) -> anyhow::Result<Option<Record>> {
        Ok(self
            .db
            .query_row(
                "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
                 FROM records WHERE book_id=?1 AND entity_id=?2",
                params![book, id],
                row_to_record,
            )
            .optional()?)
    }

    fn import_vcf(&mut self, book: &str, file: &Path) -> anyhow::Result<usize> {
        let text = fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
        let contacts = parse_vcards(&text)?;
        let mut count = 0;
        for (entity_id, contact) in contacts {
            let body = serde_json::to_value(&contact)?;
            self.append(book, &entity_id, "contact.put", body)?;
            count += 1;
        }
        Ok(count)
    }

    fn export_vcf(&self, book: &str, output: &Path) -> anyhow::Result<usize> {
        let records = self.list_schema(book, CONTACT_SCHEMA, usize::MAX)?;
        let mut out = String::new();
        for record in &records {
            let contact: Contact = serde_json::from_value(record.fields.clone())?;
            out.push_str(&contact.raw_vcard);
            if !out.ends_with('\n') {
                out.push('\n');
            }
        }
        fs::write(output, out)?;
        Ok(records.len())
    }

    fn import_ics(&mut self, book: &str, file: &Path) -> anyhow::Result<usize> {
        let text = fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
        let events = parse_ics(&text)?;
        let mut count = 0;
        for (entity_id, event) in events {
            let body = serde_json::to_value(&event)?;
            self.put_record(book, &entity_id, CALENDAR_SCHEMA, CALENDAR_VERSION, body)?;
            count += 1;
        }
        Ok(count)
    }

    fn export_ics(&self, book: &str, output: &Path) -> anyhow::Result<usize> {
        let records = self.list_schema(book, CALENDAR_SCHEMA, usize::MAX)?;
        let mut events = Vec::new();
        let mut timezones = Vec::new();
        for record in &records {
            let event: CalendarEvent = serde_json::from_value(record.fields.clone())?;
            for timezone in &event.raw_timezones {
                if !timezones.contains(timezone) {
                    timezones.push(timezone.clone());
                }
            }
            events.push(event);
        }
        let mut out = String::from(
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//fungOS//records//EN\r\nCALSCALE:GREGORIAN\r\n",
        );
        for timezone in timezones {
            out.push_str(&timezone);
            if !out.ends_with('\n') {
                out.push_str("\r\n");
            }
        }
        for event in &events {
            out.push_str(&event.raw_ics);
            if !out.ends_with('\n') {
                out.push_str("\r\n");
            }
        }
        out.push_str("END:VCALENDAR\r\n");
        fs::write(output, out)?;
        Ok(events.len())
    }

    fn import_rss(&mut self, book: &str, source: &str, limit: usize) -> anyhow::Result<usize> {
        let bytes = fetch_feed(source)?;
        self.import_feed_bytes(book, source, &bytes, limit)
    }

    fn import_feed_bytes(
        &mut self,
        book: &str,
        source: &str,
        bytes: &[u8],
        limit: usize,
    ) -> anyhow::Result<usize> {
        let feed = feed_rs::parser::parse(bytes).context("parse RSS/Atom feed")?;
        let feed_title = feed.title.as_ref().map(|title| title.content.clone());
        let feed_url = source.to_string();
        let mut count = 0;
        for entry in feed.entries.into_iter().take(limit.max(1)) {
            let link = entry.links.first().map(|link| link.href.clone());
            let uid = if entry.id.trim().is_empty() {
                link.clone().unwrap_or_default()
            } else {
                entry.id.clone()
            };
            if uid.is_empty() {
                continue;
            }
            let entity_id = format!(
                "feed-{}",
                blake3::hash(format!("{feed_url}\0{uid}").as_bytes()).to_hex()
            );
            let authors: Vec<String> = entry
                .authors
                .iter()
                .filter_map(|person| person.name.clone().or_else(|| person.email.clone()))
                .collect();
            let categories: Vec<String> = entry
                .categories
                .iter()
                .map(|category| category.term.clone())
                .filter(|term| !term.is_empty())
                .collect();
            let body = serde_json::json!({
                "feed_url": feed_url,
                "feed_title": feed_title,
                "title": entry.title.as_ref().map(|title| title.content.clone()).unwrap_or_default(),
                "link": link,
                "summary": entry.summary.as_ref().map(|summary| summary.content.clone()).unwrap_or_default(),
                "content": entry.content.as_ref().and_then(|content| content.body.clone()).unwrap_or_default(),
                "authors": authors,
                "categories": categories,
                "published": entry.published.map(|date| date.to_rfc3339()),
                "updated": entry.updated.map(|date| date.to_rfc3339()),
                "uid": uid,
            });
            self.put_record(book, &entity_id, FEED_ITEM_SCHEMA, FEED_ITEM_VERSION, body)?;
            count += 1;
        }
        Ok(count)
    }

    fn sync_feeds(&mut self, path: &Path) -> anyhow::Result<usize> {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let config: FeedsConfig = toml::from_str(&text).context("parse feeds config")?;
        let mut total = 0;
        for feed in config.feeds {
            let book = feed
                .book
                .or_else(|| config.default_book.clone())
                .ok_or_else(|| anyhow!("feed {} has no book", feed.url))?;
            let count = self.import_rss(&book, &feed.url, feed.limit)?;
            let label = feed.title.as_deref().unwrap_or(&feed.url);
            println!("{label}: {count} items -> {book}");
            total += count;
        }
        Ok(total)
    }

    /// Page live records carrying one schema id.
    pub fn list_schema(
        &self,
        book: &str,
        schema_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let mut statement = self.db.prepare(
            "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
             FROM records
             WHERE book_id=?1 AND schema_id=?2 AND deleted=0
             ORDER BY entity_id LIMIT ?3",
        )?;
        let rows = statement.query_map(params![book, schema_id, limit as i64], row_to_record)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list(
        &self,
        book: &str,
        search: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let mut statement = self.db.prepare(
            "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
             FROM records WHERE book_id=?1 AND deleted=0 ORDER BY entity_id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![book, limit as i64], row_to_record)?;
        let mut records = rows.collect::<Result<Vec<_>, _>>()?;
        if let Some(search) = search {
            let search = search.to_lowercase();
            records.retain(|record| match record.schema_id.as_str() {
                CONTACT_SCHEMA => {
                    let contact: Contact = serde_json::from_value(record.fields.clone())
                        .unwrap_or_else(|_| Contact::empty());
                    contact.display_name.to_lowercase().contains(&search)
                        || contact.organization.to_lowercase().contains(&search)
                        || contact
                            .emails
                            .iter()
                            .any(|e| e.value.to_lowercase().contains(&search))
                        || contact
                            .phones
                            .iter()
                            .any(|p| p.value.to_lowercase().contains(&search))
                }
                CALENDAR_SCHEMA => {
                    let event: CalendarEvent =
                        serde_json::from_value(record.fields.clone()).unwrap_or_default();
                    event.summary.to_lowercase().contains(&search)
                        || event.description.to_lowercase().contains(&search)
                        || event.location.to_lowercase().contains(&search)
                        || event
                            .attendees
                            .iter()
                            .any(|a| a.value.to_lowercase().contains(&search))
                }
                _ => serde_json::to_string(&record.fields)
                    .unwrap_or_default()
                    .to_lowercase()
                    .contains(&search),
            });
        }
        Ok(records)
    }

    /// Page live records by entity id. This is the SQL-backed `scan` path;
    /// callers do not load the whole book.
    pub fn scan_records(
        &self,
        book: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let mut statement = self.db.prepare(
            "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
             FROM records
             WHERE book_id=?1 AND deleted=0 AND entity_id > ?2
             ORDER BY entity_id LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![book, cursor.unwrap_or_default(), limit as i64],
            row_to_record,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Substring search over the projected record bodies in one book.
    /// Results are newest-first. This is a local discovery index, not the
    /// authority for a field-level query language.
    pub fn search_book(
        &self,
        book: &str,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<Record>> {
        if query.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self.db.prepare(
            "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
             FROM records
             WHERE book_id=?1 AND deleted=0 AND fields LIKE ?2 ESCAPE '\\'
             ORDER BY updated_hlc DESC, entity_id LIMIT ?3",
        )?;
        let pattern = format!("%{}%", escape_like(query));
        let rows = statement.query_map(params![book, pattern, limit as i64], row_to_record)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Search a host-approved set of books and merge by recency.
    pub fn search_books(
        &self,
        books: &[&str],
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let mut records = Vec::new();
        for book in books {
            records.extend(self.search_book(book, query, limit)?);
        }
        records.sort_by(|a, b| {
            b.updated_hlc
                .cmp(&a.updated_hlc)
                .then_with(|| a.entity_id.cmp(&b.entity_id))
        });
        records.truncate(limit);
        Ok(records)
    }

    /// Search one or more book namespaces in one page, newest first.
    ///
    /// This is the startup query for Spores that do not know which book will
    /// answer: inbox, contacts, calendar, and Marbles can all be asked at once.
    /// The caller supplies the namespaces it was granted; the host does not
    /// discover or guess books here.
    pub fn query_records(
        &self,
        namespaces: &[String],
        text: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<QueryPage> {
        if namespaces.is_empty() || namespaces.len() > 64 {
            bail!("query requires 1-64 namespaces");
        }
        if limit == 0 || limit > 512 {
            bail!("query limit must be 1-512");
        }
        for namespace in namespaces {
            if namespace.is_empty()
                || namespace.len() > 256
                || namespace.starts_with('/')
                || namespace
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
            {
                bail!("invalid records namespace");
            }
        }

        let mut sql = String::from(
            "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
             FROM records WHERE deleted=0",
        );
        let placeholders = std::iter::repeat_n("?", namespaces.len())
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" AND book_id IN ({placeholders})"));
        let mut values: Vec<SqlValue> = namespaces.iter().cloned().map(SqlValue::Text).collect();

        let needle = text.map(str::trim).filter(|value| !value.is_empty());
        if let Some(needle) = needle {
            sql.push_str(" AND fields LIKE ? ESCAPE '\\'");
            values.push(SqlValue::Text(format!("%{}%", escape_like(needle))));
        }
        if let Some(cursor) = cursor {
            let (revision, book, entity) = parse_query_cursor(cursor)?;
            sql.push_str(
                " AND (updated_hlc < ? OR (updated_hlc = ? AND (book_id > ? OR (book_id = ? AND entity_id > ?))))",
            );
            values.push(SqlValue::Integer(revision as i64));
            values.push(SqlValue::Integer(revision as i64));
            values.push(SqlValue::Text(book.clone()));
            values.push(SqlValue::Text(book));
            values.push(SqlValue::Text(entity));
        }
        sql.push_str(" ORDER BY updated_hlc DESC, book_id ASC, entity_id ASC LIMIT ?");
        values.push(SqlValue::Integer(limit as i64 + 1));

        let mut statement = self.db.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values.iter()), row_to_record)?;
        let mut records = rows.collect::<Result<Vec<_>, _>>()?;
        let next = if records.len() > limit {
            records.truncate(limit);
            records.last().map(query_cursor)
        } else {
            None
        };
        Ok(QueryPage { records, next })
    }

    /// The current maximum revision and live record count for one namespace.
    pub fn watermark(&self, namespace: &str) -> anyhow::Result<Watermark> {
        if namespace.is_empty()
            || namespace.len() > 256
            || namespace.starts_with('/')
            || namespace
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            bail!("invalid records namespace");
        }
        let (revision, records) = self.db.query_row(
            "SELECT COALESCE(MAX(updated_hlc),0),COUNT(*)
             FROM records WHERE book_id=?1 AND deleted=0",
            params![namespace],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
        )?;
        Ok(Watermark { revision, records })
    }

    pub fn get(&self, book: &str, id: &str) -> anyhow::Result<Record> {
        self.db
            .query_row(
                "SELECT book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin
                 FROM records WHERE book_id=?1 AND entity_id=?2",
                params![book, id],
                row_to_record,
            )
            .optional()?
            .ok_or_else(|| anyhow!("record not found: {id}"))
    }

    pub fn vector(&self, book: &str) -> anyhow::Result<Vector> {
        let mut statement = self
            .db
            .prepare("SELECT device_id, MAX(seq) FROM ops WHERE book_id=?1 GROUP BY device_id")?;
        let rows = statement.query_map(params![book], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        let devices = rows.collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(Vector {
            book_id: book.into(),
            devices,
        })
    }

    pub fn dump(&self, book: &str, since: Option<&Vector>) -> anyhow::Result<Vec<Op>> {
        let mut statement = self.db.prepare(
            "SELECT op_id,book_id,device_id,seq,hlc,kind,entity_id,body
             FROM ops WHERE book_id=?1 ORDER BY hlc, device_id, seq",
        )?;
        let rows = statement.query_map(params![book], row_to_op)?;
        let mut ops = rows.collect::<Result<Vec<_>, _>>()?;
        if let Some(vector) = since {
            ops.retain(|op| vector.devices.get(&op.device_id).copied().unwrap_or(0) < op.seq);
        }
        Ok(ops)
    }

    pub fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        if op.op_id != op_id(op)? {
            bail!("op id mismatch for {}", op.op_id);
        }
        let exists: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops WHERE op_id=?1)",
            params![op.op_id],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(());
        }
        apply_projection(&self.db, op)?;
        let body = serde_json::to_string(&op.body)?;
        self.db.execute(
            "INSERT INTO ops(op_id,book_id,device_id,seq,hlc,kind,entity_id,body)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                op.op_id,
                op.book_id,
                op.device_id,
                op.seq as i64,
                op.hlc as i64,
                op.kind,
                op.entity_id,
                body
            ],
        )?;
        Ok(())
    }
}

impl Contact {
    fn empty() -> Self {
        Self {
            display_name: String::new(),
            given_name: String::new(),
            family_name: String::new(),
            organization: String::new(),
            title: String::new(),
            emails: Vec::new(),
            phones: Vec::new(),
            addresses: Vec::new(),
            urls: Vec::new(),
            note: String::new(),
            birthday: String::new(),
            uid: String::new(),
            created: String::new(),
            last_modified: String::new(),
            raw_vcard: String::new(),
        }
    }
}

fn apply_projection(db: &Connection, op: &Op) -> anyhow::Result<()> {
    let (schema_id, schema_version, fields, deleted) = match op.kind.as_str() {
        "contact.put" => (
            CONTACT_SCHEMA.to_string(),
            CONTACT_VERSION,
            op.body.clone(),
            false,
        ),
        "contact.delete" => (
            CONTACT_SCHEMA.to_string(),
            CONTACT_VERSION,
            serde_json::json!({}),
            true,
        ),
        "record.put" => (
            op.body
                .get("schema_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown/record")
                .to_string(),
            op.body
                .get("schema_version")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(1),
            op.body
                .get("fields")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            false,
        ),
        "record.delete" => (
            op.body
                .get("schema_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown/record")
                .to_string(),
            op.body
                .get("schema_version")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(1),
            serde_json::json!({}),
            true,
        ),
        other => bail!("unknown op kind {other}"),
    };
    let revision = revision(&fields);
    db.execute(
        "INSERT INTO records(book_id,entity_id,schema_id,schema_version,revision,fields,updated_hlc,deleted,origin)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(book_id,entity_id) DO UPDATE SET
           schema_id=excluded.schema_id,
           schema_version=excluded.schema_version,
           revision=excluded.revision,
           fields=excluded.fields,
           updated_hlc=excluded.updated_hlc,
           deleted=excluded.deleted,
           origin=excluded.origin
         WHERE excluded.updated_hlc > records.updated_hlc
            OR (excluded.updated_hlc = records.updated_hlc AND excluded.origin >= records.origin)",
        params![
            op.book_id,
            op.entity_id,
            schema_id,
            schema_version,
            revision,
            serde_json::to_string(&fields)?,
            op.hlc as i64,
            deleted as i64,
            op.device_id,
        ],
    )?;
    Ok(())
}

fn row_to_op(row: &rusqlite::Row<'_>) -> rusqlite::Result<Op> {
    let body: String = row.get(7)?;
    Ok(Op {
        op_id: row.get(0)?,
        book_id: row.get(1)?,
        device_id: row.get(2)?,
        seq: row.get::<_, i64>(3)? as u64,
        hlc: row.get::<_, i64>(4)? as u64,
        kind: row.get(5)?,
        entity_id: row.get(6)?,
        body: serde_json::from_str(&body)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
    })
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<Record> {
    let fields: String = row.get(5)?;
    Ok(Record {
        book_id: row.get(0)?,
        entity_id: row.get(1)?,
        schema_id: row.get(2)?,
        schema_version: row.get(3)?,
        revision: row.get(4)?,
        fields: serde_json::from_str(&fields).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
        })?,
        updated_hlc: row.get::<_, i64>(6)? as u64,
        deleted: row.get::<_, i64>(7)? != 0,
        origin: row.get(8)?,
    })
}

fn op_id(op: &Op) -> anyhow::Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(op.book_id.as_bytes());
    hasher.update(op.device_id.as_bytes());
    hasher.update(&op.seq.to_le_bytes());
    hasher.update(&op.hlc.to_le_bytes());
    hasher.update(op.kind.as_bytes());
    hasher.update(op.entity_id.as_bytes());
    hasher.update(serde_json::to_string(&op.body)?.as_bytes());
    Ok(format!("op-{}", hasher.finalize().to_hex()))
}

fn revision(body: &serde_json::Value) -> String {
    format!(
        "blake3:{}",
        blake3::hash(serde_json::to_string(body).unwrap_or_default().as_bytes()).to_hex()
    )
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn parse_cursor(cursor: &str) -> anyhow::Result<(u64, String)> {
    let (hlc, entity) = cursor
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid records cursor"))?;
    Ok((
        hlc.parse().context("invalid records cursor hlc")?,
        entity.into(),
    ))
}

fn parse_query_cursor(cursor: &str) -> anyhow::Result<(u64, String, String)> {
    let mut parts = cursor.splitn(3, ':');
    let revision = parts
        .next()
        .ok_or_else(|| anyhow!("invalid query cursor"))?
        .parse()
        .context("invalid query cursor revision")?;
    let book = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("invalid query cursor namespace"))?;
    let entity = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("invalid query cursor entity"))?;
    Ok((revision, book.into(), entity.into()))
}

fn query_cursor(record: &Record) -> String {
    format!(
        "{}:{}:{}",
        record.updated_hlc, record.book_id, record.entity_id
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn parse_vcards(text: &str) -> anyhow::Result<Vec<(String, Contact)>> {
    let lines = VCardParser::new(text.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parse vCard")?;
    let mut blocks: Vec<Vec<Contentline>> = Vec::new();
    let mut current: Vec<Contentline> = Vec::new();
    for line in lines {
        let name = line.name().to_ascii_uppercase();
        if name == "BEGIN" && line.value().eq_ignore_ascii_case("VCARD") {
            current.clear();
        }
        current.push(line.clone());
        if name == "END" && line.value().eq_ignore_ascii_case("VCARD") {
            blocks.push(std::mem::take(&mut current));
        }
    }
    let mut out = Vec::new();
    for block in blocks {
        let contact = contact_from_lines(&block)?;
        let uid = if contact.uid.is_empty() {
            format!(
                "contact-{}",
                blake3::hash(contact.raw_vcard.as_bytes()).to_hex()
            )
        } else {
            contact.uid.clone()
        };
        out.push((uid, contact));
    }
    Ok(out)
}

fn parse_ics(text: &str) -> anyhow::Result<Vec<(String, CalendarEvent)>> {
    let lines = VCardParser::new(text.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parse iCalendar")?;
    let mut timezones = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if is_begin(&lines[index], "VTIMEZONE") {
            let (block, next) = collect_component(&lines, index);
            timezones.push(raw_block(&block));
            index = next;
        } else {
            index += 1;
        }
    }

    let mut events = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if is_begin(&lines[index], "VEVENT") {
            let (block, next) = collect_component(&lines, index);
            let mut event = event_from_lines(&block)?;
            event.raw_timezones = timezones.clone();
            let entity_id = if event.uid.trim().is_empty() {
                format!("event-{}", blake3::hash(event.raw_ics.as_bytes()).to_hex())
            } else {
                event.uid.trim().to_string()
            };
            events.push((entity_id, event));
            index = next;
        } else {
            index += 1;
        }
    }
    Ok(events)
}

fn is_begin(line: &Contentline, component: &str) -> bool {
    line.name().eq_ignore_ascii_case("BEGIN") && line.value().eq_ignore_ascii_case(component)
}

fn collect_component(lines: &[Contentline], start: usize) -> (Vec<Contentline>, usize) {
    let mut depth = 0usize;
    for index in start..lines.len() {
        let name = lines[index].name();
        if name.eq_ignore_ascii_case("BEGIN") {
            depth += 1;
        } else if name.eq_ignore_ascii_case("END") {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return (lines[start..=index].to_vec(), index + 1);
            }
        }
    }
    (lines[start..].to_vec(), lines.len())
}

fn raw_block(lines: &[Contentline]) -> String {
    let mut raw = String::new();
    for line in lines {
        raw.push_str(&line.to_string());
        raw.push_str("\r\n");
    }
    raw
}

fn event_from_lines(lines: &[Contentline]) -> anyhow::Result<CalendarEvent> {
    let mut event = CalendarEvent {
        raw_ics: raw_block(lines),
        ..CalendarEvent::default()
    };
    let mut depth = 0usize;
    for line in lines {
        let name = line.name().to_ascii_uppercase();
        if name == "BEGIN" {
            depth += 1;
            continue;
        }
        if name == "END" {
            depth = depth.saturating_sub(1);
            continue;
        }
        if depth != 1 {
            continue;
        }
        match name.as_str() {
            "SUMMARY" => event.summary = unescape(line.value()),
            "DESCRIPTION" => event.description = unescape(line.value()),
            "LOCATION" => event.location = unescape(line.value()),
            "STATUS" => event.status = unescape(line.value()),
            "UID" => event.uid = unescape(line.value()),
            "DTSTART" => {
                event.start = unescape(line.value());
                event.all_day = is_date_value(line);
                event.timezone = param(line, "TZID").unwrap_or_default();
            }
            "DTEND" => event.end = unescape(line.value()),
            "DURATION" => event.duration = unescape(line.value()),
            "RRULE" => event.rrule = unescape(line.value()),
            "URL" => event.url = unescape(line.value()),
            "CREATED" => event.created = unescape(line.value()),
            "LAST-MODIFIED" => event.last_modified = unescape(line.value()),
            "SEQUENCE" => event.sequence = unescape(line.value()),
            "CATEGORIES" => {
                event.categories = line
                    .value()
                    .split(',')
                    .map(unescape)
                    .filter(|value| !value.is_empty())
                    .collect();
            }
            "ORGANIZER" => event.organizer = attendee_from_line(line),
            "ATTENDEE" => event.attendees.push(attendee_from_line(line)),
            _ => {}
        }
    }
    Ok(event)
}

fn param(line: &Contentline, name: &str) -> Option<String> {
    line.params()
        .iter()
        .filter(|param| param.name().eq_ignore_ascii_case(name))
        .flat_map(|param| param.values().iter())
        .map(|value| unescape(value.as_str()))
        .next()
}

fn is_date_value(line: &Contentline) -> bool {
    if param(line, "VALUE").is_some_and(|value| value.eq_ignore_ascii_case("DATE")) {
        return true;
    }
    let value = line.value();
    value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn attendee_from_line(line: &Contentline) -> CalendarAttendee {
    let name = param(line, "CN").unwrap_or_default();
    let mut value = unescape(line.value());
    if value
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
    {
        value = value.get(7..).unwrap_or_default().to_string();
    }
    CalendarAttendee { name, value }
}

fn contact_from_lines(lines: &[Contentline]) -> anyhow::Result<Contact> {
    let mut contact = Contact::empty();
    let mut raw = String::new();
    for line in lines {
        raw.push_str(&line.to_string());
        raw.push_str("\r\n");
        match line.name().to_ascii_uppercase().as_str() {
            "FN" => contact.display_name = unescape(line.value()),
            "N" => {
                let parts: Vec<_> = line.value().split(';').collect();
                contact.family_name = unescape(parts.first().copied().unwrap_or(""));
                contact.given_name = unescape(parts.get(1).copied().unwrap_or(""));
            }
            "ORG" => contact.organization = unescape(line.value()),
            "TITLE" => contact.title = unescape(line.value()),
            "UID" => contact.uid = unescape(line.value()),
            "NOTE" => contact.note = unescape(line.value()),
            "BDAY" => contact.birthday = unescape(line.value()),
            "CREATED" => contact.created = unescape(line.value()),
            "LAST-MODIFIED" | "REV" => contact.last_modified = unescape(line.value()),
            "EMAIL" => contact.emails.push(labeled(line)),
            "TEL" => contact.phones.push(labeled(line)),
            "ADR" => contact.addresses.push(labeled(line)),
            "URL" => contact.urls.push(labeled(line)),
            _ => {}
        }
    }
    contact.raw_vcard = raw;
    if contact.display_name.is_empty() {
        contact.display_name = format!("{} {}", contact.given_name, contact.family_name)
            .trim()
            .into();
    }
    Ok(contact)
}

fn labeled(line: &Contentline) -> LabeledValue {
    let label = line
        .params()
        .iter()
        .filter(|param| param.name().eq_ignore_ascii_case("TYPE"))
        .flat_map(|param| param.values().iter())
        .map(|value| value.as_str().to_string())
        .collect::<Vec<_>>()
        .join(",");
    LabeledValue {
        label,
        value: unescape(line.value()),
    }
}

fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') | Some('N') => out.push('\n'),
                Some(',') => out.push(','),
                Some(';') => out.push(';'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn fetch_feed(source: &str) -> anyhow::Result<Vec<u8>> {
    if source.starts_with("http://") || source.starts_with("https://") {
        let response = ureq::get(source)
            .timeout(FEED_TIMEOUT)
            .call()
            .with_context(|| format!("fetch {source}"))?;
        let mut bytes = Vec::new();
        response
            .into_reader()
            .take(MAX_FEED_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("read feed response")?;
        if bytes.len() as u64 > MAX_FEED_BYTES {
            bail!("feed exceeds {MAX_FEED_BYTES} bytes");
        }
        Ok(bytes)
    } else {
        let path = expand_home(source);
        let metadata = fs::metadata(&path).with_context(|| format!("stat {}", path.display()))?;
        if metadata.len() > MAX_FEED_BYTES {
            bail!("feed exceeds {MAX_FEED_BYTES} bytes");
        }
        fs::read(&path).with_context(|| format!("read {}", path.display()))
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn expand_home(value: &str) -> PathBuf {
    PathBuf::from(expand_home_string(value))
}

fn expand_home_path(value: &Path) -> PathBuf {
    if value == Path::new("~") || value.starts_with("~/") {
        return expand_home(&value.to_string_lossy());
    }
    value.to_path_buf()
}

fn expand_home_string(value: &str) -> String {
    if value == "~" {
        std::env::var("HOME").unwrap_or_else(|_| value.into())
    } else if let Some(rest) = value.strip_prefix("~/") {
        format!("{}/{}", std::env::var("HOME").unwrap_or_default(), rest)
    } else {
        value.into()
    }
}

/// Local endpoint identity for one shared `unibus/peer/1` session.
#[derive(Clone, Debug)]
pub struct PeerEndpoint {
    pub installation_id: String,
    pub producer_id: String,
}

/// Result of one peer-session records exchange.
#[derive(Clone, Debug)]
pub struct PeerSyncOutcome {
    pub pulled: usize,
    pub pushed: usize,
    pub local: Vector,
    pub remote: Vector,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PullRequest {
    request_id: String,
    book_id: String,
    vector: Vector,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpsResponse {
    request_id: String,
    book_id: String,
    ops: Vec<Op>,
    vector: Vector,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PushRequest {
    request_id: String,
    book_id: String,
    ops: Vec<Op>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AckResponse {
    request_id: String,
    book_id: String,
    applied: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncError {
    request_id: String,
    book_id: String,
    code: String,
    detail: String,
}

fn load_client(path: &Path) -> anyhow::Result<Arc<Client>> {
    let config: ClientConfig = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read {}", path.display()))?,
    )
    .context("parse Unibus client config")?;
    Ok(Arc::new(
        Client::connect(config).context("connect Unibus hub")?,
    ))
}

fn serve_unibus(store: &mut Store, config: &Path) -> anyhow::Result<()> {
    let client = load_client(config)?;
    eprintln!("records: serving as {}", client.config().producer_id);
    loop {
        let Some((envelope, payload)) = client
            .try_read()
            .map_err(|error| anyhow!("read Unibus frame: {error:#}"))?
        else {
            thread::sleep(Duration::from_millis(5));
            continue;
        };
        if !payload.is_empty() {
            continue;
        }
        match envelope.message.kind.as_str() {
            PULL_KIND => {
                let Ok(request) =
                    serde_json::from_value::<PullRequest>(envelope.message.body.clone().into())
                else {
                    eprintln!("records: invalid pull request");
                    continue;
                };
                match store.dump(&request.book_id, Some(&request.vector)) {
                    Ok(ops) => {
                        let response = OpsResponse {
                            request_id: request.request_id.clone(),
                            book_id: request.book_id.clone(),
                            vector: store.vector(&request.book_id)?,
                            ops,
                        };
                        reply(&client, &envelope, OPS_KIND, &response)?;
                    }
                    Err(error) => reply_error(
                        &client,
                        &envelope,
                        &request.request_id,
                        &request.book_id,
                        "pull_failed",
                        &error.to_string(),
                    )?,
                }
            }
            PUSH_KIND => {
                let Ok(request) =
                    serde_json::from_value::<PushRequest>(envelope.message.body.clone().into())
                else {
                    eprintln!("records: invalid push request");
                    continue;
                };
                let mut applied = 0;
                let mut failure = None;
                for op in &request.ops {
                    match store.apply_op(op) {
                        Ok(()) => applied += 1,
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                if let Some(error) = failure {
                    reply_error(
                        &client,
                        &envelope,
                        &request.request_id,
                        &request.book_id,
                        "push_failed",
                        &error.to_string(),
                    )?;
                } else {
                    let response = AckResponse {
                        request_id: request.request_id,
                        book_id: request.book_id,
                        applied,
                    };
                    reply(&client, &envelope, ACK_KIND, &response)?;
                }
            }
            ERROR_KIND => {}
            _ => {}
        }
    }
}

fn reply<T: Serialize>(
    client: &Client,
    request: &Envelope,
    kind: &str,
    body: &T,
) -> anyhow::Result<()> {
    let reply = client
        .envelope(
            Some(&request.producer_id),
            Message::new(kind, serde_json::to_value(body)?),
        )
        .context("build reply")?;
    client.send(&reply, &[]).context("send reply")
}

fn reply_error(
    client: &Client,
    request: &Envelope,
    request_id: &str,
    book_id: &str,
    code: &str,
    detail: &str,
) -> anyhow::Result<()> {
    reply(
        client,
        request,
        ERROR_KIND,
        &SyncError {
            request_id: request_id.into(),
            book_id: book_id.into(),
            code: code.into(),
            detail: detail.into(),
        },
    )
}

fn sync_unibus(store: &mut Store, book: &str, config: &Path, peer: &str) -> anyhow::Result<()> {
    let client = load_client(config)?;
    let request_id = Uuid::new_v4().to_string();
    let pull = PullRequest {
        request_id: request_id.clone(),
        book_id: book.into(),
        vector: store.vector(book)?,
    };
    send_request(&client, peer, PULL_KIND, &pull)?;
    let response: OpsResponse = wait_for(&client, OPS_KIND, &request_id)?;
    if response.book_id != book {
        bail!("peer answered for {}", response.book_id);
    }
    let mut pulled = 0;
    for op in &response.ops {
        store.apply_op(op)?;
        pulled += 1;
    }
    let push_ops = store.dump(book, Some(&response.vector))?;
    let pushed = push_ops.len();
    if !push_ops.is_empty() {
        let push = PushRequest {
            request_id: request_id.clone(),
            book_id: book.into(),
            ops: push_ops,
        };
        send_request(&client, peer, PUSH_KIND, &push)?;
        let ack: AckResponse = wait_for(&client, ACK_KIND, &request_id)?;
        if ack.book_id != book {
            bail!("peer acknowledged {}", ack.book_id);
        }
    }
    println!(
        "unibus sync {book}: pulled {pulled}, pushed {pushed} (local {} remote {})",
        vector_string(&store.vector(book)?),
        vector_string(&response.vector)
    );
    Ok(())
}

/// Serve `records.sync.*` frames on one already-authorized shared peer session.
///
/// This is transport-neutral: the same function can run over a Unibus TCP hub
/// adapter, an Iroh `unibus/peer/1` session, or a test session. The session
/// layer owns identity, grants, expiry, and transport choice.
pub async fn serve_peer_session(
    session: &mut dyn PeerSession,
    store: &mut Store,
    endpoint: &PeerEndpoint,
) -> anyhow::Result<()> {
    loop {
        let (envelope, payload) = session
            .receive()
            .await
            .map_err(|error| anyhow!("peer receive: {error}"))?;
        handle_peer_frame(session, store, endpoint, &envelope, &payload).await?;
    }
}

/// Handle one frame on a shared peer session. Returns whether the kind belongs
/// to the records sync protocol. A shared host router can call this alongside
/// Geode and other application handlers.
pub fn is_sync_kind(kind: &str) -> bool {
    matches!(
        kind,
        PULL_KIND | OPS_KIND | PUSH_KIND | ACK_KIND | ERROR_KIND
    )
}

pub async fn handle_peer_frame(
    session: &mut dyn PeerSession,
    store: &mut Store,
    endpoint: &PeerEndpoint,
    envelope: &Envelope,
    payload: &[u8],
) -> anyhow::Result<bool> {
    if !payload.is_empty() || !is_sync_kind(&envelope.message.kind) {
        return Ok(false);
    }
    {
        match envelope.message.kind.as_str() {
            PULL_KIND => {
                let Ok(request) =
                    serde_json::from_value::<PullRequest>(envelope.message.body.clone().into())
                else {
                    return Ok(true);
                };
                let response = match store.dump(&request.book_id, Some(&request.vector)) {
                    Ok(ops) => OpsResponse {
                        request_id: request.request_id.clone(),
                        book_id: request.book_id.clone(),
                        vector: store.vector(&request.book_id)?,
                        ops,
                    },
                    Err(error) => {
                        reply_peer_error(
                            session,
                            endpoint,
                            &envelope,
                            &request.request_id,
                            &request.book_id,
                            "pull_failed",
                            &error.to_string(),
                        )
                        .await?;
                        return Ok(true);
                    }
                };
                reply_peer(session, endpoint, &envelope, OPS_KIND, &response).await?;
            }
            PUSH_KIND => {
                let Ok(request) =
                    serde_json::from_value::<PushRequest>(envelope.message.body.clone().into())
                else {
                    return Ok(true);
                };
                let mut applied = 0;
                let mut failure = None;
                for op in &request.ops {
                    match store.apply_op(op) {
                        Ok(()) => applied += 1,
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                if let Some(error) = failure {
                    reply_peer_error(
                        session,
                        endpoint,
                        &envelope,
                        &request.request_id,
                        &request.book_id,
                        "push_failed",
                        &error.to_string(),
                    )
                    .await?;
                } else {
                    reply_peer(
                        session,
                        endpoint,
                        &envelope,
                        ACK_KIND,
                        &AckResponse {
                            request_id: request.request_id,
                            book_id: request.book_id,
                            applied,
                        },
                    )
                    .await?;
                }
            }
            _ => {}
        }
    }
    Ok(true)
}

/// Pull then push one book over an already-authorized shared peer session.
pub async fn sync_peer_session(
    session: &mut dyn PeerSession,
    store: &mut Store,
    book: &str,
    endpoint: &PeerEndpoint,
) -> anyhow::Result<PeerSyncOutcome> {
    let request_id = Uuid::new_v4().to_string();
    let pull = PullRequest {
        request_id: request_id.clone(),
        book_id: book.into(),
        vector: store.vector(book)?,
    };
    send_peer(session, endpoint, PULL_KIND, &pull).await?;
    let response: OpsResponse = wait_peer(session, OPS_KIND, &request_id).await?;
    if response.book_id != book {
        bail!("peer answered for {}", response.book_id);
    }
    let mut pulled = 0;
    for op in &response.ops {
        store.apply_op(op)?;
        pulled += 1;
    }
    let push_ops = store.dump(book, Some(&response.vector))?;
    let pushed = push_ops.len();
    if !push_ops.is_empty() {
        send_peer(
            session,
            endpoint,
            PUSH_KIND,
            &PushRequest {
                request_id: request_id.clone(),
                book_id: book.into(),
                ops: push_ops,
            },
        )
        .await?;
        let ack: AckResponse = wait_peer(session, ACK_KIND, &request_id).await?;
        if ack.book_id != book {
            bail!("peer acknowledged {}", ack.book_id);
        }
    }
    Ok(PeerSyncOutcome {
        pulled,
        pushed,
        local: store.vector(book)?,
        remote: response.vector,
    })
}

async fn send_peer<T: Serialize>(
    session: &mut dyn PeerSession,
    endpoint: &PeerEndpoint,
    kind: &str,
    body: &T,
) -> anyhow::Result<()> {
    let envelope = Envelope::new(
        &endpoint.installation_id,
        &endpoint.producer_id,
        Some(session.peer_id()),
        "",
        Message::new(kind, serde_json::to_value(body)?),
    );
    session
        .send(&envelope, &[])
        .await
        .map_err(|error| anyhow!("peer send: {error}"))
}

async fn reply_peer<T: Serialize>(
    session: &mut dyn PeerSession,
    endpoint: &PeerEndpoint,
    request: &Envelope,
    kind: &str,
    body: &T,
) -> anyhow::Result<()> {
    let envelope = Envelope::new(
        &endpoint.installation_id,
        &endpoint.producer_id,
        Some(&request.producer_id),
        "",
        Message::new(kind, serde_json::to_value(body)?),
    );
    session
        .send(&envelope, &[])
        .await
        .map_err(|error| anyhow!("peer reply: {error}"))
}

async fn reply_peer_error(
    session: &mut dyn PeerSession,
    endpoint: &PeerEndpoint,
    request: &Envelope,
    request_id: &str,
    book_id: &str,
    code: &str,
    detail: &str,
) -> anyhow::Result<()> {
    reply_peer(
        session,
        endpoint,
        request,
        ERROR_KIND,
        &SyncError {
            request_id: request_id.into(),
            book_id: book_id.into(),
            code: code.into(),
            detail: detail.into(),
        },
    )
    .await
}

async fn wait_peer<T: for<'de> Deserialize<'de>>(
    session: &mut dyn PeerSession,
    kind: &str,
    request_id: &str,
) -> anyhow::Result<T> {
    loop {
        let (envelope, payload) = session
            .receive()
            .await
            .map_err(|error| anyhow!("peer receive: {error}"))?;
        if !payload.is_empty() {
            continue;
        }
        if envelope.message.kind == ERROR_KIND {
            let error: SyncError =
                serde_json::from_value(envelope.message.body.into()).context("parse peer error")?;
            if error.request_id == request_id {
                bail!("{}: {} ({})", error.code, error.detail, error.book_id);
            }
            continue;
        }
        if envelope.message.kind != kind {
            continue;
        }
        let body: serde_json::Value = envelope.message.body.into();
        let response_request = body
            .get("request_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if response_request != request_id {
            continue;
        }
        return serde_json::from_value(body).context("decode peer response");
    }
}

fn send_request<T: Serialize>(
    client: &Client,
    peer: &str,
    kind: &str,
    body: &T,
) -> anyhow::Result<()> {
    let envelope = client
        .envelope(Some(peer), Message::new(kind, serde_json::to_value(body)?))
        .context("build Unibus request")?;
    client.send(&envelope, &[]).context("send Unibus request")
}

fn wait_for<T: for<'de> Deserialize<'de>>(
    client: &Client,
    kind: &str,
    request_id: &str,
) -> anyhow::Result<T> {
    let deadline = Instant::now() + SYNC_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            bail!("timed out waiting for {kind}");
        }
        let Some((envelope, payload)) = client
            .try_read()
            .map_err(|error| anyhow!("read Unibus frame: {error:#}"))?
        else {
            thread::sleep(Duration::from_millis(10));
            continue;
        };
        if !payload.is_empty() {
            continue;
        }
        if envelope.message.kind == ERROR_KIND {
            let error: SyncError =
                serde_json::from_value(envelope.message.body.into()).context("parse sync error")?;
            if error.request_id == request_id {
                bail!("{}: {} ({})", error.code, error.detail, error.book_id);
            }
            continue;
        }
        if envelope.message.kind != kind {
            continue;
        }
        let body: serde_json::Value = envelope.message.body.into();
        let response_request = body
            .get("request_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if response_request != request_id {
            continue;
        }
        return serde_json::from_value(body).context("decode Unibus response");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CARD: &str = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:test-1\r\nFN:Sync Probe\r\nN:Probe;Sync;;;\r\nEMAIL;TYPE=WORK:sync-probe@example.invalid\r\nCREATED:20260102T030405Z\r\nREV:20260203T040506Z\r\nEND:VCARD\r\n";

    #[test]
    fn imports_exports_and_round_trips_ops() {
        let dir = tempfile::tempdir().unwrap();
        let card = dir.path().join("card.vcf");
        fs::write(&card, CARD).unwrap();
        let mut source = Store::open(&dir.path().join("a"), &dir.path().join("device-a")).unwrap();
        assert_eq!(source.import_vcf("test/contacts", &card).unwrap(), 1);
        let records = source.list("test/contacts", None, 10).unwrap();
        assert_eq!(records.len(), 1);
        let contact: Contact = serde_json::from_value(records[0].fields.clone()).unwrap();
        assert_eq!(contact.display_name, "Sync Probe");
        assert_eq!(contact.emails[0].value, "sync-probe@example.invalid");
        assert_eq!(contact.created, "20260102T030405Z");
        assert_eq!(contact.last_modified, "20260203T040506Z");

        let ops = source.dump("test/contacts", None).unwrap();
        assert_eq!(ops.len(), 1);

        let mut target = Store::open(&dir.path().join("b"), &dir.path().join("device-b")).unwrap();
        target.apply_op(&ops[0]).unwrap();
        assert_eq!(target.list("test/contacts", None, 10).unwrap().len(), 1);
        assert_eq!(target.vector("test/contacts").unwrap().devices.len(), 1);

        let out = dir.path().join("out.vcf");
        assert_eq!(target.export_vcf("test/contacts", &out).unwrap(), 1);
        assert!(fs::read_to_string(out).unwrap().contains("FN:Sync Probe"));
    }

    #[test]
    fn imports_and_exports_ical_events() {
        let dir = tempfile::tempdir().unwrap();
        let ics = dir.path().join("calendar.ics");
        fs::write(
            &ics,
            "BEGIN:VCALENDAR\r\n\
             VERSION:2.0\r\n\
             PRODID:-//Probe//EN\r\n\
             BEGIN:VTIMEZONE\r\n\
             TZID:America/Los_Angeles\r\n\
             BEGIN:STANDARD\r\n\
             DTSTART:19701101T020000\r\n\
             TZOFFSETFROM:-0700\r\n\
             TZOFFSETTO:-0800\r\n\
             END:STANDARD\r\n\
             END:VTIMEZONE\r\n\
             BEGIN:VEVENT\r\n\
             UID:event-1\r\n\
             SUMMARY:Design review\r\n\
             DESCRIPTION:Line one\\nLine two\r\n\
             LOCATION:Studio\r\n\
             DTSTART;TZID=America/Los_Angeles:20261105T100000\r\n\
             DTEND;TZID=America/Los_Angeles:20261105T110000\r\n\
             ORGANIZER;CN=Avery:mailto:avery@example.invalid\r\n\
             ATTENDEE;CN=Ada:mailto:ada@example.invalid\r\n\
             RRULE:FREQ=WEEKLY;COUNT=3\r\n\
             CATEGORIES:Work,Design\r\n\
             BEGIN:VALARM\r\n\
             ACTION:DISPLAY\r\n\
             TRIGGER:-PT15M\r\n\
             DESCRIPTION:Reminder\r\n\
             END:VALARM\r\n\
             END:VEVENT\r\n\
             BEGIN:VEVENT\r\n\
             UID:event-2\r\n\
             SUMMARY:Holiday\r\n\
             DTSTART;VALUE=DATE:20261225\r\n\
             END:VEVENT\r\n\
             END:VCALENDAR\r\n",
        )
        .unwrap();

        let mut store = Store::open(&dir.path().join("store"), &dir.path().join("device")).unwrap();
        assert_eq!(store.import_ics("test/calendar", &ics).unwrap(), 2);
        let records = store
            .list_schema("test/calendar", CALENDAR_SCHEMA, 10)
            .unwrap();
        assert_eq!(records.len(), 2);
        let first: CalendarEvent = serde_json::from_value(records[0].fields.clone()).unwrap();
        assert_eq!(first.summary, "Design review");
        assert_eq!(first.location, "Studio");
        assert_eq!(first.start, "20261105T100000");
        assert_eq!(first.timezone, "America/Los_Angeles");
        assert_eq!(first.organizer.value, "avery@example.invalid");
        assert_eq!(first.attendees[0].name, "Ada");
        assert_eq!(first.rrule, "FREQ=WEEKLY;COUNT=3");
        assert_eq!(first.categories, ["Work", "Design"]);
        assert!(first.raw_ics.contains("BEGIN:VALARM"));
        assert_eq!(first.raw_timezones.len(), 1);
        assert!(first.description.contains("Line one\nLine two"));

        let second: CalendarEvent = serde_json::from_value(records[1].fields.clone()).unwrap();
        assert!(second.all_day);
        assert_eq!(second.start, "20261225");

        let out = dir.path().join("out.ics");
        assert_eq!(store.export_ics("test/calendar", &out).unwrap(), 2);
        let exported = fs::read_to_string(out).unwrap();
        assert!(exported.starts_with("BEGIN:VCALENDAR"));
        assert!(exported.contains("BEGIN:VTIMEZONE"));
        assert!(exported.contains("SUMMARY:Design review"));
        assert!(exported.contains("SUMMARY:Holiday"));
    }

    #[test]
    fn imports_rss_items_as_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), &dir.path().join("device")).unwrap();
        let rss = br#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <link>https://example.invalid/</link>
    <description>Example</description>
    <item>
      <title>First post</title>
      <link>https://example.invalid/first</link>
      <guid>post-1</guid>
      <pubDate>Tue, 05 Nov 2026 10:00:00 GMT</pubDate>
      <description>Hello from RSS</description>
    </item>
  </channel>
</rss>"#;
        assert_eq!(
            store
                .import_feed_bytes("test/feeds", "https://example.invalid/feed.xml", rss, 10)
                .unwrap(),
            1
        );
        let records = store
            .list_schema("test/feeds", FEED_ITEM_SCHEMA, 10)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].fields["title"], "First post");
        assert_eq!(records[0].fields["link"], "https://example.invalid/first");
        assert_eq!(records[0].fields["summary"], "Hello from RSS");
        assert_eq!(records[0].fields["feed_title"], "Example Feed");
    }

    #[test]
    fn remote_entries_merge_by_revision_and_origin() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), &dir.path().join("device")).unwrap();
        let book = "test/records";
        let fields = serde_json::json!({"value": "first"});
        assert!(
            store
                .apply_entry(
                    book,
                    RecordEntry {
                        entity_id: "one",
                        schema_id: "test/record",
                        schema_version: 1,
                        fields: fields.clone(),
                        deleted: false,
                        revision: 100,
                        origin: "a",
                    },
                )
                .unwrap()
        );
        assert!(
            !store
                .apply_entry(
                    book,
                    RecordEntry {
                        entity_id: "one",
                        schema_id: "test/record",
                        schema_version: 1,
                        fields: fields.clone(),
                        deleted: false,
                        revision: 100,
                        origin: "0",
                    },
                )
                .unwrap(),
            "same revision from a lower origin must not win"
        );
        assert!(
            !store
                .apply_entry(
                    book,
                    RecordEntry {
                        entity_id: "one",
                        schema_id: "test/record",
                        schema_version: 1,
                        fields: fields.clone(),
                        deleted: false,
                        revision: 100,
                        origin: "a",
                    },
                )
                .unwrap(),
            "re-delivery must be idempotent"
        );
        let record = store.get(book, "one").unwrap();
        assert_eq!(record.updated_hlc, 100);
        assert_eq!(record.origin, "a");

        assert!(
            store
                .apply_entry(
                    book,
                    RecordEntry {
                        entity_id: "one",
                        schema_id: "test/record",
                        schema_version: 1,
                        fields: serde_json::json!({"value": "second"}),
                        deleted: false,
                        revision: 101,
                        origin: "c",
                    },
                )
                .unwrap()
        );
        let changes = store.scan_changes(book, None, 10).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].fields["value"], "second");
        assert_eq!(changes[0].origin, "c");

        assert!(
            store
                .apply_entry(
                    book,
                    RecordEntry {
                        entity_id: "one",
                        schema_id: "test/record",
                        schema_version: 1,
                        fields: serde_json::json!({}),
                        deleted: true,
                        revision: 102,
                        origin: "a",
                    },
                )
                .unwrap()
        );
        let changes = store.scan_changes(book, None, 10).unwrap();
        assert!(changes[0].deleted);
        let cursor = format!("{}:{}", changes[0].updated_hlc, changes[0].entity_id);
        assert!(
            store
                .scan_changes(book, Some(&cursor), 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn searches_books_by_body_substring() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), &dir.path().join("device")).unwrap();
        store
            .put_record(
                "test/contacts",
                "ada",
                "fungos.contacts/contact",
                1,
                serde_json::json!({"display_name": "Ada Lovelace", "emails": [{"value": "ada@example.invalid"}]}),
            )
            .unwrap();
        store
            .put_record(
                "test/contacts",
                "grace",
                "fungos.contacts/contact",
                1,
                serde_json::json!({"display_name": "Grace Hopper", "emails": [{"value": "grace@example.invalid"}]}),
            )
            .unwrap();
        let hits = store.search_book("test/contacts", "lovelace", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entity_id, "ada");
        assert!(
            store
                .search_book("test/contacts", "100%", 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .search_books(&["test/contacts"], "example.invalid", 10)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn shared_peer_session_syncs_records() {
        use tokio::sync::mpsc;
        use unibus::PeerFuture;

        struct TestSession {
            peer: String,
            outgoing: mpsc::UnboundedSender<(Envelope, Vec<u8>)>,
            incoming: mpsc::UnboundedReceiver<(Envelope, Vec<u8>)>,
        }

        impl PeerSession for TestSession {
            fn peer_id(&self) -> &str {
                &self.peer
            }

            fn send<'a>(
                &'a mut self,
                envelope: &'a Envelope,
                payload: &'a [u8],
            ) -> PeerFuture<'a, ()> {
                Box::pin(async move {
                    self.outgoing
                        .send((envelope.clone(), payload.to_vec()))
                        .map_err(|error| unibus::PeerError(error.to_string()))
                })
            }

            fn receive(&mut self) -> PeerFuture<'_, (Envelope, Vec<u8>)> {
                Box::pin(async move {
                    self.incoming
                        .recv()
                        .await
                        .ok_or_else(|| unibus::PeerError("closed".into()))
                })
            }
        }

        fn pair(left: &str, right: &str) -> (TestSession, TestSession) {
            let (left_tx, left_rx) = mpsc::unbounded_channel();
            let (right_tx, right_rx) = mpsc::unbounded_channel();
            (
                TestSession {
                    peer: right.into(),
                    outgoing: right_tx,
                    incoming: left_rx,
                },
                TestSession {
                    peer: left.into(),
                    outgoing: left_tx,
                    incoming: right_rx,
                },
            )
        }

        let dir = tempfile::tempdir().unwrap();
        let mut server = Store::open(
            &dir.path().join("studio"),
            &dir.path().join("studio-device"),
        )
        .unwrap();
        let mut client =
            Store::open(&dir.path().join("neo"), &dir.path().join("neo-device")).unwrap();
        server
            .put_record(
                "test/contacts",
                "ada",
                "fungos.contacts/contact",
                1,
                serde_json::json!({"display_name": "Ada Lovelace"}),
            )
            .unwrap();

        let (mut client_session, mut server_session) = pair("records.macbook", "records.studio");
        let server_endpoint = PeerEndpoint {
            installation_id: "home".into(),
            producer_id: "records.studio".into(),
        };
        let client_endpoint = PeerEndpoint {
            installation_id: "home".into(),
            producer_id: "records.macbook".into(),
        };
        let server_task = tokio::spawn(async move {
            let _ = serve_peer_session(&mut server_session, &mut server, &server_endpoint).await;
        });
        let outcome = sync_peer_session(
            &mut client_session,
            &mut client,
            "test/contacts",
            &client_endpoint,
        )
        .await
        .unwrap();
        assert_eq!(outcome.pulled, 1);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(client.list("test/contacts", None, 10).unwrap().len(), 1);
        server_task.abort();
    }

    #[test]
    fn outbox_is_durable_per_peer_and_does_not_echo_remote_ops() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), &dir.path().join("device")).unwrap();
        store
            .set_sync_peers("test/contacts", &["peer-a".into(), "peer-b".into()])
            .unwrap();
        let local = store
            .put_record(
                "test/contacts",
                "ada",
                "fungos.contacts/contact",
                1,
                serde_json::json!({"display_name": "Ada Lovelace"}),
            )
            .unwrap();
        for peer in ["peer-a", "peer-b"] {
            let pending = store
                .pending_outbox("test/contacts", peer, u64::MAX, 10)
                .unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].op_id, local.op_id);
        }

        let mut remote = Op {
            op_id: String::new(),
            book_id: "test/contacts".into(),
            device_id: "remote-device".into(),
            seq: 1,
            hlc: local.hlc + 1,
            kind: "record.put".into(),
            entity_id: "grace".into(),
            body: serde_json::json!({
                "schema_id": "fungos.contacts/contact",
                "schema_version": 1,
                "fields": {"display_name": "Grace Hopper"}
            }),
        };
        remote.op_id = op_id(&remote).unwrap();
        store.apply_op(&remote).unwrap();
        assert_eq!(
            store
                .pending_outbox("test/contacts", "peer-a", u64::MAX, 10)
                .unwrap()
                .len(),
            1
        );

        store
            .defer_outbox("test/contacts", "peer-a", &[local.clone()], 100)
            .unwrap();
        assert!(
            store
                .pending_outbox("test/contacts", "peer-a", 99, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .pending_outbox("test/contacts", "peer-a", 100, 10)
                .unwrap()
                .len(),
            1
        );
        store
            .ack_outbox("test/contacts", "peer-a", &[local])
            .unwrap();
        assert_eq!(store.outbox_len("test/contacts", "peer-a").unwrap(), 0);
        assert_eq!(store.outbox_len("test/contacts", "peer-b").unwrap(), 1);
        store
            .ack_outbox(
                "test/contacts",
                "peer-b",
                &store
                    .pending_outbox("test/contacts", "peer-b", u64::MAX, 10)
                    .unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn op_ids_are_deterministic() {
        let op = Op {
            op_id: String::new(),
            book_id: "test/contacts".into(),
            device_id: "device-a".into(),
            seq: 1,
            hlc: 42,
            kind: "contact.put".into(),
            entity_id: "test-1".into(),
            body: serde_json::json!({"display_name":"Sync Probe","emails":[]}),
        };
        assert_eq!(op_id(&op).unwrap(), op_id(&op).unwrap());
    }
}
