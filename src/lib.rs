//! First local-first records slice for fungOS PIM.
//!
//! This implementation is intentionally narrow: a SQLite materialized view over
//! a signed-by-transport (SSH for now) per-device op log, plus vCard import and
//! export. The sync transport is a separate command so Unibus can replace it
//! without changing the store.

use std::{
    collections::BTreeMap,
    fs,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow, bail};
use clap::{Parser, Subcommand};
use ical_vcard::{Contentline, Parser as VCardParser};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use unibus::client::{Client, ClientConfig};
use unibus::{Envelope, Message};
use uuid::Uuid;

const CONTACT_SCHEMA: &str = "fungos.contacts/contact";
const CONTACT_VERSION: i64 = 1;
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
    pub raw_vcard: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LabeledValue {
    label: String,
    value: String,
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
        apply_projection(&self.db, &op)?;
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
        let records = self.list(book, None, usize::MAX)?;
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
            records.retain(|record| {
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
            });
        }
        Ok(records)
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
        let rows = statement.query_map(params![book], |row| {
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
        })?;
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

fn parse_cursor(cursor: &str) -> anyhow::Result<(u64, String)> {
    let (hlc, entity) = cursor
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid records cursor"))?;
    Ok((
        hlc.parse().context("invalid records cursor hlc")?,
        entity.into(),
    ))
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

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn expand_home(value: &str) -> PathBuf {
    PathBuf::from(expand_home_string(value))
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

#[derive(Debug, Serialize, Deserialize)]
struct PullRequest {
    request_id: String,
    book_id: String,
    vector: Vector,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpsResponse {
    request_id: String,
    book_id: String,
    ops: Vec<Op>,
    vector: Vector,
}

#[derive(Debug, Serialize, Deserialize)]
struct PushRequest {
    request_id: String,
    book_id: String,
    ops: Vec<Op>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AckResponse {
    request_id: String,
    book_id: String,
    applied: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncError {
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

    const CARD: &str = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:test-1\r\nFN:Sync Probe\r\nN:Probe;Sync;;;\r\nEMAIL;TYPE=WORK:sync-probe@example.invalid\r\nEND:VCARD\r\n";

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
