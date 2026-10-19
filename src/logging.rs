use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
};

const MAX_BYTES: u64 = 10 * 1024 * 1024;
const MAX_RECORDS: usize = 10_000;
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Record {
    pub timestamp: String,
    pub session: String,
    pub level: String,
    pub message: String,
    pub client: Option<u64>,
    pub address: String,
    pub port: Option<u16>,
    pub request: Option<u64>,
    pub command: String,
    pub stage: String,
    pub up_bytes: u64,
    pub down_bytes: u64,
    pub backpressure_ms: Option<f64>,
    pub queue_ms: Option<f64>,
    pub execution_ms: Option<f64>,
}
#[derive(Default)]
pub struct LogState {
    pub records: VecDeque<Record>,
    pub write_error: String,
    pub dropped: u64,
    pub loading: bool,
}
#[derive(Clone)]
pub struct Logger {
    pub state: Arc<Mutex<LogState>>,
    tx: mpsc::SyncSender<Record>,
    pub directory: PathBuf,
    session: String,
}
impl Logger {
    pub fn start(directory: PathBuf) -> (Self, thread::JoinHandle<()>) {
        let (tx, rx) = mpsc::sync_channel::<Record>(4096);
        let state = Arc::new(Mutex::new(LogState {
            loading: true,
            ..Default::default()
        }));
        let logger = Self {
            state: state.clone(),
            tx,
            directory: directory.clone(),
            session: format!(
                "{}-{}",
                chrono::Utc::now().timestamp_millis(),
                std::process::id()
            ),
        };
        let handle = thread::spawn(move || {
            let result = (|| -> std::io::Result<()> {
                std::fs::create_dir_all(&directory)?;
                let records = read_retained(&directory)?;
                {
                    let mut s = state.lock().unwrap();
                    let current = std::mem::take(&mut s.records);
                    s.records = records;
                    s.records.extend(current);
                    trim(&mut s.records);
                    s.loading = false;
                }
                let mut file = open_current(&directory)?;
                let mut size = file.metadata()?.len();
                while let Ok(record) = rx.recv() {
                    let result = write_record(&directory, &mut file, &mut size, &record, MAX_BYTES);
                    let mut s = state.lock().unwrap();
                    if let Err(e) = result {
                        s.write_error = e.to_string();
                        s.dropped += 1;
                    }
                }
                file.flush()?;
                Ok(())
            })();
            if let Err(e) = result {
                let mut s = state.lock().unwrap();
                s.write_error = e.to_string();
                s.loading = false;
            }
        });
        (logger, handle)
    }
    pub fn record(&self, mut record: Record) {
        record.timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        record.session = self.session.clone();
        match record.level.as_str() {
            "ERROR" => tracing::error!("{}: {}", record.stage, record.message),
            "WARN" => tracing::warn!("{}", record.message),
            _ => tracing::info!("{}", record.message),
        }
        {
            let mut s = self.state.lock().unwrap();
            s.records.push_back(record.clone());
            trim(&mut s.records);
        }
        if self.tx.try_send(record).is_err() {
            self.state.lock().unwrap().dropped += 1;
        }
    }
    pub fn event(&self, level: &str, message: impl Into<String>) {
        self.record(Record {
            level: level.into(),
            message: message.into(),
            ..Default::default()
        });
    }
}
fn trim(records: &mut VecDeque<Record>) {
    while records.len() > MAX_RECORDS {
        records.pop_front();
    }
}
fn open_current(dir: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("proxy.jsonl"))
}
fn write_record(
    dir: &Path,
    file: &mut File,
    size: &mut u64,
    record: &Record,
    limit: u64,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    if *size > 0 && *size + bytes.len() as u64 > limit {
        file.flush()?;
        // Close the handle before renaming so rotation also works on Windows.
        let replacement = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("rotation.tmp"))?;
        drop(std::mem::replace(file, replacement));
        let oldest = dir.join("proxy.4.jsonl");
        if oldest.exists() {
            std::fs::remove_file(oldest)?;
        }
        for i in (1..4).rev() {
            let from = dir.join(format!("proxy.{i}.jsonl"));
            if from.exists() {
                std::fs::rename(from, dir.join(format!("proxy.{}.jsonl", i + 1)))?;
            }
        }
        std::fs::rename(dir.join("proxy.jsonl"), dir.join("proxy.1.jsonl"))?;
        *file = open_current(dir)?;
        let _ = std::fs::remove_file(dir.join("rotation.tmp"));
        *size = 0;
    }
    file.write_all(&bytes)?;
    file.flush()?;
    *size += bytes.len() as u64;
    Ok(())
}
pub fn read_retained(dir: &Path) -> std::io::Result<VecDeque<Record>> {
    let mut records = VecDeque::new();
    let names = (1..=4)
        .rev()
        .map(|i| format!("proxy.{i}.jsonl"))
        .chain(std::iter::once("proxy.jsonl".into()));
    for name in names {
        let file = match File::open(dir.join(name)) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for line in BufReader::new(file).lines() {
            if let Ok(record) = serde_json::from_str::<Record>(&line?) {
                records.push_back(record);
                trim(&mut records);
            }
        }
    }
    Ok(records)
}

/// Scan on a background thread, retaining only one page of matching records.
pub fn read_page(
    dir: &Path,
    offset: usize,
    matches: impl Fn(&Record) -> bool,
) -> std::io::Result<(Vec<Record>, bool)> {
    let mut records = Vec::new();
    let mut skipped = 0;
    let names = (1..=4)
        .rev()
        .map(|i| format!("proxy.{i}.jsonl"))
        .chain(std::iter::once("proxy.jsonl".into()));
    for name in names {
        let file = match File::open(dir.join(name)) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for line in BufReader::new(file).lines() {
            let Ok(record) = serde_json::from_str::<Record>(&line?) else {
                continue;
            };
            if !matches(&record) {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if records.len() == MAX_RECORDS {
                return Ok((records, true));
            }
            records.push(record);
        }
    }
    Ok((records, false))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotates_and_reads() {
        let dir = std::env::temp_dir().join(format!("nanonis-log-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut f = open_current(&dir).unwrap();
        let mut size = 0;
        for i in 0..20 {
            write_record(
                &dir,
                &mut f,
                &mut size,
                &Record {
                    message: i.to_string(),
                    ..Default::default()
                },
                500,
            )
            .unwrap();
        }
        let records = read_retained(&dir).unwrap();
        assert_eq!(records.back().unwrap().message, "19");
        assert!(records.len() < 20);
        assert!(!dir.join("proxy.5.jsonl").exists());
        let (matches, more) = read_page(&dir, 0, |r| r.message == "19").unwrap();
        assert_eq!(matches.len(), 1);
        assert!(!more);
        drop(f);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn storage_failure_keeps_memory_diagnostics() {
        let path =
            std::env::temp_dir().join(format!("nanonis-unwritable-log-{}", std::process::id()));
        std::fs::write(&path, b"not a directory").unwrap();
        let (logger, thread) = Logger::start(path.clone());
        thread.join().unwrap();
        logger.event("ERROR", "still visible");
        let state = logger.state.lock().unwrap();
        assert!(!state.write_error.is_empty());
        assert_eq!(state.dropped, 1);
        assert_eq!(state.records.back().unwrap().message, "still visible");
        drop(state);
        drop(logger);
        std::fs::remove_file(path).unwrap();
    }
}
