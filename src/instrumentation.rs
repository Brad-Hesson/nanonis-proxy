//! GUI observations only. None of these hooks schedule or forward requests.
use crate::{
    logging::{Logger, Record},
    telemetry::*,
};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};
use tokio::io::AsyncWrite;

#[derive(Clone)]
pub struct Instrumentation {
    pub model: Shared,
    pub logger: Logger,
}
impl Instrumentation {
    pub fn client(&self, address: SocketAddr) -> ClientMetrics {
        static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
        self.model.lock().unwrap().clients.insert(
            id,
            Client {
                id,
                address: address.to_string(),
                since: Instant::now(),
                state: "Idle",
                port: None,
                request: None,
                command: String::new(),
                waiting_since: None,
                counters: Counters::default(),
            },
        );
        let client = ClientMetrics {
            observer: self.clone(),
            id,
            address,
        };
        client.log("INFO", "Client connected");
        client
    }
    pub fn worker_state(&self, port: u16, state: &'static str) {
        self.model.lock().unwrap().workers[index(port)].set_state(state);
    }
    pub fn connected(&self, port: u16) {
        self.worker_state(port, "Idle");
        self.logger.record(Record {
            level: "INFO".into(),
            port: Some(port),
            message: "Nanonis connected".into(),
            ..Default::default()
        });
    }
    pub fn connection_failed(&self, port: u16, error: &str) {
        {
            let mut model = self.model.lock().unwrap();
            let worker = &mut model.workers[index(port)];
            worker.set_state("Retrying");
            worker.retries += 1;
            worker.error = error.into();
        }
        self.logger.record(Record {
            level: "WARN".into(),
            port: Some(port),
            stage: "connect".into(),
            message: error.into(),
            ..Default::default()
        });
    }
    pub fn worker_error(&self, port: u16, error: &str) {
        self.model.lock().unwrap().workers[index(port)].error = error.into();
    }
}
fn index(port: u16) -> usize {
    (port - PORTS[0]) as usize
}

pub struct ClientMetrics {
    observer: Instrumentation,
    id: u64,
    address: SocketAddr,
}
impl ClientMetrics {
    fn log(&self, level: &str, message: &str) {
        self.observer.logger.record(Record {
            level: level.into(),
            message: message.into(),
            client: Some(self.id),
            address: self.address.to_string(),
            ..Default::default()
        });
    }
    pub fn error(&self, error: &str) {
        self.log("ERROR", error);
    }
    pub fn idle(&self) {
        if let Some(c) = self
            .observer
            .model
            .lock()
            .unwrap()
            .clients
            .get_mut(&self.id)
        {
            c.state = "Idle";
            c.port = None;
            c.request = None;
            c.command.clear();
            c.waiting_since = None;
        }
    }
    pub fn request(&self, id: u64) -> PendingRequest {
        let waiting = Instant::now();
        if let Some(c) = self
            .observer
            .model
            .lock()
            .unwrap()
            .clients
            .get_mut(&self.id)
        {
            c.state = "Enqueue blocked";
            c.waiting_since = Some(waiting);
            c.request = Some(id);
        }
        PendingRequest {
            observer: self.observer.clone(),
            id,
            client: self.id,
            address: self.address,
            waiting,
            queued: Arc::new(Mutex::new(None)),
        }
    }
}
impl Drop for ClientMetrics {
    fn drop(&mut self) {
        self.observer.model.lock().unwrap().clients.remove(&self.id);
        self.log("INFO", "Client disconnected");
    }
}

#[derive(Clone)]
pub struct PendingRequest {
    observer: Instrumentation,
    id: u64,
    client: u64,
    address: SocketAddr,
    waiting: Instant,
    queued: Arc<Mutex<Option<Instant>>>,
}
impl PendingRequest {
    pub fn enqueued(&self) {
        let mut queued = self.queued.lock().unwrap();
        if queued.is_none() {
            *queued = Some(Instant::now());
            if let Some(c) = self
                .observer
                .model
                .lock()
                .unwrap()
                .clients
                .get_mut(&self.client)
            {
                c.state = "Queued";
            }
        }
    }
    pub fn started(self, port: u16) -> Transfer {
        // Kanal may hand a request directly to a worker before send() resumes.
        self.enqueued();
        let queued = self.queued.lock().unwrap().unwrap();
        let started = Instant::now();
        {
            let mut m = self.observer.model.lock().unwrap();
            let w = &mut m.workers[index(port)];
            w.set_state("Busy");
            w.client = Some(self.client);
            w.started = Some(started);
            if let Some(c) = m.clients.get_mut(&self.client) {
                c.state = "Running";
                c.port = Some(port);
                c.waiting_since = None;
            }
        }
        let record = Record {
            client: Some(self.client),
            address: self.address.to_string(),
            request: Some(self.id),
            port: Some(port),
            stage: "request header".into(),
            backpressure_ms: Some(queued.duration_since(self.waiting).as_secs_f64() * 1000.),
            queue_ms: Some(started.duration_since(queued).as_secs_f64() * 1000.),
            ..Default::default()
        };
        Transfer {
            observer: self.observer,
            record,
            started,
            finished: false,
        }
    }
}

pub struct Transfer {
    observer: Instrumentation,
    record: Record,
    started: Instant,
    finished: bool,
}
impl Transfer {
    pub fn command(&mut self, name: &str) {
        self.record.command = name.into();
        let mut m = self.observer.model.lock().unwrap();
        m.workers[index(self.record.port.unwrap())].command = name.into();
        if let Some(c) = m.clients.get_mut(&self.record.client.unwrap()) {
            c.command = name.into();
        }
    }
    pub fn stage(&mut self, stage: &str) {
        self.record.stage = stage.into();
    }
    pub fn writer<'a, W: AsyncWrite + Unpin>(
        &'a mut self,
        inner: &'a mut W,
        up: bool,
    ) -> CountingWriter<'a, W> {
        CountingWriter {
            inner,
            transfer: self,
            up,
        }
    }
    pub fn finish(&mut self, result: &color_eyre::Result<()>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.started.elapsed().as_secs_f64() * 1000.;
        self.record.execution_ms = Some(elapsed);
        {
            let mut m = self.observer.model.lock().unwrap();
            m.timings.push_back(Timing {
                at: Instant::now(),
                backpressure_ms: self.record.backpressure_ms.unwrap(),
                queue_ms: self.record.queue_ms.unwrap(),
                execution_ms: elapsed,
            });
            while m.timings.len() > 100_000 {
                m.timings.pop_front();
            }
            let count = |c: &mut Counters| {
                if result.is_ok() {
                    c.completed += 1;
                } else {
                    c.failed += 1;
                }
            };
            count(&mut m.total);
            count(&mut m.workers[index(self.record.port.unwrap())].counters);
            if let Some(c) = m.clients.get_mut(&self.record.client.unwrap()) {
                count(&mut c.counters);
            }
        }
        if let Err(error) = result {
            self.record.level = "ERROR".into();
            self.record.message = format!("{error:#}");
            self.observer
                .worker_error(self.record.port.unwrap(), &self.record.message);
            self.observer.logger.record(self.record.clone());
        }
    }
}
impl Drop for Transfer {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(&Err(color_eyre::eyre::eyre!(
                "Transfer interrupted by shutdown"
            )));
        }
    }
}

/// Observe successful destination writes, including partial writes before failure.
/// Tokio still owns buffering, backpressure, and the copy loop.
pub struct CountingWriter<'a, W> {
    inner: &'a mut W,
    transfer: &'a mut Transfer,
    up: bool,
}
impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWriter<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(n)) = result {
            let r = &mut this.transfer.record;
            this.transfer.observer.model.lock().unwrap().bytes(
                r.client.unwrap(),
                index(r.port.unwrap()),
                this.up,
                n as u64,
            );
            if this.up {
                r.up_bytes += n as u64;
            } else {
                r.down_bytes += n as u64;
            }
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_shutdown(cx)
    }
}
