use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub const PORTS: [u16; 4] = [6501, 6502, 6503, 6504];
pub const QUEUE_CAPACITY: usize = 32;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Running,
    Draining,
    Force,
    Stopped,
}
#[derive(Clone, Default)]
pub struct Counters {
    pub up: u64,
    pub down: u64,
    pub completed: u64,
    pub failed: u64,
    pub up_rate: f64,
    pub down_rate: f64,
    pub completed_rate: f64,
    last_up: u64,
    last_down: u64,
    last_completed: u64,
}
impl Counters {
    fn sample(&mut self, secs: f64) {
        self.up_rate = (self.up - self.last_up) as f64 / secs;
        self.down_rate = (self.down - self.last_down) as f64 / secs;
        self.last_up = self.up;
        self.last_down = self.down;
        self.completed_rate = (self.completed - self.last_completed) as f64 / secs;
        self.last_completed = self.completed;
    }
}
#[derive(Clone)]
pub struct Client {
    pub id: u64,
    pub address: String,
    pub since: Instant,
    pub state: &'static str,
    pub port: Option<u16>,
    pub request: Option<u64>,
    pub command: String,
    pub waiting_since: Option<Instant>,
    pub counters: Counters,
}
#[derive(Clone)]
pub struct Worker {
    pub port: u16,
    pub state: &'static str,
    pub client: Option<u64>,
    pub command: String,
    pub started: Option<Instant>,
    pub retries: u64,
    pub error: String,
    pub counters: Counters,
    pub connected_secs: f64,
    pub idle_secs: f64,
    pub disconnected_secs: f64,
    state_since: Instant,
}
impl Worker {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            state: "Disconnected",
            client: None,
            command: String::new(),
            started: None,
            retries: 0,
            error: String::new(),
            counters: Counters::default(),
            connected_secs: 0.,
            idle_secs: 0.,
            disconnected_secs: 0.,
            state_since: Instant::now(),
        }
    }
    fn account_time(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.state_since).as_secs_f64();
        if self.state == "Idle" || self.state == "Busy" {
            self.connected_secs += elapsed;
            if self.state == "Idle" {
                self.idle_secs += elapsed;
            }
        } else {
            self.disconnected_secs += elapsed;
        }
        self.state_since = now;
    }
    pub fn set_state(&mut self, state: &'static str) {
        self.account_time(Instant::now());
        self.state = state;
        if state != "Busy" {
            self.client = None;
            self.command.clear();
            self.started = None;
        }
    }
    pub fn idle_percent(&self) -> Option<f64> {
        (self.connected_secs > 0.).then(|| 100. * self.idle_secs / self.connected_secs)
    }
}
#[derive(Clone)]
pub struct Timing {
    pub at: Instant,
    pub backpressure_ms: f64,
    pub queue_ms: f64,
    pub execution_ms: f64,
}
#[derive(Clone, Default)]
pub struct Sample {
    pub seconds: f64,
    pub queue: f64,
    pub busy: f64,
    pub up: f64,
    pub down: f64,
    pub wait_ms: f64,
}
#[derive(Clone)]
pub struct Model {
    pub host: String,
    pub phase: Phase,
    pub listener: String,
    pub control_error: String,
    pub clients: BTreeMap<u64, Client>,
    pub workers: Vec<Worker>,
    pub total: Counters,
    pub timings: VecDeque<Timing>,
    pub history: VecDeque<Sample>,
    pub since: Instant,
    pub last_sample: Instant,
}
pub type Shared = Arc<Mutex<Model>>;
impl Model {
    pub fn new(host: String) -> Self {
        Self {
            host,
            phase: Phase::Stopped,
            listener: "Stopped".into(),
            control_error: String::new(),
            clients: BTreeMap::new(),
            workers: PORTS.into_iter().map(Worker::new).collect(),
            total: Counters::default(),
            timings: VecDeque::new(),
            history: VecDeque::new(),
            since: Instant::now(),
            last_sample: Instant::now(),
        }
    }
    pub fn queue_depth(&self) -> usize {
        self.clients
            .values()
            .filter(|c| c.state == "Queued")
            .count()
    }
    pub fn blocked(&self) -> usize {
        self.clients
            .values()
            .filter(|c| c.state == "Enqueue blocked")
            .count()
    }
    pub fn bytes(&mut self, client: u64, worker: usize, up: bool, n: u64) {
        fn add(c: &mut Counters, up: bool, n: u64) {
            if up {
                c.up += n;
            } else {
                c.down += n;
            }
        }
        add(&mut self.total, up, n);
        add(&mut self.workers[worker].counters, up, n);
        if let Some(c) = self.clients.get_mut(&client) {
            add(&mut c.counters, up, n);
        }
    }
    pub fn sample(&mut self) {
        let now = Instant::now();
        let secs = now
            .duration_since(self.last_sample)
            .as_secs_f64()
            .max(0.001);
        self.last_sample = now;
        self.total.sample(secs);
        for c in self.clients.values_mut() {
            c.counters.sample(secs);
        }
        for w in &mut self.workers {
            w.account_time(now);
            w.counters.sample(secs);
        }
        while self
            .timings
            .front()
            .is_some_and(|t| now.duration_since(t.at) > Duration::from_secs(60))
        {
            self.timings.pop_front();
        }
        let sample = Sample {
            seconds: now.duration_since(self.since).as_secs_f64(),
            queue: self.queue_depth() as f64,
            busy: self.workers.iter().filter(|w| w.state == "Busy").count() as f64,
            up: self.total.up_rate,
            down: self.total.down_rate,
            wait_ms: percentile(self.timings.iter().map(|t| t.queue_ms), 0.95).unwrap_or(0.),
        };
        self.history.push_back(sample);
        while self.history.len() > 900 {
            self.history.pop_front();
        }
    }
}
pub fn percentile(values: impl Iterator<Item = f64>, quantile: f64) -> Option<f64> {
    let mut v: Vec<_> = values.collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    Some(
        v[((v.len() as f64 * quantile).ceil() as usize)
            .saturating_sub(1)
            .min(v.len() - 1)],
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentiles_and_accounting() {
        assert_eq!(percentile([].into_iter(), 0.95), None);
        assert_eq!(percentile([1., 3., 2., 100.].into_iter(), 0.5), Some(2.));
        let mut m = Model::new("127.0.0.1".parse().unwrap());
        assert_eq!(m.workers[0].idle_percent(), None);
        m.bytes(99, 0, true, 40);
        m.bytes(99, 0, false, 60);
        assert_eq!(m.total.up, 40);
        assert_eq!(m.workers[0].counters.down, 60);
        m.workers[0].set_state("Idle");
        m.workers[0].state_since = Instant::now() - Duration::from_secs(2);
        m.workers[0].set_state("Busy");
        assert!(m.workers[0].idle_percent().unwrap() > 99.);
        for _ in 0..1000 {
            m.sample();
        }
        assert_eq!(m.history.len(), 900);
    }
}
