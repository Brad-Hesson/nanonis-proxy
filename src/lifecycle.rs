//! Desktop/headless lifecycle adapter. Connection controls and snapshot
//! publishing stay here; backend.rs owns the original proxy flow.
use crate::{
    backend,
    config::{self, Config},
    instrumentation::Instrumentation,
    logging::Logger,
    telemetry::{Model, Phase},
};
use color_eyre::{Result, eyre::eyre};
use std::{
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    runtime::Runtime,
    sync::{mpsc, watch},
    task::JoinHandle,
};

#[derive(Clone, Debug)]
pub enum Control {
    Connect,
    Disconnect,
    ForceDisconnect,
    SetHost(String),
    Shutdown,
}
pub struct Handle {
    pub controls: mpsc::Sender<Control>,
    pub snapshots: watch::Receiver<Model>,
}

/// Only waiting for new work is canceled by graceful disconnect. A transfer
/// runs to completion unless Force Disconnect or application shutdown is used.
#[derive(Clone)]
pub struct Session {
    mode: watch::Receiver<Phase>,
}
impl Session {
    pub fn new(mode: watch::Receiver<Phase>) -> Self {
        Self { mode }
    }
    pub fn running(&self) -> bool {
        *self.mode.borrow() == Phase::Running
    }
    pub async fn wait<T>(&self, future: impl Future<Output = T>) -> Result<T> {
        let mut mode = self.mode.clone();
        tokio::select! { biased;
            _ = mode.wait_for(|phase| *phase != Phase::Running) => Err(eyre!("Proxy disconnected")),
            result = future => Ok(result),
        }
    }
    pub async fn transfer<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let mut mode = self.mode.clone();
        tokio::select! { biased;
            _ = mode.wait_for(|phase| matches!(phase, Phase::Force | Phase::Stopped)) => Err(eyre!("Transfer interrupted by forced shutdown")),
            result = future => result,
        }
    }
}

pub fn start(
    runtime: &Runtime,
    host: String,
    logger: Logger,
    config_path: PathBuf,
) -> (Handle, JoinHandle<()>) {
    let model = Arc::new(Mutex::new(Model::new(host)));
    let (snapshot_tx, snapshots) = watch::channel(model.lock().unwrap().clone());
    let (controls, rx) = mpsc::channel(32);
    let task = runtime.spawn(run(
        Instrumentation { model, logger },
        rx,
        snapshot_tx,
        config_path,
    ));
    (
        Handle {
            controls,
            snapshots,
        },
        task,
    )
}

async fn connect(
    observer: &Instrumentation,
    mode: &watch::Sender<Phase>,
) -> Option<JoinHandle<Result<()>>> {
    let listener = match TcpListener::bind("localhost:6500").await {
        Ok(listener) => listener,
        Err(error) => {
            let error = format!("Cannot listen on localhost:6500: {error}");
            observer.model.lock().unwrap().control_error = error.clone();
            observer.logger.event("ERROR", error);
            return None;
        }
    };
    let host = {
        let mut m = observer.model.lock().unwrap();
        m.phase = Phase::Running;
        m.listener = listener.local_addr().unwrap().to_string();
        m.control_error.clear();
        m.host.clone()
    };
    mode.send_replace(Phase::Running);
    observer
        .logger
        .event("INFO", format!("Proxy connected; Nanonis {host}"));
    Some(tokio::spawn(backend::run(
        listener,
        host,
        observer.clone(),
        Session::new(mode.subscribe()),
    )))
}

async fn run(
    observer: Instrumentation,
    mut controls: mpsc::Receiver<Control>,
    snapshots: watch::Sender<Model>,
    config_path: PathBuf,
) {
    let (mode, _) = watch::channel(Phase::Stopped);
    let mut proxy = connect(&observer, &mode).await;
    let mut shutdown = false;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            command = controls.recv(), if !shutdown => {
                match command.unwrap_or(Control::Shutdown) {
                    Control::Connect if proxy.is_none() => { proxy = connect(&observer, &mode).await; }
                    Control::Connect => { observer.model.lock().unwrap().control_error = "Disconnect must finish before connecting".into(); }
                    Control::SetHost(host) if proxy.is_none() => {
                        let path = config_path.clone();
                        let config = Config { nanonis_host: host.clone() };
                        match tokio::task::spawn_blocking(move || config::save_to(&config, &path)).await {
                            Ok(Ok(())) => {
                                let mut m = observer.model.lock().unwrap();
                                m.host = host;
                                m.control_error.clear();
                            }
                            error => { observer.model.lock().unwrap().control_error = format!("Cannot save host: {error:?}"); }
                        }
                    }
                    Control::SetHost(_) => { observer.model.lock().unwrap().control_error = "Disconnect before changing the host".into(); }
                    command => {
                        shutdown = matches!(command, Control::Shutdown);
                        if proxy.is_some() {
                            let phase = if matches!(command, Control::Disconnect) && *mode.borrow() != Phase::Force {
                                Phase::Draining
                            } else { Phase::Force };
                            mode.send_replace(phase);
                            let mut m = observer.model.lock().unwrap();
                            m.phase = phase;
                            m.listener = "Stopped".into();
                            observer.logger.event("INFO", format!("Proxy {phase:?}; queued requests canceled"));
                        }
                    }
                }
            }
            result = async { proxy.as_mut().unwrap().await }, if proxy.is_some() => {
                if !matches!(result, Ok(Ok(()))) {
                    observer.logger.event("ERROR", format!("Proxy stopped with error: {result:?}"));
                }
                proxy = None;
                mode.send_replace(Phase::Stopped);
                let mut m = observer.model.lock().unwrap();
                m.phase = Phase::Stopped;
                m.listener = "Stopped".into();
                for worker in &mut m.workers { worker.set_state("Disconnected"); }
            }
            _ = tick.tick() => {
                let mut m = observer.model.lock().unwrap();
                if m.last_sample.elapsed() >= Duration::from_secs(1) { m.sample(); }
                snapshots.send_replace(m.clone());
            }
        }
        if shutdown && proxy.is_none() {
            break;
        }
    }
    observer.logger.event("INFO", "Proxy stopped");
    snapshots.send_replace(observer.model.lock().unwrap().clone());
}
