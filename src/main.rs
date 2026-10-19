mod backend;
mod config;
mod gui;
mod instrumentation;
mod lifecycle;
mod logging;
mod telemetry;

use color_eyre::{Result, eyre::eyre};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let mut headless = false;
    let mut override_host = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--headless" => headless = true,
            "--nanonis-host" | "--nanonis-ip" => {
                let value = args
                    .next()
                    .ok_or_else(|| eyre!("{arg} needs a hostname or IP address"))?;
                override_host = Some(config::parse_host(&value).map_err(|e| eyre!(e))?);
            }
            "--help" | "-h" => {
                println!(
                    "nanonis-proxy [--headless] [--nanonis-host HOST]\n\nHOST accepts a hostname or IPv4/IPv6 address; --nanonis-ip is an alias.\nDesktop dashboard by default. Fixed listener localhost:6500; upstream ports 6501–6504.\nHost override applies only to this launch. Ctrl-C stops headless mode."
                );
                return Ok(());
            }
            _ => return Err(eyre!("Unknown argument: {arg}. Use --help.")),
        }
    }
    let mut config = config::load().map_err(|e| eyre!("Cannot load configuration: {e}"))?;
    if let Some(host) = override_host {
        config.nanonis_host = host;
    }
    let (config_path, log_dir) = config::paths().map_err(|e| eyre!(e))?;
    let (logger, log_thread) = logging::Logger::start(log_dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (handle, backend) =
        lifecycle::start(&runtime, config.nanonis_host, logger.clone(), config_path);
    let control = handle.controls.clone();
    let gui_logger = logger.clone();
    let result = if headless {
        drop(gui_logger);
        runtime
            .block_on(async { tokio::signal::ctrl_c().await })
            .map_err(Into::into)
    } else {
        let options = eframe::NativeOptions {
            viewport: eframe::egui::ViewportBuilder::default()
                .with_inner_size([1200., 850.])
                .with_min_inner_size([700., 500.]),
            ..Default::default()
        };
        eframe::run_native(
            "Nanonis Proxy",
            options,
            Box::new(move |_| Ok(Box::new(gui::App::new(handle, gui_logger)))),
        )
        .map_err(|e| eyre!(e.to_string()))
    };
    runtime.block_on(async {
        let _ = control.send(lifecycle::Control::Shutdown).await;
        backend.await
    })?;
    drop(control);
    drop(runtime);
    drop(logger);
    // All logger senders have now been dropped. Finish queued disk writes.
    log_thread
        .join()
        .map_err(|_| eyre!("Log writer panicked"))?;
    result
}
