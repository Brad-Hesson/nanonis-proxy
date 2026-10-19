use crate::{
    lifecycle::{Control, Handle},
    logging::{Logger, Record},
    telemetry::{Model, Phase, Sample, percentile},
};
use eframe::egui::{self, Color32, RichText};
use std::{path::PathBuf, time::Duration};

#[derive(Default, PartialEq)]
enum Tab {
    #[default]
    Overview,
    Clients,
    Logs,
}
#[derive(Clone, Default)]
struct Filters {
    text: String,
    client: String,
    port: String,
    request: String,
    command: String,
    from: String,
    to: String,
    level: usize,
}
impl Filters {
    fn matches(&self, r: &Record) -> bool {
        (self.level == 0
            || (self.level == 1 && r.level == "ERROR")
            || (self.level == 2 && matches!(r.level.as_str(), "ERROR" | "WARN")))
            && (self.client.is_empty() || r.client.is_some_and(|v| v.to_string() == self.client))
            && (self.port.is_empty() || r.port.is_some_and(|v| v.to_string() == self.port))
            && (self.request.is_empty() || r.request.is_some_and(|v| v.to_string() == self.request))
            && (self.command.is_empty()
                || r.command
                    .to_lowercase()
                    .contains(&self.command.to_lowercase()))
            && (self.from.is_empty() || r.timestamp >= self.from)
            && (self.to.is_empty() || r.timestamp <= self.to)
            && (self.text.is_empty()
                || format!(
                    "{} {} {} {} {}",
                    r.message, r.stage, r.command, r.address, r.session
                )
                .to_lowercase()
                .contains(&self.text.to_lowercase()))
    }
}
type ArchiveJob = std::thread::JoinHandle<Result<(Vec<Record>, bool), String>>;
pub struct App {
    handle: Handle,
    logger: Logger,
    tab: Tab,
    host: String,
    ports: [u16; 4],
    pending: String,
    filters: Filters,
    paused: bool,
    follow: bool,
    records: Vec<Record>,
    selected: Option<Record>,
    export_path: String,
    export_job: Option<std::thread::JoinHandle<Result<(), String>>>,
    sort: usize,
    descending: bool,
    archive_active: bool,
    archive_offset: usize,
    archive_more: bool,
    archive_job: Option<ArchiveJob>,
}
impl App {
    pub fn new(handle: Handle, logger: Logger) -> Self {
        let host = handle.snapshots.borrow().host.to_string();
        let ports = handle.snapshots.borrow().ports();
        let export_path = logger.directory.join("export.jsonl").display().to_string();
        Self {
            handle,
            logger,
            tab: Tab::Overview,
            host,
            ports,
            pending: String::new(),
            filters: Filters::default(),
            paused: false,
            follow: true,
            records: vec![],
            selected: None,
            export_path,
            export_job: None,
            sort: 0,
            descending: false,
            archive_active: false,
            archive_offset: 0,
            archive_more: false,
            archive_job: None,
        }
    }
    fn send(&mut self, command: Control) {
        if let Err(e) = self.handle.controls.try_send(command) {
            self.pending = format!("Could not send control: {e}");
        } else {
            self.pending.clear();
        }
    }
    fn controls(&mut self, ui: &mut egui::Ui, model: &Model) {
        let logs = self.logger.state.lock().unwrap();
        if !logs.write_error.is_empty() || logs.dropped > 0 {
            ui.colored_label(
                ui.visuals().error_fg_color,
                format!(
                    "Diagnostics storage: {} | {} records not persisted",
                    logs.write_error, logs.dropped
                ),
            );
        }
        drop(logs);
        ui.horizontal_wrapped(|ui| {
            ui.heading("Nanonis proxy");
            ui.separator();
            ui.label(format!("{:?}", model.phase));
            if ui
                .add_enabled(model.phase == Phase::Stopped, egui::Button::new("Connect"))
                .clicked()
            {
                self.send(Control::Connect);
            }
            if ui
                .add_enabled(
                    model.phase == Phase::Running,
                    egui::Button::new("Disconnect"),
                )
                .clicked()
            {
                self.send(Control::Disconnect);
            }
            if model.phase == Phase::Draining
                && ui
                    .button("Force Disconnect")
                    .on_hover_text("Interrupt running transfers and close their client connections")
                    .clicked()
            {
                self.send(Control::ForceDisconnect);
            }
            ui.separator();
            ui.label("Nanonis host / IP:");
            ui.add_enabled(
                model.phase == Phase::Stopped,
                egui::TextEdit::singleline(&mut self.host).desired_width(160.),
            );
            ui.label("Upstream ports:");
            for (index, port) in self.ports.iter_mut().enumerate() {
                ui.add_enabled(
                    model.phase == Phase::Stopped,
                    egui::DragValue::new(port).range(1..=u16::MAX).speed(1.),
                )
                .on_hover_text(format!("Nanonis connection {} port", index + 1));
            }
            if ui
                .add_enabled(
                    model.phase == Phase::Stopped,
                    egui::Button::new("Apply settings"),
                )
                .on_hover_text(
                    "Available only when all upstream connections and attempts have stopped",
                )
                .clicked()
            {
                match crate::config::parse_host(&self.host) {
                    Ok(host) => {
                        let config = crate::config::Config {
                            nanonis_host: host,
                            nanonis_ports: self.ports,
                        };
                        match config.validate() {
                            Ok(()) => self.send(Control::SetConfig(config)),
                            Err(error) => self.pending = error,
                        }
                    }
                    Err(error) => self.pending = error,
                }
            }
        });
        ui.label(format!(
            "Listener: {}    Upstream: {}    Ports: {}",
            model.listener,
            model.host,
            model.ports().map(|p| p.to_string()).join(", ")
        ));
        if model.phase == Phase::Draining {
            ui.label("Draining active transfers. Queued work was canceled; client connections will close.");
        }
        for error in [&model.control_error, &self.pending] {
            if !error.is_empty() {
                ui.colored_label(ui.visuals().error_fg_color, error);
            }
        }
        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.tab, Tab::Overview, "Overview");
            ui.selectable_value(
                &mut self.tab,
                Tab::Clients,
                format!("Clients ({})", model.clients.len()),
            );
            ui.selectable_value(&mut self.tab, Tab::Logs, "Diagnostics");
        });
    }
    fn overview(&mut self, ui: &mut egui::Ui, m: &Model) {
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("Clients: {}", m.clients.len()));
            ui.separator();
            ui.label(format!("Queue: {} / 32", m.queue_depth()));
            ui.label(format!("Enqueue blocked: {}", m.blocked()));
            let oldest = m
                .clients
                .values()
                .filter_map(|c| c.waiting_since)
                .map(|t| t.elapsed().as_secs_f64())
                .reduce(f64::max);
            ui.label(format!(
                "Oldest wait: {}",
                oldest.map_or("N/A".into(), |s| format!("{s:.1} s"))
            ));
            ui.separator();
            ui.label(format!(
                "Forwarded: {}   Transfer failures: {}",
                m.total.completed, m.total.failed
            ));
        });
        ui.label(format!(
            "Request: {}  ({})    Response: {}  ({})",
            rate(m.total.up_rate),
            bytes(m.total.up),
            rate(m.total.down_rate),
            bytes(m.total.down)
        ));
        ui.label("Transport completion does not indicate Nanonis application success. Byte counts measure bytes forwarded to each destination.");
        let connected: f64 = m.workers.iter().map(|w| w.connected_secs).sum();
        let idle: f64 = m.workers.iter().map(|w| w.idle_secs).sum();
        ui.label(format!(
            "Forwarded requests: {:.1}/s    Aggregate idle: {}",
            m.total.completed_rate,
            if connected > 0. {
                format!("{:.1}%", 100. * idle / connected)
            } else {
                "N/A".into()
            }
        ));
        ui.add_space(6.);
        ui.columns(2, |columns| {
            for (i, worker) in m.workers.iter().enumerate() {
                columns[i % 2].group(|ui| {
                    ui.horizontal(|ui| {
                        ui.colored_label(state_color(worker.state), "●");
                        ui.strong(format!("Port {} — {}", worker.port, worker.state));
                    });
                    ui.label(format!(
                        "Client: {}   Command: {}",
                        worker.client.map_or("—".into(), |id| id.to_string()),
                        if worker.command.is_empty() {
                            "—"
                        } else {
                            &worker.command
                        }
                    ));
                    ui.label(format!(
                        "Elapsed: {}   Idle: {}",
                        worker.started.map_or("—".into(), |t| format!(
                            "{:.1} s",
                            t.elapsed().as_secs_f64()
                        )),
                        worker
                            .idle_percent()
                            .map_or("N/A".into(), |p| format!("{p:.1}%"))
                    ));
                    ui.label(format!(
                        "↑ {}   ↓ {}   {:.1} requests/s",
                        rate(worker.counters.up_rate),
                        rate(worker.counters.down_rate),
                        worker.counters.completed_rate
                    ));
                    ui.label(format!(
                        "Bytes ↑ {}  ↓ {}   Complete {} / failed {}",
                        bytes(worker.counters.up),
                        bytes(worker.counters.down),
                        worker.counters.completed,
                        worker.counters.failed
                    ));
                    ui.label(format!(
                        "Retries: {}   Disconnected: {:.0} s",
                        worker.retries, worker.disconnected_secs
                    ));
                    if !worker.error.is_empty() {
                        ui.label(RichText::new(format!("Last error: {}", worker.error)).small());
                    }
                });
            }
        });
        ui.add_space(8.);
        egui::Grid::new("timings").striped(true).show(ui, |ui| {
            ui.strong("Last 60 seconds");
            ui.strong("Median");
            ui.strong("p95");
            ui.end_row();
            for (name, values) in [
                (
                    "Enqueue backpressure",
                    m.timings
                        .iter()
                        .map(|t| t.backpressure_ms)
                        .collect::<Vec<_>>(),
                ),
                (
                    "Inside queue",
                    m.timings.iter().map(|t| t.queue_ms).collect(),
                ),
                (
                    "Worker execution (includes client I/O)",
                    m.timings.iter().map(|t| t.execution_ms).collect(),
                ),
            ] {
                ui.label(name);
                ui.label(ms(percentile(values.iter().copied(), 0.5)));
                ui.label(ms(percentile(values.iter().copied(), 0.95)));
                ui.end_row();
            }
        });
        ui.label("Idle percentage uses connected time only. Timing samples include failed transfers; at most 100,000 recent samples are retained.");
        let history: Vec<_> = m.history.iter().collect();
        plot(ui, "Queue depth", &history, |s| s.queue, 32., "requests");
        plot(ui, "Busy workers", &history, |s| s.busy, 4., "workers");
        plot(ui, "Request throughput", &history, |s| s.up, 1., "B/s");
        plot(ui, "Response throughput", &history, |s| s.down, 1., "B/s");
        plot(ui, "Queue wait p95", &history, |s| s.wait_ms, 1., "ms");
    }
    fn clients(&mut self, ui: &mut egui::Ui, m: &Model) {
        ui.horizontal(|ui| {
            ui.label("Sort by:");
            egui::ComboBox::from_id_salt("client_sort")
                .selected_text(
                    [
                        "Connection ID",
                        "Address",
                        "State",
                        "Throughput",
                        "Connection age",
                    ][self.sort],
                )
                .show_ui(ui, |ui| {
                    for (i, name) in [
                        "Connection ID",
                        "Address",
                        "State",
                        "Throughput",
                        "Connection age",
                    ]
                    .iter()
                    .enumerate()
                    {
                        ui.selectable_value(&mut self.sort, i, *name);
                    }
                });
            ui.checkbox(&mut self.descending, "Descending");
        });
        let mut clients: Vec<_> = m.clients.values().collect();
        clients.sort_by(|a, b| match self.sort {
            1 => a.address.cmp(&b.address),
            2 => a.state.cmp(b.state),
            3 => (a.counters.up_rate + a.counters.down_rate)
                .total_cmp(&(b.counters.up_rate + b.counters.down_rate)),
            4 => b.since.cmp(&a.since),
            _ => a.id.cmp(&b.id),
        });
        if self.descending {
            clients.reverse();
        }
        egui::ScrollArea::horizontal().show(ui, |ui| {
            egui::Grid::new("clients").striped(true).show(ui, |ui| {
                for label in [
                    "ID",
                    "Remote address",
                    "Age",
                    "State",
                    "Port",
                    "Request / command",
                    "Request bytes",
                    "Response bytes",
                    "Request rate",
                    "Response rate",
                    "Complete / failed",
                    "Requests/s",
                    "",
                ] {
                    ui.strong(label);
                }
                ui.end_row();
                for c in clients {
                    ui.label(c.id.to_string());
                    ui.label(&c.address);
                    ui.label(format!("{:.0}s", c.since.elapsed().as_secs_f64()));
                    ui.label(c.state);
                    ui.label(c.port.map_or("—".into(), |p| p.to_string()));
                    ui.label(format!(
                        "{} {}",
                        c.request.map_or("—".into(), |r| r.to_string()),
                        c.command
                    ));
                    ui.label(bytes(c.counters.up));
                    ui.label(bytes(c.counters.down));
                    ui.label(rate(c.counters.up_rate));
                    ui.label(rate(c.counters.down_rate));
                    ui.label(format!("{} / {}", c.counters.completed, c.counters.failed));
                    ui.label(format!("{:.1}", c.counters.completed_rate));
                    if ui.button("Logs").clicked() {
                        self.filters.client = c.id.to_string();
                        self.tab = Tab::Logs;
                    }
                    ui.end_row();
                }
            });
        });
        if m.clients.is_empty() {
            ui.label("No clients connected.");
        }
    }
    fn logs(&mut self, ui: &mut egui::Ui) {
        if !self.paused && !self.archive_active {
            self.records = self
                .logger
                .state
                .lock()
                .unwrap()
                .records
                .iter()
                .cloned()
                .collect();
        }
        let state = self.logger.state.lock().unwrap();
        ui.label(format!(
            "Logs: {} | {} records in viewer | {} records not persisted",
            self.logger.directory.display(),
            self.records.len(),
            state.dropped
        ));
        if state.loading {
            ui.spinner();
        }
        if !state.write_error.is_empty() {
            ui.colored_label(
                ui.visuals().error_fg_color,
                format!("Log storage error: {}", state.write_error),
            );
        }
        drop(state);
        ui.horizontal_wrapped(|ui| {
            ui.checkbox(&mut self.paused, "Pause view");
            ui.checkbox(&mut self.follow, "Follow tail");
            egui::ComboBox::from_id_salt("severity")
                .selected_text(["All levels", "Errors", "Warnings + errors"][self.filters.level])
                .show_ui(ui, |ui| {
                    for (i, text) in ["All levels", "Errors", "Warnings + errors"]
                        .iter()
                        .enumerate()
                    {
                        ui.selectable_value(&mut self.filters.level, i, *text);
                    }
                });
            if ui.button("Reset filters").clicked() {
                self.filters = Filters::default();
            }
        });
        ui.horizontal_wrapped(|ui| {
            field(ui, "Text", &mut self.filters.text, 160.);
            field(ui, "Client ID", &mut self.filters.client, 60.);
            field(ui, "Port", &mut self.filters.port, 60.);
            field(ui, "Request ID", &mut self.filters.request, 70.);
            field(ui, "Command", &mut self.filters.command, 120.);
        });
        ui.horizontal_wrapped(|ui| {
            field(ui, "From UTC (ISO)", &mut self.filters.from, 170.);
            field(ui, "To UTC (ISO)", &mut self.filters.to, 170.);
        });
        ui.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(
                    self.archive_job.is_none(),
                    egui::Button::new("Search retained files"),
                )
                .clicked()
            {
                self.archive_offset = 0;
                self.load_archive();
            }
            if self.archive_active {
                ui.label(format!(
                    "Archive page {} (oldest first)",
                    self.archive_offset / 10_000 + 1
                ));
                if ui
                    .add_enabled(
                        self.archive_offset > 0 && self.archive_job.is_none(),
                        egui::Button::new("Previous"),
                    )
                    .clicked()
                {
                    self.archive_offset = self.archive_offset.saturating_sub(10_000);
                    self.load_archive();
                }
                if ui
                    .add_enabled(
                        self.archive_more && self.archive_job.is_none(),
                        egui::Button::new("Next"),
                    )
                    .clicked()
                {
                    self.archive_offset += 10_000;
                    self.load_archive();
                }
                if ui
                    .add_enabled(
                        self.archive_job.is_none(),
                        egui::Button::new("Back to live"),
                    )
                    .clicked()
                {
                    self.archive_active = false;
                    self.paused = false;
                }
            }
            if self.archive_job.is_some() {
                ui.spinner();
            }
        });
        if self.archive_job.as_ref().is_some_and(|j| j.is_finished()) {
            match self.archive_job.take().unwrap().join() {
                Ok(Ok((records, more))) => {
                    self.records = records;
                    self.archive_more = more;
                }
                Ok(Err(error)) => self.pending = format!("Archive search failed: {error}"),
                Err(_) => self.pending = "Archive search thread failed".into(),
            }
        }
        let filtered: Vec<_> = self
            .records
            .iter()
            .filter(|r| self.filters.matches(r))
            .collect();
        ui.horizontal(|ui| {
            ui.label(format!("{} matches", filtered.len()));
            ui.text_edit_singleline(&mut self.export_path);
            if ui
                .add_enabled(self.export_job.is_none(), egui::Button::new("Export JSONL"))
                .on_hover_text("Creates a new file; existing files are never overwritten")
                .clicked()
            {
                let records: Vec<_> = filtered.iter().map(|r| (*r).clone()).collect();
                let path = PathBuf::from(&self.export_path);
                self.export_job = Some(std::thread::spawn(move || {
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path)
                        .map_err(|e| e.to_string())?;
                    for r in records {
                        serde_json::to_writer(&mut file, &r).map_err(|e| e.to_string())?;
                        writeln!(file).map_err(|e| e.to_string())?;
                    }
                    Ok(())
                }));
            }
        });
        if self.export_job.as_ref().is_some_and(|j| j.is_finished()) {
            self.pending = match self.export_job.take().unwrap().join() {
                Ok(Ok(())) => "Export saved".into(),
                Ok(Err(e)) => format!("Export failed: {e}"),
                Err(_) => "Export thread failed".into(),
            };
        }
        egui::Frame::new()
            .fill(Color32::from_rgb(12, 15, 20))
            .inner_margin(8.)
            .show(ui, |ui| {
                let size = ui.available_size();
                ui.set_min_size(size);
                ui.visuals_mut().override_text_color = Some(Color32::from_rgb(220, 224, 230));
                egui::ScrollArea::both()
                    .id_salt("log_rows")
                    .auto_shrink([false, false])
                    .max_height(size.y)
                    .stick_to_bottom(self.follow)
                    .show_rows(ui, 22., filtered.len(), |ui, range| {
                        ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Extend);
                        for i in range {
                            let r = filtered[i];
                            let mut line = egui::text::LayoutJob::default();
                            let mut append = |text: &str, color| {
                                line.append(
                                    text,
                                    0.,
                                    egui::TextFormat {
                                        font_id: egui::FontId::monospace(13.),
                                        color,
                                        ..Default::default()
                                    },
                                );
                            };
                            append(
                                &format!("{} ", r.timestamp),
                                Color32::from_rgb(140, 150, 165),
                            );
                            let color = match r.level.as_str() {
                                "ERROR" => Color32::from_rgb(255, 105, 105),
                                "WARN" => Color32::from_rgb(245, 200, 90),
                                "INFO" => Color32::from_rgb(110, 210, 140),
                                "DEBUG" => Color32::from_rgb(110, 180, 255),
                                _ => Color32::from_rgb(190, 145, 235),
                            };
                            append(&format!("{:>5} ", r.level), color);
                            append(
                                &format!(
                                    "client {} | port {} | request {} | {} {}: ",
                                    opt(r.client),
                                    opt(r.port),
                                    opt(r.request),
                                    r.command,
                                    r.stage,
                                ),
                                Color32::from_rgb(110, 180, 220),
                            );
                            append(
                                &r.message.replace('\n', " "),
                                Color32::from_rgb(220, 224, 230),
                            );
                            if ui
                                .selectable_label(
                                    self.selected.as_ref().is_some_and(|s| {
                                        s.timestamp == r.timestamp
                                            && s.request == r.request
                                            && s.message == r.message
                                    }),
                                    line,
                                )
                                .clicked()
                            {
                                self.selected = Some(r.clone());
                            }
                        }
                    });
            });
        if let Some(r) = &self.selected {
            let mut open = true;
            egui::Window::new("Record details")
                .open(&mut open)
                .default_width(600.)
                .vscroll(true)
                .show(ui.ctx(), |ui| {
                    let mut details = serde_json::to_string_pretty(r).unwrap();
                    if ui.button("Copy details").clicked() {
                        ui.ctx().copy_text(details.clone());
                    }
                    ui.add(
                        egui::TextEdit::multiline(&mut details)
                            .font(egui::TextStyle::Monospace)
                            .desired_width(f32::INFINITY)
                            .interactive(false),
                    );
                });
            if !open {
                self.selected = None;
            }
        }
    }
    fn load_archive(&mut self) {
        self.archive_active = true;
        let directory = self.logger.directory.clone();
        let offset = self.archive_offset;
        let filters = self.filters.clone();
        self.archive_job = Some(std::thread::spawn(move || {
            crate::logging::read_page(&directory, offset, |r| filters.matches(r))
                .map_err(|e| e.to_string())
        }));
    }
}
impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let model = self.handle.snapshots.borrow_and_update().clone();
        ctx.request_repaint_after(Duration::from_millis(250));
        egui::TopBottomPanel::top("controls").show(ctx, |ui| self.controls(ui, &model));
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.tab == Tab::Logs {
                self.logs(ui);
                return;
            }
            egui::ScrollArea::vertical().show(ui, |ui| match self.tab {
                Tab::Overview => self.overview(ui, &model),
                Tab::Clients => self.clients(ui, &model),
                Tab::Logs => self.logs(ui),
            });
        });
    }
}
fn field(ui: &mut egui::Ui, label: &str, value: &mut String, width: f32) {
    ui.label(label);
    ui.add(egui::TextEdit::singleline(value).desired_width(width));
}
fn opt<T: ToString>(value: Option<T>) -> String {
    value.map_or("—".into(), |v| v.to_string())
}
fn ms(value: Option<f64>) -> String {
    value.map_or("N/A".into(), |v| format!("{v:.2} ms"))
}
fn bytes(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{:.2} MiB", n as f64 / 1_048_576.)
    } else if n >= 1024 {
        format!("{:.1} KiB", n as f64 / 1024.)
    } else {
        format!("{n} B")
    }
}
fn rate(n: f64) -> String {
    let mut value = n;
    let mut unit = "B/s";
    for next in ["kB/s", "MB/s", "GB/s", "TB/s", "PB/s", "EB/s"] {
        if value.abs() < 1000. {
            break;
        }
        value /= 1000.;
        unit = next;
    }
    format!("{value:.2} {unit}")
}
fn state_color(state: &str) -> Color32 {
    match state {
        "Idle" => Color32::from_rgb(70, 170, 100),
        "Busy" => Color32::from_rgb(80, 150, 230),
        "Connecting" | "Retrying" => Color32::from_rgb(200, 150, 50),
        _ => Color32::GRAY,
    }
}
fn plot(
    ui: &mut egui::Ui,
    title: &str,
    history: &[&Sample],
    value: impl Fn(&Sample) -> f64,
    minimum: f64,
    unit: &str,
) {
    let max = history.iter().map(|s| value(s)).fold(minimum, f64::max);
    let format_value = |n: f64| {
        if unit == "B/s" {
            rate(n)
        } else {
            format!("{n:.1} {unit}")
        }
    };
    ui.label(format!(
        "{title} — {} (scale 0–{}; up to 15 min)",
        history
            .last()
            .map_or("N/A".into(), |s| format_value(value(s))),
        format_value(max)
    ));
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width().max(100.), 72.),
        egui::Sense::hover(),
    );
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 2., ui.visuals().extreme_bg_color);
    let start = history.first().map_or(0., |s| s.seconds);
    let end = history.last().map_or(1., |s| s.seconds).max(start + 1.);
    let points: Vec<_> = history
        .iter()
        .map(|s| {
            egui::pos2(
                rect.left() + ((s.seconds - start) / (end - start)) as f32 * rect.width(),
                rect.bottom() - (value(s) / max) as f32 * rect.height(),
            )
        })
        .collect();
    if points.len() > 1 {
        painter.add(egui::Shape::line(
            points,
            egui::Stroke::new(1.5_f32, ui.visuals().selection.bg_fill),
        ));
    }
    if let Some(pos) = response.hover_pos() {
        let fraction = ((pos.x - rect.left()) / rect.width()).clamp(0., 1.) as f64;
        let target = start + fraction * (end - start);
        if let Some(sample) = history.iter().min_by(|a, b| {
            (a.seconds - target)
                .abs()
                .total_cmp(&(b.seconds - target).abs())
        }) {
            response.on_hover_text(format!(
                "{:.0}s since startup: {}",
                sample.seconds,
                format_value(value(sample))
            ));
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn log_filters_combine() {
        let r = Record {
            level: "ERROR".into(),
            command: "Scan.Start".into(),
            client: Some(12),
            port: Some(6501),
            message: "Truncated packet".into(),
            ..Default::default()
        };
        let mut f = Filters {
            level: 1,
            client: "12".into(),
            text: "TRUNCATED".into(),
            ..Default::default()
        };
        assert!(f.matches(&r));
        f.port = "6502".into();
        assert!(!f.matches(&r));
    }
}
