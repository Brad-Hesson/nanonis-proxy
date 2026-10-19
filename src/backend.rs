//! Original proxy flow: a shared Kanal queue, one manager per upstream port,
//! and a oneshot that returns each client socket after forwarding a request.
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use color_eyre::{
    Result,
    eyre::{Context, ContextCompat},
};
use kanal::AsyncReceiver;
use nanonis_tcp::{CodecRead, CodecWrite};
use tokio::{
    io::{AsyncReadExt, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
};
use tracing::{error, info, trace, warn};

use crate::instrumentation::{Instrumentation, PendingRequest};
use crate::lifecycle::Session;

const HEADER_LEN: usize =
    nanonis_tcp::Header::CODEC_LEN.expect("nanonis_tcp::Header should be constant length");
const NANONIS_RETRY_PERIOD: Duration = Duration::from_secs(5);
static COMMAND_ID: AtomicU64 = AtomicU64::new(0);

pub async fn run(
    listener: TcpListener,
    host: String,
    observer: Instrumentation,
    session: Session,
) -> Result<()> {
    let (tx, rx) = kanal::bounded_async::<Request>(crate::telemetry::QUEUE_CAPACITY);
    // Retain task ownership so stopping the application also closes its sockets.
    let mut tasks = JoinSet::new();
    for port in 6501..=6504 {
        let host = host.clone();
        let observer = observer.clone();
        let rx = rx.clone();
        let session = session.clone();
        tasks.spawn(async move {
            nanonis_port_manager(rx, host, port, observer.clone(), session).await;
            observer.worker_state(port, "Disconnected");
            Ok(())
        });
    }
    loop {
        tokio::select! {
            accepted = session.wait(listener.accept()) => {
                let Ok(accepted) = accepted else { break; };
                let (stream, addr) = accepted?;
                tasks.spawn(client_handler(tx.clone(), stream, addr, observer.clone(), session.clone()));
            }
            result = tasks.join_next() => {
                if let Some(result) = result { result??; }
            }
        }
    }
    drop(listener);
    // Closing Kanal cancels queued and enqueue-blocked requests. Active ones
    // still own their sockets and may finish before the session returns.
    let _ = rx.close();
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

#[tracing::instrument(skip(request_tx, client, observer, session))]
async fn client_handler(
    request_tx: kanal::AsyncSender<Request>,
    mut client: TcpStream,
    client_addr: SocketAddr,
    observer: Instrumentation,
    session: Session,
) -> Result<()> {
    let client_metrics = observer.client(client_addr);
    info!("connected");
    let res = (async || -> Result<()> {
        loop {
            trace!("waiting for a command");
            if session
                .wait(stream_wait_for_data(&client))
                .await?
                .context("failed while waiting for client")?
                == StreamStatus::Closed
            {
                info!("disconnected gracefully");
                return Ok(());
            }
            let command_id = COMMAND_ID.fetch_add(1, Ordering::SeqCst);
            let span = tracing::info_span!(
                "request",
                nanonis_port = tracing::field::Empty,
                id = command_id,
                name = tracing::field::Empty,
            );
            let metrics = client_metrics.request(command_id);
            let (response_tx, response_rx) = oneshot::channel();
            let req = Request {
                client,
                response_tx,
                span: span.clone(),
                id: command_id,
                client_addr,
                metrics: metrics.clone(),
            };
            trace!(parent: &span, "sending to queue");
            session
                .wait(request_tx.send(req))
                .await?
                .context("failed to send client to queue")?;
            metrics.enqueued();
            trace!(parent: &span, "waiting for return from worker");
            let resp = response_rx
                .await
                .context("failed to get client back from queue")?;
            client = resp.client;
            client_metrics.idle();
        }
    })()
    .await;
    if let Err(e) = res
        && session.running()
    {
        error!("client connection failed: {e:#}");
        client_metrics.error(&format!("{e:#}"));
    }
    Ok(())
}

#[tracing::instrument(skip(request_rx, observer, session))]
async fn nanonis_port_manager(
    request_rx: AsyncReceiver<Request>,
    host: String,
    port: u16,
    observer: Instrumentation,
    session: Session,
) {
    while session.running() {
        let conn = loop {
            observer.worker_state(port, "Connecting");
            info!("trying to connect");
            let Ok(connection) = session
                .wait(TcpStream::connect((host.as_str(), port)))
                .await
            else {
                return;
            };
            match connection {
                Ok(conn) => break conn,
                Err(e) => {
                    warn!("failed to connect: {e}");
                    observer.connection_failed(port, &e.to_string());
                    if session
                        .wait(tokio::time::sleep(NANONIS_RETRY_PERIOD))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        };
        info!("connected");
        observer.connected(port);
        if let Err(e) =
            nanonis_conn_handler(request_rx.clone(), conn, port, &observer, &session).await
            && session.running()
        {
            error!("nanonis connection errored: {e:#}");
            observer.worker_error(port, &format!("{e:#}"));
        }
    }
}

async fn nanonis_conn_handler(
    request_rx: AsyncReceiver<Request>,
    mut conn: TcpStream,
    port: u16,
    observer: &Instrumentation,
    session: &Session,
) -> Result<()> {
    loop {
        observer.worker_state(port, "Idle");
        trace!("waiting for command");
        let Request {
            mut client,
            response_tx,
            span,
            id,
            client_addr,
            metrics,
        } = session
            .wait(request_rx.recv())
            .await?
            .context("failed to receive request from queue")?;
        span.record("nanonis_port", port);
        let mut transfer = metrics.started(port);
        let result = session
            .transfer(async {
                trace!(parent: &span, "reading request header");
                let req_header = read_header(&mut client)
                    .await
                    .context("failed to read request header from client")?;
                span.record("name", &*req_header.inner.name);
                transfer.command(&req_header.inner.name);
                trace!(
                    "command `[{}] {}` received from `{}`, working on it",
                    id, &*req_header.inner.name, client_addr
                );

                transfer.stage("request forwarding");
                forward_packet(
                    &req_header,
                    &mut client,
                    &mut transfer.writer(&mut conn, true),
                )
                .await
                .context("failed to forward packet from client to nanonis")?;
                if req_header.inner.response != 0 {
                    transfer.stage("response header");
                    let resp_header = read_header(&mut conn)
                        .await
                        .context("failed to read response header from nanonis")?;
                    transfer.stage("response forwarding");
                    forward_packet(
                        &resp_header,
                        &mut conn,
                        &mut transfer.writer(&mut client, false),
                    )
                    .await
                    .context("failed to forward packet from nanonis to client")?;
                }
                Ok(())
            })
            .await;
        transfer.finish(&result);
        result?;
        response_tx
            .send(Response { client })
            .ok()
            .context("failed to send stream back to client handler")?;
        trace!("command completed");
    }
}

async fn read_header(stream: &mut TcpStream) -> Result<Header> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut header_buf)
        .await
        .context("failed to receive header data")?;
    let header = nanonis_tcp::Header::codec_read(&mut header_buf.as_slice())
        .context("failed to parse header")?;
    Ok(Header {
        inner: header,
        buf: header_buf,
    })
}

async fn forward_packet(
    header: &Header,
    src: &mut TcpStream,
    dst: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let body_len = header
        .inner
        .body_len
        .try_into()
        .context("header body length value was negative")?;
    let mut request = std::io::Cursor::new(header.buf).chain(src.take(body_len));
    tokio::io::copy(&mut request, dst)
        .await
        .context("failed to copy packet")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamStatus {
    HasData,
    Closed,
}
async fn stream_wait_for_data(stream: &TcpStream) -> Result<StreamStatus> {
    match stream.peek(&mut [0u8; 1]).await? {
        0 => Ok(StreamStatus::Closed),
        _ => Ok(StreamStatus::HasData),
    }
}

struct Request {
    id: u64,
    client_addr: SocketAddr,
    client: TcpStream,
    response_tx: oneshot::Sender<Response>,
    span: tracing::Span,
    metrics: PendingRequest,
}
struct Response {
    client: TcpStream,
}
struct Header {
    inner: nanonis_tcp::Header,
    buf: [u8; HEADER_LEN],
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;
