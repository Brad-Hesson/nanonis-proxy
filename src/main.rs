use std::{net::SocketAddr, sync::atomic::AtomicU64, time::Duration};

use color_eyre::{
    Result,
    eyre::{Context, ContextCompat},
};
use kanal::AsyncReceiver;
use nanonis_tcp::{CodecRead, CodecWrite};
use tokio::{io::AsyncReadExt, net::TcpStream, sync::oneshot};
use tracing::{error, info, trace, warn};
use tracing_subscriber::EnvFilter;

const HEADER_LEN: usize =
    nanonis_tcp::Header::CODEC_LEN.expect("nanonis_tcp::Header should be constant length");

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let tcp = tokio::net::TcpListener::bind("localhost:6500")
        .await
        .unwrap();
    let (tx, rx) = kanal::bounded_async::<Request>(32);
    for port in 6501..=6504 {
        tokio::spawn(nanonis_port_manager(rx.clone(), port));
    }
    loop {
        let (stream, addr) = tcp.accept().await.unwrap();
        tokio::spawn(client_handler(tx.clone(), stream, addr));
    }
}

static COMMAND_ID: AtomicU64 = AtomicU64::new(0);

#[tracing::instrument(skip(request_tx, client))]
async fn client_handler(
    request_tx: kanal::AsyncSender<Request>,
    mut client: TcpStream,
    client_addr: SocketAddr,
) -> Result<()> {
    info!("connected");
    let res = (async || -> Result<()> {
        loop {
            trace!("waiting for a command");
            if stream_wait_for_data(&client)
                .await
                .context("failed while waiting for client")?
                == StreamStatus::Closed
            {
                info!("disconnected gracefully");
                return Ok(());
            }
            trace!("received command");
            let command_id = COMMAND_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let span = tracing::info_span!(
                "request",
                nanonis_port = tracing::field::Empty,
                id = command_id,
                name = tracing::field::Empty,
            );
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let req = Request {
                client,
                response_tx,
                span: span.clone(),
                id: command_id,
                client_addr,
            };
            trace!(parent: &span, "sending to queue");
            request_tx
                .send(req)
                .await
                .context("failed to send client to queue")
                .inspect_err(|e| error!("{e:#}"))?;
            trace!(parent: &span, "waiting for return from worker");
            let resp = response_rx
                .await
                .context("failed to get client back from queue")
                .inspect_err(|e| error!("{e:#}"))?;
            trace!(parent: &span, "stream returned from worker");
            client = resp.client;
        }
    })()
    .await;
    if let Err(e) = res {
        error!("client connection failed: {e:#}");
    }
    Ok(())
}

const NANONIS_RETRY_PERIOD: Duration = Duration::from_secs(5);

#[tracing::instrument(skip(request_rx))]
async fn nanonis_port_manager(request_rx: AsyncReceiver<Request>, port: u16) {
    loop {
        let conn = loop {
            info!("trying to connect");
            match tokio::net::TcpStream::connect(("localhost", port)).await {
                Ok(conn) => break conn,
                Err(e) => {
                    warn!("failed to connect: {e}");
                    info!("retrying in {} seconds", NANONIS_RETRY_PERIOD.as_secs_f64());
                    tokio::time::sleep(Duration::from_secs(5)).await
                }
            };
        };
        info!("connected");
        if let Err(e) = nanonis_conn_handler(request_rx.clone(), conn, port).await {
            error!("nanonis connection errored: {e:#}");
        }
    }
}

async fn nanonis_conn_handler(
    request_rx: AsyncReceiver<Request>,
    mut conn: TcpStream,
    port: u16,
) -> Result<()> {
    loop {
        trace!("waiting for command");
        let Request {
            mut client,
            response_tx,
            span,
            id,
            client_addr,
        } = request_rx
            .recv()
            .await
            .context("failed to receive request from queue")?;
        span.record("nanonis_port", port);

        trace!(parent: &span, "reading request header");
        let req_header = read_header(&mut client)
            .await
            .context("failed to read request header from client")
            .inspect_err(|e| error!(parent: &span, "{e:#}"))?;
        let command_name = &*req_header.inner.name;
        span.record("name", command_name);

        trace!(
            "command `[{}] {}` received from `{}`, working on it",
            id, command_name, client_addr
        );

        trace!(parent: &span, "forwarding request");
        forward_packet(&req_header, &mut client, &mut conn)
            .await
            .context("failed to forward packet from client to nanonis")
            .inspect_err(|e| error!(parent: &span, "{e:#}"))?;

        if req_header.inner.response != 0 {
            trace!(parent: &span, "reading response header");
            let resp_header = read_header(&mut conn)
                .await
                .context("failed to read response header from nanonis")?;

            trace!(parent: &span, "forwarding response");
            forward_packet(&resp_header, &mut conn, &mut client)
                .await
                .context("failed to forward packet from nanonis to client")
                .inspect_err(|e| error!(parent: &span, "{e:#}"))?;
        }
        trace!(parent: &span, "returning to client handler");
        response_tx
            .send(Response { client })
            .ok()
            .context("failed to send stream back to client handler")
            .inspect_err(|e| error!(parent: &span, "{e:#}"))?;
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

async fn forward_packet(header: &Header, src: &mut TcpStream, dst: &mut TcpStream) -> Result<()> {
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
    client: TcpStream,
    client_addr: SocketAddr,
    response_tx: oneshot::Sender<Response>,
    span: tracing::Span,
}

struct Response {
    client: TcpStream,
}

struct Header {
    inner: nanonis_tcp::Header,
    buf: [u8; HEADER_LEN],
}
