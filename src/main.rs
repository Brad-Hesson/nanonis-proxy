use color_eyre::{Result, eyre::Context};
use kanal::AsyncReceiver;
use nanonis_tcp::{CodecRead, Header};
use tokio::{io::AsyncReadExt, net::TcpStream, sync::oneshot};
use tracing::info;

const HEADER_LEN: usize = 40;

#[tokio::main]
async fn main() {
    let tcp = tokio::net::TcpListener::bind("0.0.0.0:6500").await.unwrap();
    let (tx, rx) = kanal::bounded_async::<Request>(32);
    for port in 6501..=6504 {
        let tcp = tokio::net::TcpStream::connect(("0.0.0.0", port))
            .await
            .unwrap();
        tokio::spawn(conn_handler(rx.clone(), tcp));
    }
    loop {
        let (stream, _addr) = tcp.accept().await.unwrap();
        tokio::spawn(client_handler(tx.clone(), stream));
    }
}

async fn client_handler(
    request_tx: kanal::AsyncSender<Request>,
    mut client: TcpStream,
) -> Result<()> {
    loop {
        match stream_status(&client)
            .await
            .context("failed while waiting for client")?
        {
            StreamStatus::HasData => {}
            StreamStatus::Closed => {
                info!("Client disconnected");
                return Ok(());
            }
        };

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let req = Request {
            client,
            response_tx,
        };
        request_tx
            .send(req)
            .await
            .context("failed to send client to queue")?;
        let resp = response_rx
            .await
            .context("failed to get client back from queue")?;
        client = resp.client;
    }
}

async fn conn_handler(request_rx: AsyncReceiver<Request>, mut conn: TcpStream) -> Result<()> {
    loop {
        let Request {
            mut client,
            response_tx,
        } = request_rx
            .recv()
            .await
            .context("failed to receive request from queue")?;

        let header = forward_packet(&mut client, &mut conn)
            .await
            .context("failed to forward packet from client to nanonis")?;

        if header.response != 0 {
            forward_packet(&mut conn, &mut client)
                .await
                .context("failed to forward packet from nanonis to client")?;
        }

        response_tx.send(Response { client }).ok();
    }
}

async fn read_header(stream: &mut TcpStream) -> Result<(Header, [u8; HEADER_LEN])> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut header_buf)
        .await
        .context("failed to read header")?;
    let header =
        Header::codec_read(&mut header_buf.as_slice()).context("failed to parse header")?;
    Ok((header, header_buf))
}

async fn forward_packet(src: &mut TcpStream, dst: &mut TcpStream) -> Result<Header> {
    let (header, header_buf) = read_header(src).await?;
    let body_stream = src.take(header.body_len as u64);
    let mut request = std::io::Cursor::new(header_buf).chain(body_stream);
    tokio::io::copy(&mut request, dst)
        .await
        .context("failed to copy packet")?;
    Ok(header)
}

enum StreamStatus {
    HasData,
    Closed,
}
async fn stream_status(stream: &TcpStream) -> Result<StreamStatus> {
    match stream.peek(&mut [0u8; 1]).await? {
        0 => Ok(StreamStatus::Closed),
        _ => Ok(StreamStatus::HasData),
    }
}

struct Request {
    client: TcpStream,
    response_tx: oneshot::Sender<Response>,
}

struct Response {
    client: TcpStream,
}
