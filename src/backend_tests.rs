use super::*;
use crate::{
    logging::Logger,
    telemetry::{Model, Phase},
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncWriteExt, sync::watch};

fn packet(body: &[u8], response: bool) -> Vec<u8> {
    let mut bytes = vec![0; HEADER_LEN];
    bytes[..11].copy_from_slice(b"Test.Packet");
    bytes[32..36].copy_from_slice(&(body.len() as i32).to_be_bytes());
    bytes[36..38].copy_from_slice(&u16::from(response).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}
async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    (client, listener.accept().await.unwrap().0)
}
fn observer() -> (Instrumentation, std::thread::JoinHandle<()>, PathBuf) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "nanonis-restored-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let (logger, thread) = Logger::start(dir.clone());
    (
        Instrumentation {
            logger,
            model: Arc::new(Mutex::new(Model::new(
                "127.0.0.1".into(),
                crate::config::DEFAULT_PORTS,
            ))),
        },
        thread,
        dir,
    )
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition timed out");
}

#[tokio::test]
async fn original_copy_streams_before_body_finishes_and_preserves_packet_boundary() {
    let (observer, thread, dir) = observer();
    let (mut user, mut client) = pair().await;
    let client_metrics = observer.client(client.peer_addr().unwrap());
    let mut transfer = client_metrics.request(1).started(6501);
    let (mut conn, mut upstream) = pair().await;
    let mut first = packet(b"", false);
    first[32..36].copy_from_slice(&32768_i32.to_be_bytes());
    user.write_all(&first).await.unwrap();
    let header = read_header(&mut client).await.unwrap();
    let forwarding = async {
        forward_packet(&header, &mut client, &mut transfer.writer(&mut conn, true))
            .await
            .unwrap();
        transfer.finish(&Ok(()));
        let next = read_header(&mut client).await.unwrap();
        assert_eq!(next.inner.body_len, 0);
    };
    let peers = async {
        // A reader must see this first chunk while the rest of the body is withheld.
        user.write_all(&vec![42; 1024]).await.unwrap();
        let mut prefix = vec![0; HEADER_LEN + 1024];
        tokio::time::timeout(Duration::from_secs(1), upstream.read_exact(&mut prefix))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&prefix[..HEADER_LEN], &first);
        assert!(prefix[HEADER_LEN..].iter().all(|b| *b == 42));
        assert_eq!(
            observer.model.lock().unwrap().total.up,
            (HEADER_LEN + 1024) as u64
        );
        user.write_all(&vec![42; 32768 - 1024]).await.unwrap();
        user.write_all(&packet(b"", false)).await.unwrap();
        let mut rest = vec![0; 32768 - 1024];
        upstream.read_exact(&mut rest).await.unwrap();
        assert!(rest.iter().all(|b| *b == 42));
    };
    tokio::join!(forwarding, peers);
    assert_eq!(
        observer.model.lock().unwrap().total.up,
        (HEADER_LEN + 32768) as u64
    );
    drop(transfer);
    drop(client_metrics);
    drop(observer);
    thread.join().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn original_handlers_forward_opaque_responses_and_response_free_requests() {
    let (observer, thread, dir) = observer();
    let (mode, rx_mode) = watch::channel(Phase::Running);
    let session = Session::new(rx_mode);
    let (tx, rx) = kanal::bounded_async(32);
    let (mut user, client) = pair().await;
    let address = client.peer_addr().unwrap();
    let handler = tokio::spawn(client_handler(
        tx,
        client,
        address,
        observer.clone(),
        session.clone(),
    ));
    let (conn, mut upstream) = pair().await;
    let worker_observer = observer.clone();
    let worker = tokio::spawn(async move {
        nanonis_conn_handler(rx, conn, 6501, &worker_observer, &session).await
    });
    user.write_all(&packet(b"request", true)).await.unwrap();
    let mut request = vec![0; HEADER_LEN + 7];
    upstream.read_exact(&mut request).await.unwrap();
    assert_eq!(request, packet(b"request", true));
    let reply = packet(&[0, 1, 2, 255], false);
    upstream.write_all(&reply).await.unwrap();
    let mut received = vec![0; reply.len()];
    user.read_exact(&mut received).await.unwrap();
    assert_eq!(received, reply);
    user.write_all(&packet(b"no reply", false)).await.unwrap();
    let mut request = vec![0; HEADER_LEN + 8];
    upstream.read_exact(&mut request).await.unwrap();
    assert_eq!(request, packet(b"no reply", false));
    until(|| observer.model.lock().unwrap().total.completed == 2).await;
    {
        let model = observer.model.lock().unwrap();
        assert_eq!(model.total.up, (HEADER_LEN * 2 + 15) as u64);
        assert_eq!(model.total.down, reply.len() as u64);
        assert_eq!(model.total.failed, 0);
        assert_eq!(model.timings.len(), 2);
    }
    mode.send_replace(Phase::Draining);
    handler.await.unwrap().unwrap();
    assert!(worker.await.unwrap().is_err());
    assert!(observer.model.lock().unwrap().clients.is_empty());
    drop(observer);
    thread.join().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn kanal_four_workers_backpressure_and_graceful_drain() {
    let (observer, thread, dir) = observer();
    let (mode, rx_mode) = watch::channel(Phase::Running);
    let session = Session::new(rx_mode);
    let (tx, rx) = kanal::bounded_async(32);
    let mut workers = JoinSet::new();
    let mut servers = Vec::new();
    for port in 6501..=6504 {
        let (conn, upstream) = pair().await;
        let rx = rx.clone();
        let observer = observer.clone();
        let session = session.clone();
        workers
            .spawn(async move { nanonis_conn_handler(rx, conn, port, &observer, &session).await });
        servers.push(upstream);
    }
    let mut clients = JoinSet::new();
    let mut users = Vec::new();
    for _ in 0..40 {
        let (mut user, client) = pair().await;
        let address = client.peer_addr().unwrap();
        clients.spawn(client_handler(
            tx.clone(),
            client,
            address,
            observer.clone(),
            session.clone(),
        ));
        user.write_all(&packet(b"", true)).await.unwrap();
        users.push(user);
    }
    until(|| {
        let m = observer.model.lock().unwrap();
        m.queue_depth() == 32 && m.blocked() == 4 && m.workers.iter().all(|w| w.state == "Busy")
    })
    .await;
    mode.send_replace(Phase::Draining);
    rx.close().unwrap();
    for server in &mut servers {
        let mut header = [0; HEADER_LEN];
        server.read_exact(&mut header).await.unwrap();
        server.write_all(&packet(b"ok", false)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(result) = workers.join_next().await {
            assert!(result.unwrap().is_err());
        }
        while let Some(result) = clients.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
    assert!(observer.model.lock().unwrap().clients.is_empty());
    assert_eq!(observer.model.lock().unwrap().total.completed, 4);
    drop(observer);
    thread.join().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn forced_disconnect_counts_partial_transfer_and_closes_client() {
    let (observer, thread, dir) = observer();
    let (mode, rx_mode) = watch::channel(Phase::Running);
    let session = Session::new(rx_mode);
    let (tx, rx) = kanal::bounded_async(32);
    let (mut user, client) = pair().await;
    let address = client.peer_addr().unwrap();
    let handler = tokio::spawn(client_handler(
        tx,
        client,
        address,
        observer.clone(),
        session.clone(),
    ));
    let (conn, mut upstream) = pair().await;
    let worker_observer = observer.clone();
    let worker = tokio::spawn(async move {
        nanonis_conn_handler(rx, conn, 6501, &worker_observer, &session).await
    });
    user.write_all(&packet(b"", true)).await.unwrap();
    let mut header = [0; HEADER_LEN];
    upstream.read_exact(&mut header).await.unwrap();
    mode.send_replace(Phase::Force);
    assert!(worker.await.unwrap().is_err());
    handler.await.unwrap().unwrap();
    assert_eq!(user.read(&mut [0]).await.unwrap(), 0);
    {
        let model = observer.model.lock().unwrap();
        assert_eq!(model.total.failed, 1);
        assert_eq!(model.total.up, HEADER_LEN as u64);
        assert_eq!(model.total.down, 0);
    }
    assert!(
        observer
            .logger
            .state
            .lock()
            .unwrap()
            .records
            .iter()
            .any(|r| r.stage == "response header"
                && r.up_bytes == HEADER_LEN as u64
                && r.message.contains("forced shutdown"))
    );
    drop(observer);
    thread.join().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn counted_writer_retains_partial_bytes_when_copy_fails() {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    struct FailingWriter {
        remaining: usize,
    }
    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.remaining == 0 {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            let n = bytes.len().min(this.remaining);
            this.remaining -= n;
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let (observer, thread, dir) = observer();
    let client_metrics = observer.client("127.0.0.1:1234".parse().unwrap());
    let mut transfer = client_metrics.request(1).started(6501);
    transfer.stage("request forwarding");
    let mut writer = FailingWriter { remaining: 3 };
    let result = tokio::io::copy(
        &mut &b"payload"[..],
        &mut transfer.writer(&mut writer, true),
    )
    .await
    .map(|_| ())
    .map_err(Into::into);
    assert!(result.is_err());
    transfer.finish(&result);
    {
        let model = observer.model.lock().unwrap();
        assert_eq!(model.total.up, 3);
        assert_eq!(model.workers[0].counters.up, 3);
        assert_eq!(model.clients.values().next().unwrap().counters.up, 3);
        assert_eq!(model.total.failed, 1);
    }
    let error = observer
        .logger
        .state
        .lock()
        .unwrap()
        .records
        .back()
        .unwrap()
        .clone();
    assert_eq!(error.up_bytes, 3);
    assert_eq!(error.stage, "request forwarding");
    drop(transfer);
    drop(client_metrics);
    drop(observer);
    thread.join().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn configured_ports_connect_and_report_the_correct_workers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (observer, thread, dir) = observer();
        let mut upstream_listeners = Vec::new();
        for _ in 0..4 {
            upstream_listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let ports = std::array::from_fn(|i| upstream_listeners[i].local_addr().unwrap().port());
        *observer.model.lock().unwrap() = Model::new("localhost".into(), ports);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (mode, rx_mode) = watch::channel(Phase::Running);
        let proxy = tokio::spawn(run(
            listener,
            "localhost".into(),
            ports,
            observer.clone(),
            Session::new(rx_mode),
        ));
        let mut upstreams = Vec::new();
        for listener in upstream_listeners {
            upstreams.push(listener.accept().await.unwrap().0);
        }
        let mut users = Vec::new();
        for _ in 0..4 {
            let mut user = TcpStream::connect(address).await.unwrap();
            user.write_all(&packet(b"x", true)).await.unwrap();
            users.push(user);
        }
        until(|| {
            observer
                .model
                .lock()
                .unwrap()
                .workers
                .iter()
                .all(|w| w.state == "Busy")
        })
        .await;
        {
            let model = observer.model.lock().unwrap();
            assert_eq!(model.ports(), ports);
            for port in ports {
                assert!(model.clients.values().any(|c| c.port == Some(port)));
            }
        }
        for upstream in &mut upstreams {
            let mut request = vec![0; HEADER_LEN + 1];
            upstream.read_exact(&mut request).await.unwrap();
            assert_eq!(request, packet(b"x", true));
            upstream.write_all(&packet(b"ok", false)).await.unwrap();
        }
        for user in &mut users {
            let mut response = vec![0; HEADER_LEN + 2];
            user.read_exact(&mut response).await.unwrap();
            assert_eq!(response, packet(b"ok", false));
        }
        mode.send_replace(Phase::Draining);
        proxy.await.unwrap().unwrap();
        {
            let model = observer.model.lock().unwrap();
            assert_eq!(model.total.completed, 4);
            assert!(model.clients.is_empty());
            for (worker, port) in model.workers.iter().zip(ports) {
                assert_eq!(worker.port, port);
                assert_eq!(worker.counters.completed, 1);
                assert_eq!(worker.counters.up, (HEADER_LEN + 1) as u64);
                assert_eq!(worker.counters.down, (HEADER_LEN + 2) as u64);
                assert_eq!(worker.state, "Disconnected");
            }
        }
        drop(observer);
        thread.join().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    })
    .await
    .expect("custom-port proxy timed out");
}
