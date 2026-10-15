use bytes::Bytes;
use http_body_util::{Empty, StreamBody};
use hyper::{rt::Executor, Request, Uri};
use hyper_util::{
    client::legacy::{
        connect::{Connected, Connection},
        Client,
    },
    rt::TokioIo,
};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
trait Output {
    fn result(self) -> Option<Result<(), BoxError>>;
}
impl Output for () {
    fn result(self) -> Option<Result<(), BoxError>> {
        None
    }
}
impl<E: Into<BoxError>> Output for Result<(), E> {
    fn result(self) -> Option<Result<(), BoxError>> {
        Some(self.map_err(Into::into))
    }
}
#[derive(Clone)]
struct Recording {
    results: mpsc::UnboundedSender<Result<(), BoxError>>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    eager: bool,
}
impl<F> Executor<F> for Recording
where
    F: Future + Send + 'static,
    F::Output: Output + Send + 'static,
{
    fn execute(&self, future: F) {
        let mut future = Box::pin(future);
        if self.eager {
            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                if let Some(result) = output.result() {
                    self.results.send(result).unwrap();
                }
                return;
            }
        }
        let results = self.results.clone();
        self.tasks.lock().unwrap().push(tokio::spawn(async move {
            if let Some(result) = future.await.result() {
                results.send(result).unwrap();
            }
        }));
    }
}
impl Recording {
    fn new(eager: bool) -> (Self, mpsc::UnboundedReceiver<Result<(), BoxError>>) {
        let (results, receiver) = mpsc::unbounded_channel();
        (
            Self {
                results,
                tasks: Default::default(),
                eager,
            },
            receiver,
        )
    }
    async fn join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                task.await.unwrap();
            }
        }
    }
}
async fn next(results: &mut mpsc::UnboundedReceiver<Result<(), BoxError>>) -> Result<(), BoxError> {
    tokio::time::timeout(std::time::Duration::from_secs(10), results.recv())
        .await
        .expect("task did not publish")
        .expect("executor gone")
}
struct Io {
    stream: TokioIo<tokio::io::DuplexStream>,
    fault: Option<oneshot::Receiver<()>>,
    broken: bool,
}
impl Io {
    fn new(stream: tokio::io::DuplexStream) -> Self {
        Self {
            stream: TokioIo::new(stream),
            fault: None,
            broken: false,
        }
    }
}
impl Connection for Io {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}
impl hyper::rt::Read for Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(fault) = self.fault.as_mut() {
            if Pin::new(fault).poll(cx).is_ready() {
                self.broken = true;
                self.fault = None;
            }
        }
        if self.broken {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "connection-marker",
            )));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl hyper::rt::Write for Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
type Dial = Pin<Box<dyn Future<Output = io::Result<Io>> + Send>>;
#[derive(Clone)]
struct Connector(Arc<Mutex<VecDeque<Dial>>>);
impl Connector {
    fn new(dials: Vec<Dial>) -> Self {
        Self(Arc::new(Mutex::new(dials.into())))
    }
}
impl tower_service::Service<Uri> for Connector {
    type Response = Io;
    type Error = io::Error;
    type Future = Dial;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Uri) -> Dial {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected retry/dial")
    }
}
async fn read_head(peer: &mut tokio::io::DuplexStream) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let byte = peer.read_u8().await.unwrap();
        head.push(byte);
    }
}
async fn reply(peer: &mut tokio::io::DuplexStream) {
    peer.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
}

#[tokio::test]
async fn executor_reads_connection_error_after_success() {
    let (executor, mut results) = Recording::new(false);
    let (stream, mut peer) = tokio::io::duplex(4096);
    let (fail, fault) = oneshot::channel();
    let connector = Connector::new(vec![Box::pin(async {
        let mut io = Io::new(stream);
        io.fault = Some(fault);
        Ok(io)
    })]);
    let client: Client<_, Empty<Bytes>> = Client::builder(executor.clone()).build(connector);
    let server = tokio::spawn(async move {
        read_head(&mut peer).await;
        reply(&mut peer).await;
        peer
    });
    let response = client
        .get("http://fixture.test/".parse().unwrap())
        .await
        .unwrap();
    let peer = server.await.unwrap();
    fail.send(()).unwrap();
    let error = next(&mut results).await.unwrap_err();
    assert!(format!("{error:?}").contains("connection-marker"));
    assert_eq!(response.status(), 200);
    drop(client);
    drop(peer);
    executor.join().await;
}

#[tokio::test]
async fn request_takes_establishment_error_without_executor_duplicate() {
    let (executor, mut results) = Recording::new(true);
    let (stream, _peer) = tokio::io::duplex(4096);
    let connector = Connector::new(vec![Box::pin(async {
        let mut io = Io::new(stream);
        io.broken = true;
        Ok(io)
    })]);
    let client: Client<_, Empty<Bytes>> = Client::builder(executor.clone()).build(connector);
    let error = client
        .get("http://fixture.test/".parse().unwrap())
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("connection-marker"));
    drop(client);
    executor.join().await;
    while let Ok(result) = results.try_recv() {
        assert!(result.is_ok(), "duplicate: {result:?}");
    }
}

#[tokio::test]
async fn executor_reads_readiness_error_after_success() {
    let (executor, mut results) = Recording::new(false);
    let (stream, mut peer) = tokio::io::duplex(4096);
    let connector = Connector::new(vec![Box::pin(async { Ok(Io::new(stream)) })]);
    let client = Client::builder(executor.clone()).build(connector);
    let (frames, receiver) =
        futures_channel::mpsc::unbounded::<Result<hyper::body::Frame<Bytes>, io::Error>>();
    let server = tokio::spawn(async move {
        read_head(&mut peer).await;
        reply(&mut peer).await;
        peer
    });
    let response = client
        .request(
            Request::post("http://fixture.test/")
                .body(StreamBody::new(receiver))
                .unwrap(),
        )
        .await
        .unwrap();
    let peer = server.await.unwrap();
    frames
        .unbounded_send(Err(io::Error::new(io::ErrorKind::Other, "body-marker")))
        .unwrap();
    let mut errors = Vec::new();
    while errors.len() < 2 {
        if let Err(error) = next(&mut results).await {
            errors.push(format!("{error:?}"));
        }
    }
    assert!(
        errors.iter().any(|e| e.contains("body-marker")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|e| e.contains("ChannelClosed")),
        "{errors:?}"
    );
    assert_eq!(response.status(), 200);
    drop(client);
    drop(peer);
    executor.join().await;
}

#[tokio::test]
async fn executor_reads_losing_dial_error_without_rewriting_success() {
    let (executor, mut results) = Recording::new(false);
    let (stream, mut peer) = tokio::io::duplex(4096);
    let (dial_started, started) = oneshot::channel();
    let (fail_dial, failing) = oneshot::channel();
    let connector = Connector::new(vec![
        Box::pin(async { Ok(Io::new(stream)) }),
        Box::pin(async move {
            dial_started.send(()).unwrap();
            failing.await.unwrap();
            Err(io::Error::new(io::ErrorKind::Other, "losing-dial-marker"))
        }),
    ]);
    let client: Client<_, Empty<Bytes>> = Client::builder(executor.clone()).build(connector);
    let (first_seen, first_arrived) = oneshot::channel();
    let (reply_first, first_reply) = oneshot::channel();
    let server = tokio::spawn(async move {
        read_head(&mut peer).await;
        first_seen.send(()).unwrap();
        first_reply.await.unwrap();
        reply(&mut peer).await;
        read_head(&mut peer).await;
        reply(&mut peer).await;
        peer
    });
    let c = client.clone();
    let first =
        tokio::spawn(async move { c.get("http://fixture.test/one".parse().unwrap()).await });
    first_arrived.await.unwrap();
    let c = client.clone();
    let second =
        tokio::spawn(async move { c.get("http://fixture.test/two".parse().unwrap()).await });
    started.await.unwrap();
    reply_first.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap().status(), 200);
    let response = second.await.unwrap().unwrap();
    let peer = server.await.unwrap();
    fail_dial.send(()).unwrap();
    let error = next(&mut results).await.unwrap_err();
    assert!(format!("{error:?}").contains("losing-dial-marker"));
    assert_eq!(response.status(), 200);
    drop(client);
    drop(peer);
    executor.join().await;
}
