use http_body_util::Empty;
use hyper::{body::Bytes, Request};
use hyper_util::client::legacy::Client;
use std::future::Future;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
// Shared deterministic fixture: hold connection drivers outside the executor.

#[derive(Clone)]
struct Connector {
    http: hyper_util::client::legacy::connect::HttpConnector,
    connects: Arc<AtomicUsize>,
}
impl tower_service::Service<hyper::Uri> for Connector {
    type Response =
        <hyper_util::client::legacy::connect::HttpConnector as tower_service::Service<
            hyper::Uri,
        >>::Response;
    type Error = <hyper_util::client::legacy::connect::HttpConnector as tower_service::Service<
        hyper::Uri,
    >>::Error;
    type Future = <hyper_util::client::legacy::connect::HttpConnector as tower_service::Service<
        hyper::Uri,
    >>::Future;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tower_service::Service::poll_ready(&mut self.http, cx)
    }
    fn call(&mut self, uri: hyper::Uri) -> Self::Future {
        self.connects.fetch_add(1, Ordering::SeqCst);
        tower_service::Service::call(&mut self.http, uri)
    }
}

type Work = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
#[derive(Clone)]
struct Executor(tokio::sync::mpsc::UnboundedSender<Work>);
impl hyper::rt::Executor<Work> for Executor {
    fn execute(&self, future: Work) {
        self.0.send(future).unwrap();
    }
}

async fn drive<F: std::future::Future>(
    mut response: std::pin::Pin<&mut F>,
    work: &mut Vec<Work>,
    queue: &mut tokio::sync::mpsc::UnboundedReceiver<Work>,
) -> F::Output {
    futures_util::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(result) = response.as_mut().poll(cx) {
            return std::task::Poll::Ready(result);
        }
        while let std::task::Poll::Ready(Some(task)) = queue.poll_recv(cx) {
            work.push(task);
        }
        work.retain_mut(|task| task.as_mut().poll(cx).is_pending());
        std::task::Poll::Pending
    })
    .await
}

async fn peer(
    listener: tokio::net::TcpListener,
    requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut sockets = Vec::new();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(socket.read_u8().await.unwrap());
        }
        requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        sockets.push(socket);
    }
}

#[tokio::test]
async fn reused_connection_retry_is_optional_and_preserves_unstarted_fact() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for retry in [None, Some(false)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let server = tokio::spawn(peer(listener, requests.clone()));
            let connector = Connector {
                http: hyper_util::client::legacy::connect::HttpConnector::new(),
                connects: Arc::new(AtomicUsize::new(0)),
            };
            let dials = connector.connects.clone();
            let (tx, mut queue) = tokio::sync::mpsc::unbounded_channel();
            let mut builder = Client::builder(Executor(tx));
            builder.pool_idle_timeout(None);
            if let Some(retry) = retry {
                builder.retry_canceled_requests(retry);
            }
            let client = builder.build(connector);
            let request = || {
                Request::builder()
                    .uri(format!("http://{address}/"))
                    .body(Empty::<Bytes>::new())
                    .unwrap()
            };
            let mut work = Vec::new();
            let mut first = Box::pin(client.request(request()));
            assert_eq!(
                drive(first.as_mut(), &mut work, &mut queue)
                    .await
                    .unwrap()
                    .status(),
                200
            );

            futures_util::future::poll_fn(|cx| {
                while let std::task::Poll::Ready(Some(task)) = queue.poll_recv(cx) {
                    work.push(task);
                }
                work.retain_mut(|task| task.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(work.len(), 1, "one idle connection driver");

            let mut second = Box::pin(client.request(request()));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(second.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            work.clear();
            if retry == Some(false) {
                let error = second.await.unwrap_err();
                assert!(error.is_unstarted_on_reused_connection(), "{error:?}");
                assert_eq!(dials.load(Ordering::SeqCst), 1);
                assert_eq!(requests.load(Ordering::SeqCst), 1);
            } else {
                assert_eq!(
                    drive(second.as_mut(), &mut work, &mut queue)
                        .await
                        .unwrap()
                        .status(),
                    200
                );
                assert_eq!(dials.load(Ordering::SeqCst), 2);
                assert_eq!(requests.load(Ordering::SeqCst), 2);
            }
            work.clear();
            drop(client);
            server.abort();
            let _ = server.await;
        }
    })
    .await
    .unwrap();
}
