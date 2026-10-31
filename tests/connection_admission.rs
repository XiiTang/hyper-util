#![cfg(all(
    feature = "client-legacy",
    feature = "http1",
    feature = "http2",
    feature = "server",
    feature = "tokio"
))]
use futures_util::FutureExt;
use http_body_util::{BodyExt, Empty, Full};
use hyper::{body::Bytes, Request};
use hyper_util::{
    client::legacy::{
        connect::{ConnectionAdmission, HttpConnector, HttpInfo},
        Client,
    },
    rt::{TokioExecutor, TokioIo},
};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::{mpsc, Notify};

#[derive(Clone)]
struct Connector {
    http: HttpConnector,
    dials: Arc<AtomicUsize>,
    second: Option<Arc<Notify>>,
    dropped: Arc<AtomicUsize>,
}
struct Waiting(Arc<AtomicUsize>);
impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl tower_service::Service<hyper::Uri> for Connector {
    type Response = TokioIo<tokio::net::TcpStream>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, uri: hyper::Uri) -> Self::Future {
        let n = self.dials.fetch_add(1, Ordering::SeqCst);
        let mut http = self.http.clone();
        let gate = self.second.clone().filter(|_| n == 1);
        let dropped = self.dropped.clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _waiting = Waiting(dropped);
                gate.notified().await;
            }
            Ok(tower_service::Service::call(&mut http, uri).await?)
        })
    }
}
fn connector(second: Option<Arc<Notify>>) -> Connector {
    Connector {
        http: HttpConnector::new(),
        dials: Arc::default(),
        second,
        dropped: Arc::default(),
    }
}
fn request(
    address: std::net::SocketAddr,
    path: &str,
    admission: ConnectionAdmission,
) -> Request<Empty<Bytes>> {
    let mut request = Request::builder()
        .uri(format!("http://{address}{path}"))
        .body(Empty::new())
        .unwrap();
    request.extensions_mut().insert(admission);
    request
}
fn allow() -> ConnectionAdmission {
    ConnectionAdmission::new(|_| async { Ok::<_, std::io::Error>(()) })
}
async fn until(mut predicate: impl FnMut() -> bool) {
    while !predicate() {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn admission_wait_refusal_and_cancellation_leave_a_reused_sender_usable() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut seen) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                        tx.send(req.uri().path().to_owned()).unwrap();
                        async { Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::new()))) }
                    }),
                )
                .await
                .unwrap();
        });
        let connector = connector(None);
        let dials = connector.dials.clone();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        client
            .request(request(address, "/warm", allow()))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(seen.recv().await.unwrap(), "/warm");
        let entered = Arc::new(AtomicUsize::new(0));
        let count = entered.clone();
        let mut waiting = Box::pin(client.request(request(
            address,
            "/cancel",
            ConnectionAdmission::new(move |info| {
                let count = count.clone();
                async move {
                    let mut extras = http::Extensions::new();
                    info.get_extras(&mut extras);
                    assert_eq!(extras.get::<HttpInfo>().unwrap().remote_addr(), address);
                    count.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<Result<(), std::io::Error>>().await
                }
            }),
        )));
        assert!(waiting.as_mut().now_or_never().is_none());
        tokio::select! {
            _ = waiting.as_mut() => panic!("admission must still be waiting"),
            _ = until(|| entered.load(Ordering::SeqCst) == 1) => {},
        }
        drop(waiting);
        let error = client
            .request(request(
                address,
                "/refused",
                ConnectionAdmission::new(|_| async {
                    Err::<(), _>(std::io::Error::other("caller admission refused"))
                }),
            ))
            .await
            .unwrap_err();
        assert!(std::error::Error::source(&error)
            .unwrap()
            .to_string()
            .contains("caller admission refused"));
        client
            .request(request(address, "/after", allow()))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(seen.recv().await.unwrap(), "/after");
        assert_eq!(dials.load(Ordering::SeqCst), 1);
        drop(client);
        peer.abort();
        let _ = peer.await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn admission_dial_does_not_lose_to_a_later_pool_return_or_continue_after_cancellation() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut seen) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let released = release.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                        tx.send(req.uri().path().to_owned()).unwrap();
                        let released = released.clone();
                        async move {
                            if req.uri().path() == "/held" {
                                released.notified().await;
                            }
                            Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::new())))
                        }
                    }),
                )
                .await
                .unwrap();
        });
        let gate = Arc::new(Notify::new());
        let connector = connector(Some(gate.clone()));
        let (dials, dropped) = (connector.dials.clone(), connector.dropped.clone());
        let client = Client::builder(TokioExecutor::new()).build(connector);
        let mut held = Box::pin(client.request(request(address, "/held", allow())));
        {
            let first = async {
                assert_eq!(seen.recv().await.unwrap(), "/held");
            };
            tokio::pin!(first);
            assert!(matches!(
                futures_util::future::select(held.as_mut(), first).await,
                futures_util::future::Either::Right(_)
            ));
        }
        let mut second = Box::pin(client.request(request(address, "/unused", allow())));
        assert!(second.as_mut().now_or_never().is_none());
        assert_eq!(dials.load(Ordering::SeqCst), 2);
        release.notify_one();
        held.await.unwrap().into_body().collect().await.unwrap();
        // The now idle first socket cannot win the second request's acquiring dial.
        assert!(second.as_mut().now_or_never().is_none());
        drop(second);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        gate.notify_one();
        client
            .request(request(address, "/after", allow()))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(seen.recv().await.unwrap(), "/after");
        assert_eq!(dials.load(Ordering::SeqCst), 2);
        drop(client);
        peer.abort();
        let _ = peer.await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn http2_each_stream_admits_its_own_connection_and_refusal_does_not_end_other_streams() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut seen) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(stream),
                    hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                        tx.send(req.uri().path().to_owned()).unwrap();
                        async { Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::new()))) }
                    }),
                )
                .await
                .unwrap();
        });
        let connector = connector(None);
        let dials = connector.dials.clone();
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build(connector);
        let (first, second) = tokio::join!(
            client.request(request(address, "/warm-one", allow())),
            client.request(request(address, "/warm-two", allow())),
        );
        first.unwrap().into_body().collect().await.unwrap();
        second.unwrap().into_body().collect().await.unwrap();
        let mut paths = vec![seen.recv().await.unwrap(), seen.recv().await.unwrap()];
        paths.sort();
        assert_eq!(paths, ["/warm-one", "/warm-two"]);
        let release = Arc::new(Notify::new());
        let waiting = release.clone();
        let mut refused = Box::pin(client.request(request(
            address,
            "/refuse",
            ConnectionAdmission::new(move |_| {
                let waiting = waiting.clone();
                async move {
                    waiting.notified().await;
                    Err::<(), _>(std::io::Error::other("refused stream"))
                }
            }),
        )));
        assert!(refused.as_mut().now_or_never().is_none());
        client
            .request(request(address, "/other", allow()))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(seen.recv().await.unwrap(), "/other");
        release.notify_one();
        assert!(refused.await.is_err());
        client
            .request(request(address, "/after", allow()))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(seen.recv().await.unwrap(), "/after");
        assert_eq!(dials.load(Ordering::SeqCst), 1);
        drop(client);
        peer.abort();
        let _ = peer.await;
    })
    .await
    .unwrap();
}
