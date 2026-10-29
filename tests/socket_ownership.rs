#![cfg(all(feature = "client-legacy", feature = "tokio"))]
use hyper_util::client::legacy::connect::{HttpConnector, SocketAdmission, SocketAdmissionRefused};
use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower_service::Service;
struct Guard {
    live: Arc<AtomicUsize>,
    _send_only: Cell<()>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct Resolver {
    calls: Arc<AtomicUsize>,
    port: Option<u16>,
}
impl Service<hyper_util::client::legacy::connect::dns::Name> for Resolver {
    type Response = std::vec::IntoIter<std::net::SocketAddr>;
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(match self.port {
            Some(port) => Ok(vec![
                std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], port)),
                std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            ]
            .into_iter()),
            None => Err(std::io::Error::from(std::io::ErrorKind::NotFound)),
        })
    }
}

struct Resolution {
    addresses: std::vec::IntoIter<std::net::SocketAddr>,
    _guard: Guard,
}
impl Iterator for Resolution {
    type Item = std::net::SocketAddr;
    fn next(&mut self) -> Option<Self::Item> {
        self.addresses.next()
    }
}
#[derive(Clone)]
struct OwnedResolver {
    address: std::net::SocketAddr,
    live: Arc<AtomicUsize>,
}
impl Service<hyper_util::client::legacy::connect::dns::Name> for OwnedResolver {
    type Response = Resolution;
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<Resolution, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        self.live.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Resolution {
            addresses: vec![self.address].into_iter(),
            _guard: Guard {
                live: self.live.clone(),
                _send_only: Cell::new(()),
            },
        }))
    }
}

#[tokio::test]
async fn pending_native_dial_keeps_the_collected_resolution_until_cancelled() {
    use std::future::Future;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    let mut connector = HttpConnector::new_with_resolver(OwnedResolver {
        address: listener.local_addr().unwrap(),
        live: live.clone(),
    });
    let mut connecting = Box::pin(connector.call("http://owned.test/".parse().unwrap()));
    // The resolver is immediately ready. Its consuming iterator has been
    // collected when the actual nonblocking TCP dial first yields.
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(connecting.as_mut().poll(&mut cx).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 1);
    drop(connecting);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn socket_permit_survives_adapter_extraction_until_physical_close() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    let seen = live.clone();
    let admit = SocketAdmission::new(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(Guard {
            live: seen.clone(),
            _send_only: Cell::new(()),
        })
    });
    let mut connector = HttpConnector::new().with_socket_admission(Some(admit));
    let uri = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let transport = connector.call(uri).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 1);
    let mut transport = transport.into_inner(); // the owning TCP transport survives.
    drop(connector);
    transport.write_all(b"x").await.unwrap();
    assert_eq!(peer.read_u8().await.unwrap(), b'x');
    assert_eq!(
        live.load(Ordering::SeqCst),
        1,
        "dropping adapters does not return admission"
    );
    drop(transport);
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn refused_socket_dial_preserves_typed_cause_before_dns() {
    let called = Arc::new(AtomicUsize::new(0));
    let seen = called.clone();
    let admit = SocketAdmission::new(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        Err::<(), _>(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
    });
    let dns = Arc::new(AtomicUsize::new(0));
    let mut connector = HttpConnector::new_with_resolver(Resolver {
        calls: dns.clone(),
        port: None,
    })
    .with_socket_admission(Some(admit));
    let error = connector
        .call("http://must-not-resolve.invalid/".parse().unwrap())
        .await
        .unwrap_err();
    let refusal = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<SocketAdmissionRefused>()
        .unwrap();
    let original = std::error::Error::source(refusal)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(original.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(dns.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn happy_eyeballs_keeps_one_dial_admission() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let live = Arc::new(AtomicUsize::new(0));
    let held = live.clone();
    let admission = SocketAdmission::new(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        held.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(Guard {
            live: held.clone(),
            _send_only: Cell::new(()),
        })
    });
    let dns = Arc::new(AtomicUsize::new(0));
    let mut http = HttpConnector::new_with_resolver(Resolver {
        calls: dns.clone(),
        port: Some(listener.local_addr().unwrap().port()),
    });
    http.set_happy_eyeballs_timeout(Some(std::time::Duration::ZERO));
    let mut connector = http.with_socket_admission(Some(admission));
    let io = connector
        .call("http://dual.test/".parse().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    assert_eq!(dns.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    drop(io);
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "client-proxy")]
#[tokio::test]
async fn plain_connect_tunnel_keeps_the_socket_owner_after_adapter_extraction() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(io.read_u8().await.unwrap());
        }
        assert!(head.starts_with(b"CONNECT origin.test:80 "));
        io.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await.unwrap();
        assert_eq!(io.read_u8().await.unwrap(), b'x');
        assert_eq!(io.read(&mut [0]).await.unwrap(), 0);
    });
    let live = Arc::new(AtomicUsize::new(0));
    let held = live.clone();
    let admission = SocketAdmission::new(move |_| {
        held.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(Guard {
            live: held.clone(),
            _send_only: Cell::new(()),
        })
    });
    let connector = HttpConnector::new().with_socket_admission(Some(admission));
    let mut tunnel = hyper_util::client::legacy::connect::proxy::Tunnel::new(
        format!("http://{address}/").parse().unwrap(),
        connector,
    );
    let io = tunnel
        .call("http://origin.test:80/".parse().unwrap())
        .await
        .unwrap();
    let mut io = io.into_inner();
    drop(tunnel);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    io.write_all(b"x").await.unwrap();
    drop(io);
    server.await.unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}
