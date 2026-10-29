use super::{
    dns::{GaiResolver, Resolve},
    http::{ConnectError, HttpConnecting},
    HttpConnector, OwnedTransport,
};
use crate::rt::TokioIo;
use http::Uri;
use pin_project_lite::pin_project;
use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tokio::net::TcpStream;
use tower_service::Service;

type Guard = Box<dyn Send>;
type BoxError = Box<dyn StdError + Send + Sync>;
type Admit = dyn Fn(&Uri) -> Result<Guard, BoxError> + Send + Sync;

/// Admission at a TCP socket opener, independent of pool ownership.
///
/// The callback runs once before each `HttpConnector` dial, before DNS or socket
/// creation. Its returned guard moves into the connected transport. Happy
/// Eyeballs remains inside that one admitted dial. A custom SSH dialer should
/// carry its channel memory instead of invoking socket admission.
#[derive(Clone)]
pub struct SocketAdmission(Arc<Admit>);
impl SocketAdmission {
    /// Create an admission callback returning an opaque transport lifetime guard.
    pub fn new<F, G, E>(admit: F) -> Self
    where
        F: Fn(&Uri) -> Result<G, E> + Send + Sync + 'static,
        G: Send + 'static,
        E: Into<BoxError>,
    {
        Self(Arc::new(move |uri| {
            admit(uri)
                .map(|guard| Box::new(guard) as Guard)
                .map_err(Into::into)
        }))
    }
}
impl fmt::Debug for SocketAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketAdmission").finish_non_exhaustive()
    }
}

/// Caller admission refused before starting network work, including DNS.
///
/// Its source is the original caller error. This marker allows route adapters
/// to stop address fallback on an admission refusal without inspecting text.
#[derive(Debug)]
pub struct SocketAdmissionRefused {
    cause: BoxError,
}
impl SocketAdmissionRefused {
    /// Preserve a caller's actual pre-network admission failure, including DNS
    /// query or result admission. Native DNS/connect errors are not refusals.
    pub fn new<E>(cause: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self { cause: cause.into() }
    }
}
impl fmt::Display for SocketAdmissionRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("socket admission refused")
    }
}
impl StdError for SocketAdmissionRefused {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// A configured TCP connector with socket-opener admission.
///
/// Configure the underlying [`HttpConnector`] before creating this adapter with
/// [`HttpConnector::with_socket_admission`]. Unadorned `HttpConnector` keeps its
/// upstream response type. Adapter extraction here returns an `OwnedTransport`.
#[derive(Clone)]
pub struct SocketConnector<R = GaiResolver> {
    http: HttpConnector<R>,
    admission: Option<SocketAdmission>,
}
impl<R: fmt::Debug> fmt::Debug for SocketConnector<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketConnector")
            .field("http", &self.http)
            .field("admission", &self.admission)
            .finish()
    }
}
impl<R> HttpConnector<R> {
    /// Attach optional socket-opener admission while preserving transport ownership.
    ///
    /// Each call gets a new guard; neither the connector nor the callback retains
    /// that guard. `None` disables admission. The connected `OwnedTransport` keeps
    /// its guard through subsequent TLS, proxy and adapter extraction.
    pub fn with_socket_admission(self, admission: Option<SocketAdmission>) -> SocketConnector<R> {
        SocketConnector {
            http: self,
            admission,
        }
    }
}
pin_project! {
    // Keep the dial future before the guard: cancellation destroys any sockets
    // still inside the future before returning the dial's admission.
    #[must_use = "futures do nothing unless polled"]
    pub struct SocketConnecting<R> {
        #[pin]
        inner: Option<HttpConnecting<R>>,
        guard: Option<Guard>,
        refusal: Option<ConnectError>,
    }
}
impl<R> fmt::Debug for SocketConnecting<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketConnecting").finish_non_exhaustive()
    }
}
impl<R> Service<Uri> for SocketConnector<R>
where
    R: Resolve + Clone + Send + Sync + 'static,
    R::Future: Send,
{
    type Response = TokioIo<OwnedTransport<TcpStream>>;
    type Error = ConnectError;
    type Future = SocketConnecting<R>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx)
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        let guard = self
            .admission
            .as_ref()
            .map(|admit| (admit.0)(&uri))
            .transpose();
        match guard {
            Ok(guard) => SocketConnecting {
                inner: Some(self.http.call(uri)),
                guard,
                refusal: None,
            },
            Err(cause) => SocketConnecting {
                inner: None,
                guard: None,
                refusal: Some(ConnectError::admission(SocketAdmissionRefused::new(cause))),
            },
        }
    }
}
impl<R: Resolve> Future for SocketConnecting<R> {
    type Output = Result<TokioIo<OwnedTransport<TcpStream>>, ConnectError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Some(refusal) = this.refusal.take() {
            return Poll::Ready(Err(refusal));
        }
        let result = ready!(this
            .inner
            .as_mut()
            .as_pin_mut()
            .expect("polled after completion")
            .poll(cx));
        // Release all state of a failed dial before its guard.
        this.inner.set(None);
        match result {
            Ok(io) => Poll::Ready(Ok(TokioIo::new(OwnedTransport::with_guard(
                io.into_inner(),
                this.guard.take(),
            )))),
            Err(error) => {
                drop(this.guard.take());
                Poll::Ready(Err(error))
            }
        }
    }
}
