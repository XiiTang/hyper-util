use std::error::Error as StdError;
use std::future::Future;
use std::marker::{PhantomData, Unpin};
use std::pin::Pin;
use std::task::{self, ready, Poll};

use http::{HeaderMap, HeaderValue, Uri};
use hyper::rt::{Read, Write};
use pin_project_lite::pin_project;
use tower_service::Service;

/// Tunnel Proxy via HTTP CONNECT
///
/// This is a connector that can be used by the `legacy::Client`. It wraps
/// another connector, and after getting an underlying connection, it creates
/// an HTTP CONNECT tunnel over it.
#[derive(Debug, Clone)]
pub struct Tunnel<C> {
    headers: Headers,
    inner: C,
    proxy_dst: Uri,
}

#[derive(Clone, Debug)]
enum Headers {
    Empty,
    Auth(HeaderValue),
    Extra(HeaderMap),
}

/// Failure while opening a transport or establishing its CONNECT tunnel.
#[derive(Debug)]
pub enum TunnelError {
    /// The underlying connector failed.
    ConnectFailed(Box<dyn StdError + Send + Sync>),
    /// Tunnel I/O failed.
    Io(std::io::Error),
    /// The target has no host.
    MissingHost,
    /// The proxy requires authentication.
    ProxyAuthRequired,
    /// The received head exceeds the native bound.
    ProxyHeadersTooLong,
    /// The transport ended before a complete head.
    TunnelUnexpectedEof,
    /// The proxy refused the tunnel or sent an invalid head.
    TunnelUnsuccessful,
    /// The received head at an unsuccessful native read/parse exit.
    Response {
        /// The bytes already received for this head.
        head: bytes::Bytes,
        /// The status actually parsed, if any.
        status: Option<u16>,
        /// Whether the complete head boundary was parsed.
        head_complete: bool,
        /// The original native error.
        error: Box<TunnelError>,
    },
}

impl TunnelError {
    fn response(self, head: &[u8], status: Option<u16>, head_complete: bool) -> Self {
        if head.is_empty() {
            return self;
        }
        Self::Response {
            head: bytes::Bytes::copy_from_slice(head),
            status,
            head_complete,
            error: Box::new(self),
        }
    }
}

pin_project! {
    // Not publicly exported (so missing_docs doesn't trigger).
    //
    // We return this `Future` instead of the `Pin<Box<dyn Future>>` directly
    // so that users don't rely on it fitting in a `Pin<Box<dyn Future>>` slot
    // (and thus we can change the type in the future).
    #[must_use = "futures do nothing unless polled"]
    #[allow(missing_debug_implementations)]
    pub struct Tunneling<F, T> {
        #[pin]
        fut: BoxTunneling<T>,
        _marker: PhantomData<F>,
    }
}

type BoxTunneling<T> = Pin<Box<dyn Future<Output = Result<T, TunnelError>> + Send>>;

impl<C> Tunnel<C> {
    /// Create a new Tunnel service.
    ///
    /// This wraps an underlying connector, and stores the address of a
    /// tunneling proxy server.
    ///
    /// A `Tunnel` can then be called with any destination. The `dst` passed to
    /// `call` will not be used to create the underlying connection, but will
    /// be used in an HTTP CONNECT request sent to the proxy destination.
    pub fn new(proxy_dst: Uri, connector: C) -> Self {
        Self {
            headers: Headers::Empty,
            inner: connector,
            proxy_dst,
        }
    }

    /// Add `proxy-authorization` header value to the CONNECT request.
    pub fn with_auth(mut self, mut auth: HeaderValue) -> Self {
        // just in case the user forgot
        auth.set_sensitive(true);
        match self.headers {
            Headers::Empty => {
                self.headers = Headers::Auth(auth);
            }
            Headers::Auth(ref mut existing) => {
                *existing = auth;
            }
            Headers::Extra(ref mut extra) => {
                extra.insert(http::header::PROXY_AUTHORIZATION, auth);
            }
        }

        self
    }

    /// Add extra headers to be sent with the CONNECT request.
    ///
    /// If existing headers have been set, these will be merged.
    pub fn with_headers(mut self, mut headers: HeaderMap) -> Self {
        match self.headers {
            Headers::Empty => {
                self.headers = Headers::Extra(headers);
            }
            Headers::Auth(auth) => {
                headers
                    .entry(http::header::PROXY_AUTHORIZATION)
                    .or_insert(auth);
                self.headers = Headers::Extra(headers);
            }
            Headers::Extra(ref mut extra) => {
                extra.extend(headers);
            }
        }

        self
    }
}

impl<C> Service<Uri> for Tunnel<C>
where
    C: Service<Uri>,
    C::Future: Send + 'static,
    C::Response: Read + Write + Unpin + Send + 'static,
    C::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Response = C::Response;
    type Error = TunnelError;
    type Future = Tunneling<C::Future, C::Response>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.inner.poll_ready(cx)).map_err(|e| TunnelError::ConnectFailed(e.into()))?;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let connecting = self.inner.call(self.proxy_dst.clone());
        let headers = self.headers.clone();

        Tunneling {
            fut: Box::pin(async move {
                let conn = connecting
                    .await
                    .map_err(|e| TunnelError::ConnectFailed(e.into()))?;
                tunnel(
                    conn,
                    dst.host().ok_or(TunnelError::MissingHost)?,
                    dst.port().map(|p| p.as_u16()).unwrap_or(443),
                    &headers,
                )
                .await
            }),
            _marker: PhantomData,
        }
    }
}

impl<F, T, E> Future for Tunneling<F, T>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, TunnelError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
    }
}

async fn tunnel<T>(mut conn: T, host: &str, port: u16, headers: &Headers) -> Result<T, TunnelError>
where
    T: Read + Write + Unpin,
{
    let mut buf = format!(
        "\
         CONNECT {host}:{port} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         "
    )
    .into_bytes();

    match headers {
        Headers::Auth(auth) => {
            buf.extend_from_slice(b"Proxy-Authorization: ");
            buf.extend_from_slice(auth.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        Headers::Extra(extra) => {
            for (name, value) in extra {
                buf.extend_from_slice(name.as_str().as_bytes());
                buf.extend_from_slice(b": ");
                buf.extend_from_slice(value.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        Headers::Empty => (),
    }

    // headers end
    buf.extend_from_slice(b"\r\n");

    crate::rt::write_all(&mut conn, &buf)
        .await
        .map_err(TunnelError::Io)?;

    let mut buf = [0; 8192];
    let mut pos = 0;
    let mut start = 0;

    loop {
        if pos == buf.len() {
            return Err(TunnelError::ProxyHeadersTooLong.response(&buf[start..pos], None, false));
        }
        // The generic connector has no unread operation. Reading one byte here
        // leaves every tunneled byte in the original transport.
        let n = crate::rt::read(&mut conn, &mut buf[pos..pos + 1])
            .await
            .map_err(|error| TunnelError::Io(error).response(&buf[start..pos], None, false))?;
        if n == 0 {
            return Err(TunnelError::TunnelUnexpectedEof.response(&buf[start..pos], None, false));
        }
        pos += n;
        let mut headers = [httparse::EMPTY_HEADER; 128];
        let mut response = httparse::Response::new(&mut headers);
        let status = response
            .parse(&buf[start..pos])
            .map_err(|_| TunnelError::TunnelUnsuccessful.response(&buf[start..pos], None, false))?;
        // Upstream reports a known refusal without waiting for the remaining
        // headers. Partial informational/success heads still need their boundary.
        match response.code {
            Some(407) => {
                return Err(TunnelError::ProxyAuthRequired.response(
                    &buf[start..pos],
                    response.code,
                    status.is_complete(),
                ))
            }
            Some(100 | 102..=299) | None => {}
            Some(_) => {
                return Err(TunnelError::TunnelUnsuccessful.response(
                    &buf[start..pos],
                    response.code,
                    status.is_complete(),
                ))
            }
        }
        if status.is_partial() {
            continue;
        }
        if matches!(response.code, Some(100..=199)) && response.code != Some(101) {
            start = pos;
            continue;
        }
        return match response.code {
            Some(200..=299) => Ok(conn),
            Some(407) => {
                Err(TunnelError::ProxyAuthRequired.response(&buf[start..pos], response.code, true))
            }
            _ => {
                Err(TunnelError::TunnelUnsuccessful.response(&buf[start..pos], response.code, true))
            }
        };
    }
}

impl std::fmt::Display for TunnelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Self::Response { error, .. } = self {
            return error.fmt(f);
        }
        f.write_str("tunnel error: ")?;

        f.write_str(match self {
            TunnelError::MissingHost => "missing destination host",
            TunnelError::ProxyAuthRequired => "proxy authorization required",
            TunnelError::ProxyHeadersTooLong => "proxy response headers too long",
            TunnelError::TunnelUnexpectedEof => "unexpected end of file",
            TunnelError::TunnelUnsuccessful => "unsuccessful",
            TunnelError::ConnectFailed(_) => "failed to create underlying connection",
            TunnelError::Io(_) => "io error establishing tunnel",
            TunnelError::Response { .. } => unreachable!(),
        })
    }
}

impl std::error::Error for TunnelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TunnelError::Io(ref e) => Some(e),
            TunnelError::ConnectFailed(ref e) => Some(&**e),
            TunnelError::Response { error, .. } => Some(&**error),
            _ => None,
        }
    }
}
