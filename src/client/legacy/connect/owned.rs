use super::{Connected, Connection};
use crate::common::SyncWrapper;
use hyper::rt::{Read, ReadBufCursor, Write};
use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

/// A transport that owns an opaque lifetime guard.
///
/// The I/O is destroyed before the guard. Adapter extraction must use [`Self::map`],
/// which transfers the guard with the extracted I/O rather than returning a bare
/// stream. There is deliberately no unguarded `into_inner` operation.
pub struct OwnedTransport<T> {
    inner: T,
    guard: Option<SyncWrapper<Box<dyn Send>>>,
}

impl<T> OwnedTransport<T> {
    /// Attach a caller-owned guard to a transport.
    pub fn new<G: Send + 'static>(inner: T, guard: G) -> Self {
        Self {
            inner,
            guard: Some(SyncWrapper::new(Box::new(guard))),
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn with_guard(inner: T, guard: Option<Box<dyn Send>>) -> Self {
        Self {
            inner,
            guard: guard.map(SyncWrapper::new),
        }
    }

    /// Borrow the transport for connection metadata and socket configuration.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Extract or change an adapter while transferring its guard to the result.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> OwnedTransport<U> {
        OwnedTransport {
            inner: map(self.inner),
            guard: self.guard,
        }
    }
}
impl<T: fmt::Debug> fmt::Debug for OwnedTransport<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedTransport")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
impl<T: Connection> Connection for OwnedTransport<T> {
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}
impl<T: Read + Unpin> Read for OwnedTransport<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}
impl<T: Write + Unpin> Write for OwnedTransport<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for OwnedTransport<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}
impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for OwnedTransport<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    struct Probe(&'static str, Arc<Mutex<Vec<&'static str>>>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.1.lock().unwrap().push(self.0);
        }
    }
    #[test]
    fn extraction_keeps_guard_until_io_is_destroyed() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let transport = OwnedTransport::new(
            crate::rt::TokioIo::new(Probe("io", order.clone())),
            Probe("guard", order.clone()),
        );
        let extracted = transport.map(crate::rt::TokioIo::into_inner);
        assert!(order.lock().unwrap().is_empty());
        drop(extracted);
        assert_eq!(*order.lock().unwrap(), ["io", "guard"]);
    }
}
