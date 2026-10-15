//! A readiness waiter either takes the connection error or leaves it to its task.
use futures_channel::oneshot;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

pub(super) struct Handoff {
    error: Arc<Mutex<Option<hyper::Error>>>,
    ready: oneshot::Receiver<()>,
    _release: oneshot::Sender<()>,
}

impl Handoff {
    pub(super) async fn take(self, fallback: hyper::Error) -> hyper::Error {
        let _ = self.ready.await;
        self.error.lock().unwrap().take().unwrap_or(fallback)
    }
}

pub(super) fn connection<F>(future: F) -> (impl Future<Output = hyper::Result<()>>, Handoff)
where
    F: Future<Output = hyper::Result<()>>,
{
    let error = Arc::new(Mutex::new(None));
    let pending = error.clone();
    let (ready, waiting) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let task = async move {
        if let Err(error) = future.await {
            *pending.lock().unwrap() = Some(error);
            let _ = ready.send(());
            // The waiter is local work: take, success or cancellation releases
            // it. A cancelled waiter cannot drop the only copy of the error.
            let _ = released.await;
            if let Some(error) = pending.lock().unwrap().take() {
                return Err(error);
            }
        }
        Ok(())
    };
    (
        task,
        Handoff {
            error,
            ready: waiting,
            _release: release,
        },
    )
}

#[cfg(all(test, feature = "http1", feature = "tokio"))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::Empty;
    use std::{io, task::Poll};

    async fn error() -> hyper::Error {
        let mock = tokio_test::io::Builder::new()
            .read_error(io::Error::new(io::ErrorKind::Other, "handoff-marker"))
            .build();
        let (_sender, conn) =
            hyper::client::conn::http1::handshake::<_, Empty<Bytes>>(crate::rt::TokioIo::new(mock))
                .await
                .unwrap();
        conn.await.unwrap_err()
    }

    #[tokio::test]
    async fn cancelled_waiter_leaves_the_published_error_to_its_task() {
        let error = error().await;
        let (task, handoff) = connection(async { Err(error) });
        let mut task = Box::pin(task);
        let polled = std::future::poll_fn(|cx| Poll::Ready(task.as_mut().poll(cx))).await;
        assert!(polled.is_pending()); // Error is published, waiter has not taken it.
        drop(handoff);
        let error = task.await.unwrap_err();
        assert!(format!("{error:?}").contains("handoff-marker"));
    }
}
