use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use super::Connected;

type BoxError = Box<dyn Error + Send + Sync>;
type AdmissionFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// Admit this request's selected connection before sending any HTTP bytes.
///
/// Insert this extension into a request to await a caller-owned check after
/// checkout or dialing. Refusal is unsent and non-retryable, retaining its
/// source. Dropping the request drops this wait and releases its ordinary pool
/// reservation. No admission or speculative losing dial is spawned: an idle
/// sender is used immediately, otherwise dialing stays in this request.
#[derive(Clone)]
pub struct ConnectionAdmission(Arc<dyn Fn(Connected) -> AdmissionFuture + Send + Sync>);

impl ConnectionAdmission {
    /// Make a per-request admission callback with the selected metadata.
    pub fn new<F, Fut, E>(admit: F) -> Self
    where
        F: Fn(Connected) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError>,
    {
        Self(Arc::new(move |connected| {
            let waiting = admit(connected);
            Box::pin(async move { waiting.await.map_err(Into::into) })
        }))
    }

    pub(in crate::client::legacy) async fn check(
        &self,
        connected: Connected,
    ) -> Result<(), BoxError> {
        (self.0)(connected).await
    }
}

impl fmt::Debug for ConnectionAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionAdmission")
            .finish_non_exhaustive()
    }
}
