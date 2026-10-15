#![allow(dead_code)]

use hyper::rt::Executor;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type BoxSendFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

// Infallible maintenance/upgrade tasks and fallible client tasks share the
// same result-bearing handoff. The public Executor trait remains generic.
pub(crate) trait TaskOutput {
    fn into_result(self) -> Result<(), BoxError>;
}
impl TaskOutput for () {
    fn into_result(self) -> Result<(), BoxError> {
        Ok(())
    }
}
impl<E: Into<BoxError>> TaskOutput for Result<(), E> {
    fn into_result(self) -> Result<(), BoxError> {
        self.map_err(Into::into)
    }
}

// Either the user provides an executor for background tasks, or we use
// `tokio::spawn`.
#[derive(Clone)]
pub(crate) enum Exec {
    Executor(Arc<dyn Executor<BoxSendFuture> + Send + Sync>),
}

// ===== impl Exec =====

impl Exec {
    pub(crate) fn new<E>(inner: E) -> Self
    where
        E: Executor<BoxSendFuture> + Send + Sync + 'static,
    {
        Exec::Executor(Arc::new(inner))
    }

    pub(crate) fn execute<F>(&self, fut: F)
    where
        F: Future + Send + 'static,
        F::Output: TaskOutput,
    {
        match *self {
            Exec::Executor(ref e) => {
                e.execute(Box::pin(async move { fut.await.into_result() }));
            }
        }
    }
}

impl fmt::Debug for Exec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exec").finish()
    }
}

impl<F> hyper::rt::Executor<F> for Exec
where
    F: Future + Send + 'static,
    F::Output: TaskOutput,
{
    fn execute(&self, fut: F) {
        Exec::execute(self, fut);
    }
}
