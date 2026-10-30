mod v5;
pub use v5::{SocksV5, SocksV5Error};

mod v4;
pub use v4::{SocksV4, SocksV4Error};

use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};

use hyper::rt::Read;

/// Failure while opening a transport or performing SOCKS negotiation.
#[derive(Debug)]
pub enum SocksError<C> {
    /// The underlying connector failed.
    Inner(C),
    /// SOCKS I/O failed.
    Io(std::io::Error),

    /// No acceptable target address was resolved.
    DnsFailure,
    /// The target has no host.
    MissingHost,
    /// The target has no port.
    MissingPort,

    /// A SOCKS4 refusal.
    V4(SocksV4Error),
    /// A SOCKS5 refusal.
    V5(SocksV5Error),

    /// The received reply is malformed or incomplete.
    Parsing(ParsingError),
    /// The outgoing request could not be serialized.
    Serialize(SerializeError),
    /// The received reply at an unsuccessful native read/parse exit.
    Response {
        /// The reply being read.
        stage: SocksReplyStage,
        /// Only the bytes consumed or examined for this reply.
        bytes: Bytes,
        /// Whether the reply was completely parsed.
        complete: bool,
        /// The original native error.
        error: Box<SocksError<C>>,
    },
}

/// Which SOCKS5 reply the native parser was reading.
#[derive(Debug, Clone, Copy)]
pub enum SocksReplyStage {
    /// Method negotiation (RFC 1928).
    Method,
    /// Username/password authentication (RFC 1929).
    Authentication,
    /// The SOCKS command reply (RFC 1928).
    Command,
}

impl<C> SocksError<C> {
    fn response(self, stage: SocksReplyStage, bytes: Bytes, complete: bool) -> Self {
        if bytes.is_empty() {
            return self;
        }
        Self::Response {
            stage,
            bytes,
            complete,
            error: Box::new(self),
        }
    }
    /// Borrow an underlying connector failure, if this is one.
    pub fn connection_error(&self) -> Option<&C> {
        match self {
            Self::Inner(error) => Some(error),
            _ => None,
        }
    }

    /// Extract an underlying connector failure, preserving other errors.
    ///
    /// # Errors
    /// Returns this error unchanged when it is not a connector failure.
    pub fn into_connection_error(self) -> Result<C, Self> {
        match self {
            Self::Inner(error) => Ok(error),
            other => Err(other),
        }
    }
}

#[derive(Debug)]
pub enum ParsingError {
    Incomplete,
    WouldOverflow,
    Other,
}

#[derive(Debug)]
pub enum SerializeError {
    WouldOverflow,
}

async fn read_message<T, M, C>(mut conn: &mut T, buf: &mut BytesMut) -> Result<M, SocksError<C>>
where
    T: Read + Unpin,
    M: for<'a, 'b> TryFrom<&'a mut &'b [u8], Error = ParsingError>,
{
    let mut tmp = [0; 513];

    loop {
        let mut view = &buf[..];
        match M::try_from(&mut view) {
            Ok(res) => {
                let consumed = buf.len() - view.len();
                buf.advance(consumed);
                return Ok(res);
            }
            Err(ParsingError::Incomplete) => {}
            Err(err) => return Err(err.into()),
        }
        // A previous read can contain several handshake responses. Parse the
        // retained bytes before waiting for more from a still-open connection.
        let n = crate::rt::read(&mut conn, &mut tmp).await?;
        if n == 0 {
            return Err(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected eof").into(),
            );
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

// Unlike read_message (also used by SOCKS4), this preserves the SOCKS5
// parser's exact consumed slice before the caller reduces its status.
async fn read_observed<T, M, C>(
    conn: &mut T,
    buf: &mut BytesMut,
    stage: SocksReplyStage,
) -> Result<(M, Bytes), SocksError<C>>
where
    T: Read + Unpin,
    M: for<'a, 'b> TryFrom<&'a mut &'b [u8], Error = ParsingError>,
{
    let mut tmp = [0; 513];
    loop {
        let mut view = &buf[..];
        match M::try_from(&mut view) {
            Ok(result) => {
                let consumed = buf.len() - view.len();
                return Ok((result, buf.split_to(consumed).freeze()));
            }
            Err(ParsingError::Incomplete) => {}
            Err(error) => {
                let consumed = buf.len() - view.len();
                return Err(SocksError::Parsing(error).response(
                    stage,
                    Bytes::copy_from_slice(&buf[..consumed]),
                    false,
                ));
            }
        }
        let n = crate::rt::read(conn, &mut tmp).await.map_err(|error| {
            SocksError::Io(error).response(stage, Bytes::copy_from_slice(buf), false)
        })?;
        if n == 0 {
            return Err(SocksError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            ))
            .response(stage, Bytes::copy_from_slice(buf), false));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

impl<C> std::fmt::Display for SocksError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SOCKS error: ")?;

        match self {
            Self::Inner(_) => f.write_str("failed to create underlying connection"),
            Self::Io(_) => f.write_str("io error during SOCKS handshake"),

            Self::DnsFailure => f.write_str("could not resolve to acceptable address type"),
            Self::MissingHost => f.write_str("missing destination host"),
            Self::MissingPort => f.write_str("missing destination port"),

            Self::Parsing(_) => f.write_str("failed parsing server response"),
            Self::Serialize(_) => f.write_str("failed serialize request"),

            Self::V4(e) => e.fmt(f),
            Self::V5(e) => e.fmt(f),
            Self::Response { error, .. } => error.fmt(f),
        }
    }
}

// SOCKS services already require C::Error: 'static. The owned response
// wrapper needs that same lifetime to expose its inner error as a source.
impl<C: std::fmt::Debug + std::fmt::Display + 'static> std::error::Error for SocksError<C> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Response { error, .. } => Some(&**error),
            _ => None,
        }
    }
}

impl<C> From<std::io::Error> for SocksError<C> {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl<C> From<ParsingError> for SocksError<C> {
    fn from(err: ParsingError) -> Self {
        Self::Parsing(err)
    }
}

impl<C> From<SerializeError> for SocksError<C> {
    fn from(err: SerializeError) -> Self {
        Self::Serialize(err)
    }
}

impl<C> From<SocksV4Error> for SocksError<C> {
    fn from(err: SocksV4Error) -> Self {
        Self::V4(err)
    }
}

impl<C> From<SocksV5Error> for SocksError<C> {
    fn from(err: SocksV5Error) -> Self {
        Self::V5(err)
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
    pub struct Handshaking<F, T, E> {
        #[pin]
        fut: BoxHandshaking<T, E>,
        _marker: std::marker::PhantomData<F>
    }
}

type BoxHandshaking<T, E> = Pin<Box<dyn Future<Output = Result<T, SocksError<E>>> + Send>>;

impl<F, T, E> Future for Handshaking<F, T, E>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, SocksError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
    }
}

#[cfg(all(test, feature = "tokio"))]
mod test {
    use bytes::BytesMut;
    use tokio::io::AsyncWriteExt;

    use super::v5::messages::{ProxyRes, Status};
    use super::{read_message, SocksError};
    use crate::rt::TokioIo;

    // A SOCKS5 ProxyRes message. Successful, bound to 127.0.0.1:8080.
    const SEG1: [u8; 4] = [0x05, 0x00, 0x00, 0x01];
    const SEG2: [u8; 6] = [0x7F, 0x00, 0x00, 0x01, 0x1F, 0x90];

    #[tokio::test]
    async fn it_works_in_one_read() {
        let (client, mut server) = tokio::io::duplex(SEG1.len() + SEG2.len());
        server.write_all(&SEG1).await.unwrap();
        server.write_all(&SEG2).await.unwrap();

        let mut conn = TokioIo::new(client);
        let mut buf = BytesMut::new();

        let m: Result<ProxyRes, SocksError<()>> = read_message(&mut conn, &mut buf).await;
        assert!(m.is_ok());
        assert_eq!(m.unwrap(), ProxyRes(Status::Success));
    }

    #[tokio::test]
    async fn it_works_in_multiple_reads() {
        // Bounded stream ensures message arrives in two reads
        let (client, mut server) = tokio::io::duplex(SEG1.len());
        let _writer = tokio::spawn(async move {
            server.write_all(&SEG1).await.unwrap();
            server.write_all(&SEG2).await.unwrap();
        });

        let mut conn = TokioIo::new(client);
        let mut buf = BytesMut::new();

        let m: Result<ProxyRes, SocksError<()>> = read_message(&mut conn, &mut buf).await;
        assert!(m.is_ok());
        assert_eq!(m.unwrap(), ProxyRes(Status::Success));

        _writer.await.unwrap();
    }
}

#[cfg(test)]
mod parsing_boundary_tests {
    use super::*;
    use crate::rt::TokioIo;
    use v5::messages::{Address, AuthenticationRes, NegotiationRes, ProxyRes, Status};

    #[tokio::test]
    async fn every_reply_split_retains_the_prefix_until_complete() {
        let replies = [
            vec![5, 0, 0, 1, 127, 0, 0, 1, 0x1f, 0x90],
            [vec![5, 0, 0, 4], vec![0; 16], vec![0x1f, 0x90]].concat(),
            [vec![5, 0, 0, 3, 3], b"abc".to_vec(), vec![0x1f, 0x90]].concat(),
        ];
        for reply in replies {
            for cut in 1..reply.len() {
                let peer = tokio_test::io::Builder::new()
                    .read(&reply[..cut])
                    .read(&reply[cut..])
                    .build();
                let mut peer = TokioIo::new(peer);
                let mut buf = BytesMut::with_capacity(513);
                let result: Result<ProxyRes, SocksError<std::io::Error>> =
                    read_message(&mut peer, &mut buf).await;
                assert_eq!(result.unwrap().0, Status::Success);
                assert!(buf.is_empty());
            }
            let mut address = &reply[3..];
            if let Address::Domain(host, port) = Address::try_from(&mut address).unwrap() {
                assert_eq!(host, "abc");
                assert_eq!(port, 8080);
            }
        }
    }

    #[tokio::test]
    async fn coalesced_responses_are_parsed_without_another_socket_read() {
        let peer = tokio_test::io::Builder::new()
            .read(&[5, 2, 1, 0])
            .read_error(std::io::Error::other("another read must not be needed"))
            .build();
        let mut peer = TokioIo::new(peer);
        let mut buf = BytesMut::with_capacity(513);
        let first: Result<NegotiationRes, SocksError<std::io::Error>> =
            read_message(&mut peer, &mut buf).await;
        first.unwrap();
        assert_eq!(&buf[..], &[1, 0]);
        let second: Result<AuthenticationRes, SocksError<std::io::Error>> =
            read_message(&mut peer, &mut buf).await;
        assert!(second.unwrap().0);
        assert!(buf.is_empty());
    }
}

#[cfg(test)]
mod returned_source_tests {
    use super::*;
    #[derive(Debug)]
    struct Marker;
    impl std::fmt::Display for Marker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("typed transport failure")
        }
    }
    impl std::error::Error for Marker {}
    #[tokio::test]
    async fn a_partial_command_keeps_its_original_io_source_after_the_transport_drops() {
        use std::error::Error as _;
        let peer = tokio_test::io::Builder::new()
            .read(&[5, 0, 0, 1, 127])
            .read_error(std::io::Error::other(Marker))
            .build();
        let mut peer = crate::rt::TokioIo::new(peer);
        let result: Result<(v5::messages::ProxyRes, Bytes), SocksError<std::io::Error>> =
            read_observed(&mut peer, &mut BytesMut::new(), SocksReplyStage::Command).await;
        drop(peer);
        let error = result.err().unwrap();
        let mut cause = error.source();
        let io = loop {
            let current = cause.expect("the original I/O source survives its reply wrapper");
            if let Some(io) = current.downcast_ref::<std::io::Error>() {
                break io;
            }
            cause = current.source();
        };
        assert_eq!(io.kind(), std::io::ErrorKind::Other);
        assert!(io.get_ref().unwrap().is::<Marker>());
        match error {
            SocksError::Response {
                bytes, complete, ..
            } => {
                assert_eq!(bytes.as_ref(), &[5, 0, 0, 1, 127]);
                assert!(!complete);
            }
            other => panic!("lost the consumed command prefix: {other}"),
        }
    }
}
