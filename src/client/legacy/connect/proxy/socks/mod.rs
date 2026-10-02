mod v5;
pub use v5::{SocksV5, SocksV5Error};

mod v4;
pub use v4::{SocksV4, SocksV4Error};

use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};

use hyper::rt::Read;

#[derive(Debug)]
pub enum SocksError<C> {
    Inner(C),
    Io(std::io::Error),
    /// Authority rejected before a credential-bearing write.
    Guard(Box<dyn std::error::Error + Send + Sync>),

    DnsFailure,
    MissingHost,
    MissingPort,

    V4(SocksV4Error),
    V5(SocksV5Error),

    Parsing(ParsingError),
    Serialize(SerializeError),
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

impl<C> std::fmt::Display for SocksError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SOCKS error: ")?;

        match self {
            Self::Inner(_) => f.write_str("failed to create underlying connection"),
            Self::Io(_) => f.write_str("io error during SOCKS handshake"),
            Self::Guard(_) => f.write_str("SOCKS authority rejected"),

            Self::DnsFailure => f.write_str("could not resolve to acceptable address type"),
            Self::MissingHost => f.write_str("missing destination host"),
            Self::MissingPort => f.write_str("missing destination port"),

            Self::Parsing(_) => f.write_str("failed parsing server response"),
            Self::Serialize(_) => f.write_str("failed serialize request"),

            Self::V4(e) => e.fmt(f),
            Self::V5(e) => e.fmt(f),
        }
    }
}

impl<C: std::fmt::Debug + std::fmt::Display> std::error::Error for SocksError<C> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Guard(error) => Some(&**error),
            Self::Io(error) => Some(error),
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
