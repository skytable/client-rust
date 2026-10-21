/*
 * Copyright 2023, Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

//! # Asynchronous database I/O
//!
//! This module provides the necessary items to establish an asynchronous connection to the database server. If you need
//! to use connection pooling, consider checking the [`pool`](crate::pool) module.
//!
//! See the [`crate`] root documentation for help on establishing and using database connections.

use {
    crate::{
        error::{ClientResult, ConnectionSetupError, Error},
        protocol::{
            handshake::{ClientHandshake, ServerHandshake},
            DecodeState, Decoder, MRespState, PipelineResult, RState,
        },
        query::Pipeline,
        response::{FromResponse, Response},
        Config, Query,
    },
    native_tls::Certificate,
    std::ops::{Deref, DerefMut},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
    tokio_native_tls::{TlsConnector, TlsStream},
};

#[derive(Debug)]
/// An async `skyhash/TCP` connection
///
/// **Specification**
/// - Protocol version: `Skyhash/2.0`
/// - Query mode: `QTDEX-1A/BQL-S1`
/// - Authentication plugin: `pwd`
pub struct ConnectionAsync(TcpConnection<TcpStream>);
#[derive(Debug)]
/// An async `skyhash/TLS` connection
///
/// **Specification**
/// - Protocol version: `Skyhash/2.0`
/// - Query mode: `QTDEX-1A/BQL-S1`
/// - Authentication plugin: `pwd`
pub struct ConnectionTlsAsync(TcpConnection<TlsStream<TcpStream>>);

impl Deref for ConnectionAsync {
    type Target = TcpConnection<TcpStream>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for ConnectionAsync {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Deref for ConnectionTlsAsync {
    type Target = TcpConnection<TlsStream<TcpStream>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for ConnectionTlsAsync {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Config {
    /// Establish an async connection to the database using the current configuration
    pub async fn connect_async(&self) -> ClientResult<ConnectionAsync> {
        TcpStream::connect((self.host(), self.port()))
            .await
            .map(TcpConnection::new)?
            ._handshake(self)
            .await
            .map(ConnectionAsync)
    }
    /// Establish an async TLS connection to the database using the current configuration.
    /// Pass the certificate in PEM format.
    pub async fn connect_tls_async(&self, cert: &str) -> ClientResult<ConnectionTlsAsync> {
        let stream = TcpStream::connect((self.host(), self.port())).await?;
        // set up acceptor
        let mut builder = native_tls::TlsConnector::builder();
        builder
            .add_root_certificate(Certificate::from_pem(cert.as_bytes()).map_err(|e| {
                ConnectionSetupError::Other(format!("failed to parse certificate: {e}"))
            })?)
            .danger_accept_invalid_hostnames(true)
            .build()
            .map_err(|e| {
                ConnectionSetupError::Other(format!("failed to set up TLS acceptor: {e}"))
            })?;
        let connector = builder.build().map_err(|e| {
            ConnectionSetupError::Other(format!("failed to set up TLS acceptor: {e}"))
        })?;
        // init and handshake
        TlsConnector::from(connector)
            .connect(self.host(), stream)
            .await
            .map(TcpConnection::new)
            .map_err(|e| ConnectionSetupError::Other(format!("TLS handshake failed: {e}")))?
            ._handshake(self)
            .await
            .map(ConnectionTlsAsync)
    }
}

#[derive(Debug)]
/// The underlying socket type
pub struct TcpConnection<C: AsyncWriteExt + AsyncReadExt + Unpin> {
    con: C,
    buf: Vec<u8>,
    /// Set when a query or pipeline future was dropped before it consumed the whole
    /// reply. The stream position is then unknown; every later operation on
    /// this connection fails instead of reading another request's reply.
    poisoned: bool,
}

/// Sets `poisoned` on drop unless the exchange completed. This covers cancellation
/// at any await point (including a partial write) and I/O/protocol errors, after
/// which the next byte boundary is unknown.
struct PoisonOnDrop<'a> {
    poisoned: &'a mut bool,
    armed: bool,
}

impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.poisoned = true;
        }
    }
}

impl<C: AsyncWriteExt + AsyncReadExt + Unpin> TcpConnection<C> {
    fn new(con: C) -> Self {
        Self {
            con,
            buf: Vec::with_capacity(crate::BUFSIZE),
            poisoned: false,
        }
    }
    /// Whether a previous operation was cancelled or failed before its reply
    /// was fully consumed. Such a connection must be discarded: the pool does
    /// this in `has_broken`, while a direct user must reconnect.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    fn poisoned_error() -> Error {
        Error::IoError(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "skytable: connection poisoned by an incomplete operation",
        ))
    }
    async fn _handshake(mut self, cfg: &Config) -> ClientResult<Self> {
        let handshake = ClientHandshake::new(cfg);
        self.con.write_all(handshake.inner()).await?;
        let mut resp = [0u8; 4];
        self.con.read_exact(&mut resp).await?;
        match ServerHandshake::parse(resp)? {
            ServerHandshake::Error(e) => return Err(ConnectionSetupError::HandshakeError(e).into()),
            ServerHandshake::Okay(_suggestion) => return Ok(self),
        }
    }
    /// Execute a pipeline. The server returns the queries in the order they were sent (unless otherwise set).
    ///
    /// If the returned future is dropped after it has been polled, the
    /// connection is poisoned: the pipeline may have been written but its
    /// reply was not consumed. Later operations return `ConnectionAborted`.
    /// Drop the connection and reconnect, or use a pool, which discards
    /// poisoned connections.
    pub async fn execute_pipeline(&mut self, pipeline: &Pipeline) -> ClientResult<Vec<Response>> {
        if self.poisoned {
            return Err(Self::poisoned_error());
        }
        let Self { con, buf, poisoned } = self;
        let mut guard = PoisonOnDrop {
            poisoned,
            armed: true,
        };
        buf.clear();
        buf.push(b'P');
        // packet size
        buf.extend(itoa::Buffer::new().format(pipeline.buf().len()).as_bytes());
        buf.push(b'\n');
        // write
        con.write_all(buf).await?;
        con.write_all(pipeline.buf()).await?;
        buf.clear();
        // read
        let mut cursor = 0;
        let mut state = MRespState::default();
        loop {
            let mut chunk = [0u8; crate::BUFSIZE];
            let n = con.read(&mut chunk).await?;
            if n == 0 {
                return Err(Error::IoError(std::io::ErrorKind::ConnectionReset.into()));
            }
            buf.extend_from_slice(&chunk[..n]);
            let (_state, _position) =
                Decoder::new(buf, cursor).validate_pipe(pipeline.query_count(), state);
            match _state {
                PipelineResult::Completed(r) => {
                    guard.armed = false;
                    return Ok(r);
                }
                PipelineResult::Pending(_state) => {
                    cursor = _position;
                    state = _state;
                }
                PipelineResult::Error(e) => return Err(e.into()),
            }
        }
    }
    /// Run a query and return a raw [`Response`]
    ///
    /// If the returned future is dropped after it has been polled, the
    /// connection is poisoned: the query may have been written but its reply
    /// was not consumed. Later operations return `ConnectionAborted`. Drop the
    /// connection and reconnect, or use a pool, which discards poisoned
    /// connections.
    pub async fn query(&mut self, q: &Query) -> ClientResult<Response> {
        if self.poisoned {
            return Err(Self::poisoned_error());
        }
        // Split the borrow so the guard can watch `poisoned` while the body
        // uses the socket and the response buffer.
        let Self { con, buf, poisoned } = self;
        let mut guard = PoisonOnDrop {
            poisoned,
            armed: true,
        };
        buf.clear();
        q.write_packet(buf).unwrap();
        con.write_all(buf).await?;
        buf.clear();
        let mut state = RState::default();
        let mut cursor = 0;
        let mut expected = Decoder::MIN_READBACK;
        loop {
            let mut chunk = [0u8; crate::BUFSIZE];
            let n = con.read(&mut chunk).await?;
            if n == 0 {
                return Err(Error::IoError(std::io::ErrorKind::ConnectionReset.into()));
            }
            if n < expected {
                continue;
            }
            buf.extend_from_slice(&chunk[..n]);
            let (_state, _position) = Decoder::new(buf, cursor).validate_response(state);
            match _state {
                DecodeState::Completed(resp) => {
                    guard.armed = false;
                    return Ok(resp);
                }
                DecodeState::ChangeState(_state) => {
                    expected = 1;
                    state = _state;
                    cursor = _position;
                }
                DecodeState::Error(e) => return Err(Error::ProtocolError(e)),
            }
        }
    }
    /// Run and parse a query into the indicated type. The type must implement [`FromResponse`]
    pub async fn query_parse<T: FromResponse>(&mut self, q: &Query) -> ClientResult<T> {
        self.query(q).await.and_then(FromResponse::from_response)
    }
    /// Call this if the internally allocated buffer is growing too large and impacting your performance. However, normally
    /// you will not need to call this
    pub fn reset_buffer(&mut self) {
        self.buf.shrink_to_fit()
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use std::{
        future::{poll_fn, Future},
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    struct PendingReply(Arc<AtomicUsize>);

    /// Accepts at most `limit` bytes and then stays pending, so the query
    /// future is cancelled after a partial write.
    struct PartialWrite {
        writes: Arc<AtomicUsize>,
        accepted: usize,
        limit: usize,
    }
    impl AsyncRead for PartialWrite {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for PartialWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.accepted >= self.limit {
                return Poll::Pending;
            }
            let n = bytes.len().min(self.limit - self.accepted);
            self.accepted += n;
            self.writes.fetch_add(n, Ordering::SeqCst);
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    impl AsyncRead for PendingReply {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for PendingReply {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.0.fetch_add(bytes.len(), Ordering::SeqCst);
            Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    async fn drop_at_pending(future: impl Future) {
        let mut future = Box::pin(future);
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    #[tokio::test]
    async fn dropped_query_blocks_both_entry_points_without_writing() {
        let writes = Arc::new(AtomicUsize::new(0));
        let mut conn = TcpConnection::new(PendingReply(writes.clone()));
        drop_at_pending(conn.query(&Query::new("sysctl report status"))).await;
        assert!(conn.is_poisoned());
        let before = writes.load(Ordering::SeqCst);
        assert!(conn
            .query(&Query::new("sysctl report status"))
            .await
            .is_err());
        assert!(conn
            .query_parse::<()>(&Query::new("sysctl report status"))
            .await
            .is_err());
        assert!(conn.execute_pipeline(&Pipeline::new()).await.is_err());
        assert_eq!(before, writes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn partially_written_query_poisons_the_connection() {
        let writes = Arc::new(AtomicUsize::new(0));
        let mut conn = TcpConnection::new(PartialWrite {
            writes: writes.clone(),
            accepted: 0,
            limit: 1,
        });
        drop_at_pending(conn.query(&Query::new("sysctl report status"))).await;
        assert!(conn.is_poisoned());
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert!(conn
            .query(&Query::new("sysctl report status"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn partially_read_query_poisons_the_connection() {
        let (socket, mut peer) = tokio::io::duplex(256);
        // A response type byte without the rest of the reply: the decoder is
        // still pending when the future is dropped.
        peer.write_all(b"\x10").await.unwrap();
        let mut conn = TcpConnection::new(socket);
        drop_at_pending(conn.query(&Query::new("sysctl report status"))).await;
        assert!(conn.is_poisoned());
        assert!(conn
            .query(&Query::new("sysctl report status"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn protocol_error_poisons_the_connection() {
        let (socket, mut peer) = tokio::io::duplex(256);
        peer.write_all(b"\xff").await.unwrap();
        let mut conn = TcpConnection::new(socket);
        let err = conn
            .query(&Query::new("sysctl report status"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ProtocolError(_)), "{:?}", err);
        assert!(conn.is_poisoned());
        assert!(conn
            .query(&Query::new("sysctl report status"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn eof_poisons_the_connection() {
        let (socket, peer) = tokio::io::duplex(256);
        drop(peer);
        let mut conn = TcpConnection::new(socket);
        let err = conn
            .query(&Query::new("sysctl report status"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::IoError(_)), "{:?}", err);
        assert!(conn.is_poisoned());
        assert!(conn
            .query(&Query::new("sysctl report status"))
            .await
            .is_err());
    }
    #[tokio::test]
    async fn dropped_pipeline_also_poisons_the_connection() {
        let writes = Arc::new(AtomicUsize::new(0));
        let mut conn = TcpConnection::new(PendingReply(writes.clone()));
        let mut pipeline = Pipeline::new();
        pipeline.push(&Query::new("sysctl report status"));
        drop_at_pending(conn.execute_pipeline(&pipeline)).await;
        assert!(conn.is_poisoned());
        let before = writes.load(Ordering::SeqCst);
        assert!(conn.execute_pipeline(&pipeline).await.is_err());
        assert!(conn
            .query(&Query::new("sysctl report status"))
            .await
            .is_err());
        assert_eq!(before, writes.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn completed_replies_do_not_poison_query_or_pipeline() {
        for reply in [&b"\x12"[..], &b"\x10\x01\x00"[..]] {
            let (socket, mut peer) = tokio::io::duplex(256);
            peer.write_all(reply).await.unwrap();
            let mut conn = TcpConnection::new(socket);
            conn.query(&Query::new("sysctl report status"))
                .await
                .unwrap();
            assert!(!conn.is_poisoned());
            peer.write_all(reply).await.unwrap();
            let mut pipeline = Pipeline::new();
            pipeline.push(&Query::new("sysctl report status"));
            assert_eq!(conn.execute_pipeline(&pipeline).await.unwrap().len(), 1);
            assert!(!conn.is_poisoned());
        }
    }
    #[test]
    fn unpolled_query_does_not_poison() {
        let mut conn = TcpConnection::new(PendingReply(Arc::new(AtomicUsize::new(0))));
        let query = Query::new("sysctl report status");
        drop(conn.query(&query));
        assert!(!conn.is_poisoned());
    }

    #[test]
    fn unpolled_pipeline_does_not_poison() {
        let mut conn = TcpConnection::new(PendingReply(Arc::new(AtomicUsize::new(0))));
        let mut pipeline = Pipeline::new();
        pipeline.push(&Query::new("sysctl report status"));
        drop(conn.execute_pipeline(&pipeline));
        assert!(!conn.is_poisoned());
    }
}
