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
        protocol::{ClientHandshake, DecodeState, Decoder, RState, ServerHandshake},
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
        let mut tcpstream = TcpStream::connect((self.host(), self.port())).await?;
        let handshake = ClientHandshake::new(self);
        tcpstream.write_all(handshake.inner()).await?;
        let mut resp = [0u8; 4];
        tcpstream.read_exact(&mut resp).await?;
        match ServerHandshake::parse(resp)? {
            ServerHandshake::Error(e) => return Err(ConnectionSetupError::HandshakeError(e).into()),
            ServerHandshake::Okay(_suggestion) => {
                return Ok(ConnectionAsync(TcpConnection::new(tcpstream)))
            }
        }
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
        // init
        let mut stream = TlsConnector::from(connector)
            .connect(self.host(), stream)
            .await
            .map_err(|e| ConnectionSetupError::Other(format!("TLS handshake failed: {e}")))?;
        // handshake
        let handshake = ClientHandshake::new(self);
        stream.write_all(handshake.inner()).await?;
        let mut resp = [0u8; 4];
        stream.read_exact(&mut resp).await?;
        match ServerHandshake::parse(resp)? {
            ServerHandshake::Error(e) => return Err(ConnectionSetupError::HandshakeError(e).into()),
            ServerHandshake::Okay(_suggestion) => {
                return Ok(ConnectionTlsAsync(TcpConnection::new(stream)))
            }
        }
    }
}

#[derive(Debug)]
/// The underlying socket type
pub struct TcpConnection<C: AsyncWriteExt + AsyncReadExt + Unpin> {
    con: C,
    buf: Vec<u8>,
}

impl<C: AsyncWriteExt + AsyncReadExt + Unpin> TcpConnection<C> {
    fn new(con: C) -> Self {
        Self {
            con,
            buf: Vec::with_capacity(crate::BUFSIZE),
        }
    }
    /// Run a query and return a raw [`Response`]
    pub async fn query(&mut self, q: &Query) -> ClientResult<Response> {
        self.buf.clear();
        q.write_packet(&mut self.buf).unwrap();
        self.con.write_all(&self.buf).await?;
        self.buf.clear();
        let mut state = RState::default();
        let mut cursor = 0;
        let mut expected = Decoder::MIN_READBACK;
        loop {
            let mut buf = [0u8; crate::BUFSIZE];
            let n = self.con.read(&mut buf).await?;
            if n == 0 {
                return Err(Error::IoError(std::io::ErrorKind::ConnectionReset.into()));
            }
            if n < expected {
                continue;
            }
            self.buf.extend_from_slice(&buf[..n]);
            let mut decoder = Decoder::new(&self.buf, cursor);
            match decoder.validate_response(state) {
                DecodeState::Completed(resp) => return Ok(resp),
                DecodeState::ChangeState(_state) => {
                    expected = 1;
                    state = _state;
                    cursor = decoder.position();
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
