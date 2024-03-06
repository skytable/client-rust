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

//! # Synchronous database I/O
//!
//! This module provides the necessary items to establish a synchronous connection to the database server. If you need
//! to use connection pooling, consider checking the [`pool`](crate::pool) module.
//!
//! See the [`crate`] root documentation for help on establishing and using database connections.
//!

use {
    crate::{
        config::Config,
        error::{ClientResult, ConnectionSetupError, Error},
        protocol::{
            ClientHandshake, DecodeState, Decoder, MRespState, PipelineResult, RState,
            ServerHandshake,
        },
        query::Pipeline,
        response::{FromResponse, Response},
        Query,
    },
    native_tls::{Certificate, TlsConnector, TlsStream},
    std::{
        io::{Read, Write},
        net::TcpStream,
        ops::{Deref, DerefMut},
    },
};

/// A `skyhash/TCP` connection
///
/// **Specification**
/// - Protocol version: `Skyhash/2.0`
/// - Query mode: `QTDEX-1A/BQL-S1`
/// - Authentication plugin: `pwd`
#[derive(Debug)]
pub struct Connection(TcpConnection<TcpStream>);
/// A `skyhash/TLS` connection
///
/// **Specification**
/// - Protocol version: `Skyhash/2.0`
/// - Query mode: `QTDEX-1A/BQL-S1`
/// - Authentication plugin: `pwd`
#[derive(Debug)]
pub struct ConnectionTls(TcpConnection<TlsStream<TcpStream>>);

impl Deref for Connection {
    type Target = TcpConnection<TcpStream>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Deref for ConnectionTls {
    type Target = TcpConnection<TlsStream<TcpStream>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for ConnectionTls {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Config {
    /// Establish a connection to the database using the current configuration
    pub fn connect(&self) -> ClientResult<Connection> {
        let mut tcpstream = TcpStream::connect((self.host(), self.port()))?;
        let handshake = ClientHandshake::new(self);
        tcpstream.write_all(handshake.inner())?;
        let mut resp = [0u8; 4];
        tcpstream.read_exact(&mut resp)?;
        match ServerHandshake::parse(resp)? {
            ServerHandshake::Error(e) => return Err(ConnectionSetupError::HandshakeError(e).into()),
            ServerHandshake::Okay(_suggestion) => {
                return Ok(Connection(TcpConnection::new(tcpstream)))
            }
        }
    }
    /// Establish a TLS connection to the database using the current configuration.
    /// Pass the certificate in PEM format.
    pub fn connect_tls(&self, cert: &str) -> ClientResult<ConnectionTls> {
        let stream = TcpStream::connect((self.host(), self.port()))?;
        let mut stream = TlsConnector::builder()
            .add_root_certificate(Certificate::from_pem(cert.as_bytes()).map_err(|e| {
                ConnectionSetupError::Other(format!("failed to parse certificate: {e}"))
            })?)
            .danger_accept_invalid_hostnames(true)
            .build()
            .map_err(|e| {
                ConnectionSetupError::Other(format!("failed to set up TLS acceptor: {e}"))
            })?
            .connect(self.host(), stream)
            .map_err(|e| ConnectionSetupError::Other(format!("TLS handshake failed: {e}")))?;
        let handshake = ClientHandshake::new(self);
        stream.write_all(handshake.inner())?;
        let mut resp = [0u8; 4];
        stream.read_exact(&mut resp)?;
        match ServerHandshake::parse(resp)? {
            ServerHandshake::Error(e) => return Err(ConnectionSetupError::HandshakeError(e).into()),
            ServerHandshake::Okay(_suggestion) => {
                return Ok(ConnectionTls(TcpConnection::new(stream)))
            }
        }
    }
}

#[derive(Debug)]
/// The underlying connection type
///
/// This can't be constructed directly!
pub struct TcpConnection<C: Write + Read> {
    con: C,
    buf: Vec<u8>,
}

impl<C: Write + Read> TcpConnection<C> {
    fn new(con: C) -> Self {
        Self {
            con,
            buf: Vec::with_capacity(crate::BUFSIZE),
        }
    }
    /// Execute a pipeline. The server returns the queries in the order they were sent (unless otherwise set).
    pub fn execute_pipeline(&mut self, pipeline: &Pipeline) -> ClientResult<Vec<Response>> {
        self.buf.clear();
        self.buf.push(b'P');
        // query count
        self.buf.extend(
            itoa::Buffer::new()
                .format(pipeline.query_count())
                .as_bytes(),
        );
        self.buf.push(b'\n');
        // packet size
        self.buf
            .extend(itoa::Buffer::new().format(pipeline.buf().len()).as_bytes());
        self.buf.push(b'\n');
        // write
        self.con.write_all(&self.buf)?;
        self.con.write_all(pipeline.buf())?;
        self.buf.clear();
        // read
        let mut expected = Decoder::MIN_READBACK;
        let mut cursor = 0;
        let mut state = MRespState::default();
        loop {
            let mut buf = [0u8; crate::BUFSIZE];
            let n = self.con.read(&mut buf)?;
            if n == 0 {
                return Err(Error::IoError(std::io::ErrorKind::ConnectionReset.into()));
            }
            if n < expected {
                continue;
            }
            self.buf.extend_from_slice(&buf[..n]);
            let mut decoder = Decoder::new(&self.buf, cursor);
            match decoder.validate_pipe(cursor == 0, state) {
                PipelineResult::Completed(r) => return Ok(r),
                PipelineResult::Pending(_state) => {
                    expected = 1;
                    cursor = decoder.position();
                    state = _state;
                }
                PipelineResult::Error(e) => return Err(e.into()),
            }
        }
    }
    /// Run a query and return a raw [`Response`]
    pub fn query(&mut self, q: &Query) -> ClientResult<Response> {
        self.buf.clear();
        q.write_packet(&mut self.buf).unwrap();
        self.con.write_all(&self.buf)?;
        self.buf.clear();
        let mut state = RState::default();
        let mut cursor = 0;
        loop {
            let mut buf = [0u8; crate::BUFSIZE];
            let n = self.con.read(&mut buf)?;
            if n == 0 {
                return Err(Error::IoError(std::io::ErrorKind::ConnectionReset.into()));
            }
            self.buf.extend_from_slice(&buf[..n]);
            let mut decoder = Decoder::new(&self.buf, cursor);
            match decoder.validate_response(state) {
                DecodeState::ChangeState(new_state) => {
                    state = new_state;
                    cursor = decoder.position();
                    continue;
                }
                DecodeState::Completed(resp) => return Ok(resp),
                DecodeState::Error(e) => return Err(e.into()),
            }
        }
    }
    /// Run and parse a query into the indicated type. The type must implement [`FromResponse`]
    pub fn query_parse<T: FromResponse>(&mut self, q: &Query) -> ClientResult<T> {
        self.query(q).and_then(FromResponse::from_response)
    }
    /// Call this if the internally allocated buffer is growing too large and impacting your performance. However, normally
    /// you will not need to call this
    pub fn reset_buffer(&mut self) {
        self.buf.shrink_to_fit()
    }
}
