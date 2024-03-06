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

//! # Client errors
//!
//! This module provides various enumerations and types used to represent client errors at any stage of the connect/query/response
//! process.
//!
//! You might find Skytable's documentation on error codes helpful: [https://docs.skytable.io/protocol/errors](https://docs.skytable.io/protocol/errors)
//!

use {crate::protocol::ProtocolError, core::fmt};

/// A [`Result`] type alias for the client driver
pub type ClientResult<T> = Result<T, Error>;

#[derive(Debug)]
/// Client driver errors
///
/// This is a broad classification for all kinds of possible client driver errors, across I/O, server errors and application level parse errors
pub enum Error {
    /// An I/O error occurred
    IoError(std::io::Error),
    /// A bad [`Config`](crate::config::Config) throws this error
    ConnectionSetupErr(ConnectionSetupError),
    /// When running a query, a protocol error was thrown
    ProtocolError(ProtocolError),
    /// A server error code was received
    ServerError(u16),
    /// An application level parse error
    ParseError(ParseError),
}

impl std::error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "io error: {e}"),
            Self::ConnectionSetupErr(e) => write!(f, "connection setup error: {e}"),
            Self::ProtocolError(e) => write!(f, "protocol error: {e}"),
            Self::ServerError(e) => write!(f, "server error: {e}"),
            Self::ParseError(e) => write!(f, "application parse error: {e}"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
/// An application level parse error, usually raised by [`FromResponse`](crate::response::FromResponse)
pub enum ParseError {
    /// The response is non-erroring, but the type is not what was expected
    TypeMismatch,
    /// The response is non-erroring, but not of the kind we were looking for (for example, if you try to parse a single value from a Row, it won't work!)
    ResponseMismatch,
    /// Some other parse error occurred
    Other(String),
}

impl std::error::Error for ParseError {}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch => write!(f, "data type mismatch"),
            Self::ResponseMismatch => write!(f, "response type mismatch"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
/// An error specifically returned during connection setup. This is returned usually when there is a bad configuration
pub enum ConnectionSetupError {
    /// Some error occurred while setting up a connection
    Other(String),
    /// Handshake failed while establishing a connection
    HandshakeError(u8),
    /// The server responded with an invalid handshake
    InvalidServerHandshake,
}

impl std::error::Error for ConnectionSetupError {}
impl fmt::Display for ConnectionSetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Other(e) => write!(f, "{e}"),
            Self::HandshakeError(e) => write!(f, "handshake error code {e}"),
            Self::InvalidServerHandshake => write!(f, "server sent invalid handshake"),
        }
    }
}

impl std::error::Error for ProtocolError {}
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidServerResponseForData => write!(f, "invalid data received from server"),
            Self::InvalidServerResponseUnknownDataType => {
                write!(f, "new or unknown data type received from server")
            }
            Self::InvalidPacket => write!(f, "invalid packet received from server"),
        }
    }
}

/*
    from impls
*/

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<ProtocolError> for Error {
    fn from(e: ProtocolError) -> Self {
        Self::ProtocolError(e)
    }
}

impl From<ConnectionSetupError> for Error {
    fn from(e: ConnectionSetupError) -> Self {
        Self::ConnectionSetupErr(e)
    }
}
