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

use crate::protocol::ProtocolError;

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

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The response is non-erroring, but the type is not what was expected
    TypeMismatch,
    /// The response is non-erroring, but not of the kind we were looking for (for example, if you try to parse a single value from a Row, it won't work!)
    ResponseMismatch,
    /// Some other parse error occurred
    Other(String),
}

#[derive(Debug, PartialEq)]
/// An error specifically returned during connection setup. This is returned usually when there is a bad configuration
pub enum ConnectionSetupError {
    Other(String),
    HandshakeError(u8),
    InvalidServerHandshake,
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
