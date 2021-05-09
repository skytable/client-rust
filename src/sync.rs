use crate::deserializer::{self, ClientResult};
use crate::{Query, Response};
use std::io::Read;
pub use std::io::Result as IoResult;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use crate::BUF_CAP;

#[derive(Debug)]
/// A `Connection` is a wrapper around a`TcpStream` and a read buffer
pub struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    /// Create a new connection to a Skytable instance hosted on `host` and running on `port`
    pub fn new(host: &str, port: u16) -> IoResult<Self> {
        let stream = TcpStream::connect((host, port))?;
        Ok(Connection {
            stream: stream,
            buffer: Vec::with_capacity(BUF_CAP)
        })
    }
    /// This function will write a [`Query`] to the stream and read the response from the
    /// server. It will then determine if the returned response is complete or incomplete
    /// or invalid and return an appropriate variant of [`Response`] wrapped in [`IoResult`]
    /// for any I/O errors that may occur
    pub fn run_simple_query(&mut self, mut query: Query) -> IoResult<Response> {
        query.write_query_sync(&mut self.stream)?;
        loop {
            let mut buf = [0u8; 1024];
            let read = self.stream.read(&mut buf)?;
            if read == 0 {
                break Err(Error::from(ErrorKind::ConnectionReset));
            }
            self.buffer.extend_from_slice(&buf[..read]);
            match self.try_response(read) {
                ClientResult::Empty => break Err(Error::from(ErrorKind::ConnectionReset)),
                ClientResult::Incomplete => {
                    continue;
                }
                ClientResult::SimpleResponse(r, _) => {
                    self.buffer.clear();
                    break Ok(Response::Array(r));
                }
                ClientResult::ResponseItem(r, _) => {
                    self.buffer.clear();
                    break Ok(Response::Item(r));
                }
                ClientResult::InvalidResponse => {
                    self.buffer.clear();
                    break Ok(Response::InvalidResponse);
                }
                ClientResult::ParseError => {
                    self.buffer.clear();
                    break Ok(Response::ParseError);
                }
                ClientResult::PipelinedResponse(_, _) => {
                    todo!("Pipelined queries haven't been implemented yet!")
                }
            }
        }
    }
    /// This function is a subroutine of `run_query` used to parse the response packet
    fn try_response(&mut self, read: usize) -> ClientResult {
        if self.buffer.is_empty() {
            // The connection was possibly reset
            return ClientResult::Empty;
        }
        deserializer::parse(&self.buffer[..read])
    }
}
