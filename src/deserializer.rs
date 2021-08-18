/*
 * Created on Tue May 11 2021
 *
 * Copyright (c) 2021 Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

//! # The Skyhash Protocol
//!
//! ## Introduction
//! The Skyhash Protocol is a serialization protocol that is used by Skytable for client/server communication.
//! It works in a query/response action similar to HTTP's request/response action. Skyhash supersedes the Terrapipe
//! protocol as a more simple, reliable, robust and scalable protocol.
//!
//! This module contains the [`Parser`] for the Skyhash protocol and it's enough to just pass a query packet as
//! a slice of unsigned 8-bit integers and the parser will do everything else. The Skyhash protocol was designed
//! by Sayan Nandan and this is the first client implementation of the protocol
//!

use crate::types::Array;
use crate::types::FlatElement;
use crate::RespCode;
use std::hint::unreachable_unchecked;

#[derive(Debug)]
/// # Skyhash Deserializer (Parser)
///
/// The [`Parser`] object can be used to deserialized a packet serialized by Skyhash which in turn serializes
/// it into data structures native to the Rust Language (and some Compound Types built on top of them).
///
/// ## Evaluation
///
/// The parser is pessimistic in most cases and will readily throw out any errors. On non-recusrive types
/// there is no recursion, but the parser will use implicit recursion for nested arrays. The parser will
/// happily not report any errors if some part of the next query was passed. This is very much a possibility
/// and so has been accounted for
///
/// ## Important note
///
/// All developers willing to modify the deserializer must keep this in mind: the cursor is always Ahead-Of-Position
/// that is the cursor should always point at the next character that can be read.
///
pub(super) struct Parser<'a> {
    /// The internal cursor position
    ///
    /// Do not even think of touching this externally
    cursor: usize,
    /// The buffer slice
    buffer: &'a [u8],
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
/// # Data Types
///
/// This enum represents the data types supported by the Skyhash Protocol
pub enum Element {
    /// Array types
    Array(Array),
    /// An unicode string value; `<tsymbol>` is `+`
    String(String),
    /// A binary string (`?`)
    Binstr(Vec<u8>),
    /// An unsigned integer value; `<tsymbol>` is `:`
    UnsignedInt(u64),
    /// A response code
    RespCode(RespCode),
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
/// # Parser Errors
///
/// Several errors can arise during parsing and this enum accounts for them
pub enum ParseError {
    /// Didn't get the number of expected bytes
    NotEnough,
    /// The query contains an unexpected byte
    UnexpectedByte,
    /// The packet simply contains invalid data
    ///
    /// This is rarely returned and only in the special cases where a bad client sends `0` as
    /// the query count
    BadPacket,
    /// A data type was given but the parser failed to serialize it into this type
    ///
    /// This can happen not just for elements but can also happen for their sizes ([`Self::parse_into_u64`])
    DataTypeError,
    /// A data type that the client doesn't know was passed into the query
    ///
    /// This is a frequent problem that can arise between different server editions as more data types
    /// can be added with changing server versions
    UnknownDatatype,
    /// The query is empty
    ///
    /// The **parser will never return this**, but instead it is provided for convenience with [`dbnet`]
    Empty,
}

#[derive(Debug, PartialEq)]
/// # Types of Response
///
/// A simple response carries the data for one action while a complex response carries data for
/// multiple actions
pub enum RawResponse {
    /// A simple query will just hold one element
    SimpleQuery(Element),
    /// A pipelined/batch query will hold multiple elements
    PipelinedQuery(Vec<Element>),
}

/// A generic result to indicate parsing errors thorugh the [`ParseError`] enum
pub type ParseResult<T> = Result<T, ParseError>;

impl<'a> Parser<'a> {
    /// Initialize a new parser instance
    pub const fn new(buffer: &'a [u8]) -> Self {
        Parser {
            cursor: 0usize,
            buffer,
        }
    }
    /// Read from the current cursor position to `until` number of positions ahead
    /// This **will forward the cursor itself** if the bytes exist or it will just return a `NotEnough` error
    fn read_until(&mut self, until: usize) -> ParseResult<&[u8]> {
        if let Some(b) = self.buffer.get(self.cursor..self.cursor + until) {
            self.cursor += until;
            Ok(b)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// This returns the position at which the line parsing began and the position at which the line parsing
    /// stopped, in other words, you should be able to do self.buffer[started_at..stopped_at] to get a line
    /// and do it unchecked. This **will move the internal cursor ahead** and place it **at the `\n` byte**
    fn read_line(&mut self) -> (usize, usize) {
        let started_at = self.cursor;
        let mut stopped_at = self.cursor;
        while self.cursor < self.buffer.len() {
            if self.buffer[self.cursor] == b'\n' {
                // Oh no! Newline reached, time to break the loop
                // But before that ... we read the newline, so let's advance the cursor
                self.incr_cursor();
                break;
            }
            // So this isn't an LF, great! Let's forward the stopped_at position
            stopped_at += 1;
            self.incr_cursor();
        }
        (started_at, stopped_at)
    }
    /// Push the internal cursor ahead by one
    fn incr_cursor(&mut self) {
        self.cursor += 1;
    }
    /// This function will evaluate if the byte at the current cursor position equals the `ch` argument, i.e
    /// the expression `*v == ch` is evaluated. However, if no element is present ahead, then the function
    /// will return `Ok(_this_if_nothing_ahead_)`
    fn will_cursor_give_char(&self, ch: u8, this_if_nothing_ahead: bool) -> ParseResult<bool> {
        self.buffer.get(self.cursor).map_or(
            if this_if_nothing_ahead {
                Ok(true)
            } else {
                Err(ParseError::NotEnough)
            },
            |v| Ok(*v == ch),
        )
    }
    /// Will the current cursor position give a linefeed? This will return `ParseError::NotEnough` if
    /// the current cursor points at a non-existent index in `self.buffer`
    fn will_cursor_give_linefeed(&self) -> ParseResult<bool> {
        self.will_cursor_give_char(b'\n', false)
    }
    /// Parse a stream of bytes into [`usize`]
    fn parse_into_usize(bytes: &[u8]) -> ParseResult<usize> {
        if bytes.is_empty() {
            return Err(ParseError::NotEnough);
        }
        let byte_iter = bytes.iter();
        let mut item_usize = 0usize;
        for dig in byte_iter {
            if !dig.is_ascii_digit() {
                // dig has to be an ASCII digit
                return Err(ParseError::DataTypeError);
            }
            // 48 is the ASCII code for 0, and 57 is the ascii code for 9
            // so if 0 is given, the subtraction should give 0; similarly
            // if 9 is given, the subtraction should give us 9!
            let curdig: usize = dig
                .checked_sub(48)
                .unwrap_or_else(|| unsafe { unreachable_unchecked() })
                .into();
            // The usize can overflow; check that case
            let product = match item_usize.checked_mul(10) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeError),
            };
            let sum = match product.checked_add(curdig) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeError),
            };
            item_usize = sum;
        }
        Ok(item_usize)
    }
    /// Pasre a stream of bytes into an [`u64`]
    fn parse_into_u64(bytes: &[u8]) -> ParseResult<u64> {
        if bytes.is_empty() {
            return Err(ParseError::NotEnough);
        }
        let byte_iter = bytes.iter();
        let mut item_u64 = 0u64;
        for dig in byte_iter {
            if !dig.is_ascii_digit() {
                // dig has to be an ASCII digit
                return Err(ParseError::DataTypeError);
            }
            // 48 is the ASCII code for 0, and 57 is the ascii code for 9
            // so if 0 is given, the subtraction should give 0; similarly
            // if 9 is given, the subtraction should give us 9!
            let curdig: u64 = dig
                .checked_sub(48)
                .unwrap_or_else(|| unsafe { unreachable_unchecked() })
                .into();
            // Now the entire u64 can overflow, so let's attempt to check it
            let product = match item_u64.checked_mul(10) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeError),
            };
            let sum = match product.checked_add(curdig) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeError),
            };
            item_u64 = sum;
        }
        Ok(item_u64)
    }
    /// This will return the number of datagroups present in this query packet
    ///
    /// This **will forward the cursor itself**
    fn parse_metaframe_get_datagroup_count(&mut self) -> ParseResult<usize> {
        // the smallest query we can have is: *1\n or 3 chars
        if self.buffer.len() < 3 {
            return Err(ParseError::NotEnough);
        }
        // Now we want to read `*<n>\n`
        let (start, stop) = self.read_line();
        if let Some(our_chunk) = self.buffer.get(start..stop) {
            if our_chunk[0] == b'*' {
                // Good, this will tell us the number of actions
                // Let us attempt to read the usize from this point onwards
                // that is excluding the '*' (so 1..)
                let ret = Self::parse_into_usize(&our_chunk[1..])?;
                Ok(ret)
            } else {
                Err(ParseError::UnexpectedByte)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// Get the next element **without** the tsymbol
    ///
    /// This function **does not forward the newline**
    fn __get_next_element(&mut self) -> ParseResult<&[u8]> {
        let string_sizeline = self.read_line();
        if let Some(line) = self.buffer.get(string_sizeline.0..string_sizeline.1) {
            let string_size = Self::parse_into_usize(line)?;
            let our_chunk = self.read_until(string_size)?;
            Ok(our_chunk)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// The cursor should have passed the `?` tsymbol
    fn parse_next_binstr(&mut self) -> ParseResult<Vec<u8>> {
        let our_string_chunk = self.__get_next_element()?.to_owned();
        if self.will_cursor_give_linefeed()? {
            // there is a lf after the end of the binary string; great!
            // let's skip that now
            self.incr_cursor();
            // let's return our string
            Ok(our_string_chunk)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// Parse the next null checked element
    fn parse_next_chunk_nullck(&mut self) -> ParseResult<Option<&[u8]>> {
        // we have the chunk
        let (start, stop) = self.read_line();
        if let Some(sizeline) = self.buffer.get(start..stop) {
            let string_size = Self::parse_into_usize_nullck(sizeline)?;
            if let Some(size) = string_size {
                // so it isn't null
                let our_chunk = self.read_until(size)?;
                Ok(Some(our_chunk))
            } else {
                Ok(None)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// The cursor should have passed the `+` tsymbol
    fn parse_next_string(&mut self) -> ParseResult<String> {
        Ok(String::from_utf8_lossy(&self.parse_next_binstr()?).to_string())
    }
    fn parse_next_binstr_nullck(&mut self) -> ParseResult<Option<Vec<u8>>> {
        let our_chunk = self.parse_next_chunk_nullck()?;
        if let Some(chunk) = our_chunk {
            let our_chunk = chunk.to_owned();
            if self.will_cursor_give_linefeed()? {
                // there is a lf after the end of the binary string; great!
                // let's skip that now
                self.incr_cursor();
                Ok(Some(our_chunk))
            } else {
                Err(ParseError::UnexpectedByte)
            }
        } else {
            Ok(None)
        }
    }
    fn parse_next_str_nullck(&mut self) -> ParseResult<Option<String>> {
        match self.parse_next_binstr_nullck()? {
            Some(chunk) => Ok(Some(String::from_utf8_lossy(&chunk).to_string())),
            None => Ok(None),
        }
    }
    /// The cursor should have passed the `:` tsymbol
    fn parse_next_u64(&mut self) -> ParseResult<u64> {
        let our_u64_chunk = self.__get_next_element()?;
        let our_u64 = Self::parse_into_u64(our_u64_chunk)?;
        if self.will_cursor_give_linefeed()? {
            // line feed after u64; heck yeah!
            self.incr_cursor();
            // return it
            Ok(our_u64)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    fn parse_next_respcode(&mut self) -> ParseResult<RespCode> {
        let our_respcode_chunk = self.__get_next_element()?;
        let our_respcode = RespCode::from_str(&String::from_utf8_lossy(our_respcode_chunk));
        if self.will_cursor_give_linefeed()? {
            self.incr_cursor();
            Ok(our_respcode)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// The cursor should be **at the tsymbol**
    fn parse_next_element(&mut self) -> ParseResult<Element> {
        if let Some(tsymbol) = self.buffer.get(self.cursor) {
            // so we have a tsymbol; nice, let's match it
            // but advance the cursor before doing that (skip)
            self.incr_cursor();
            let ret = match *tsymbol {
                b'?' => Element::Binstr(self.parse_next_binstr()?),
                b'+' => Element::String(self.parse_next_string()?),
                b':' => Element::UnsignedInt(self.parse_next_u64()?),
                b'&' => Element::Array(Array::Recursive(self.parse_next_array()?)),
                b'!' => Element::RespCode(self.parse_next_respcode()?),
                b'@' => {
                    // hmmm, a typed array; let's check the tsymbol
                    if let Some(array_type) = self.buffer.get(self.cursor) {
                        // got tsymbol, let's skip it
                        self.incr_cursor();
                        match array_type {
                            b'+' => Element::Array(Array::Str(self.parse_next_typed_array_str()?)),
                            b'?' => Element::Array(Array::Bin(self.parse_next_typed_array_bin()?)),
                            _ => return Err(ParseError::UnknownDatatype),
                        }
                    } else {
                        // if we couldn't fetch a tsymbol, there wasn't enough
                        // data left
                        return Err(ParseError::NotEnough);
                    }
                }
                b'_' => Element::Array(Array::Flat(self.parse_next_flat_array()?)),
                _ => return Err(ParseError::UnknownDatatype),
            };
            Ok(ret)
        } else {
            // Not enough bytes to read an element
            Err(ParseError::NotEnough)
        }
    }
    /// Parse the next null checked usize
    fn parse_into_usize_nullck(inp: &[u8]) -> ParseResult<Option<usize>> {
        if inp == [0] {
            Ok(None)
        } else {
            Ok(Some(Self::parse_into_usize(inp)?))
        }
    }
    /// The cursor should have passed the `@+` chars
    fn parse_next_typed_array_str(&mut self) -> ParseResult<Vec<Option<String>>> {
        let (start, stop) = self.read_line();
        if let Some(our_size_chunk) = self.buffer.get(start..stop) {
            // so we have a size chunk; let's get the size
            let array_size = Self::parse_into_usize(our_size_chunk)?;
            let mut array = Vec::with_capacity(array_size);
            for _ in 0..array_size {
                // no tsymbol, just elements and their sizes
                array.push(self.parse_next_str_nullck()?);
            }
            Ok(array)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// The cursor should have passed the `@?` chars
    fn parse_next_typed_array_bin(&mut self) -> ParseResult<Vec<Option<Vec<u8>>>> {
        let (start, stop) = self.read_line();
        if let Some(our_size_chunk) = self.buffer.get(start..stop) {
            // got size chunk, let's get the size
            let array_size = Self::parse_into_usize(our_size_chunk)?;
            let mut array = Vec::with_capacity(array_size);
            for _ in 0..array_size {
                array.push(self.parse_next_binstr_nullck()?);
            }
            Ok(array)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// The cursor should have passed the tsymbol
    fn parse_next_flat_array(&mut self) -> ParseResult<Vec<FlatElement>> {
        let (start, stop) = self.read_line();
        if let Some(our_size_chunk) = self.buffer.get(start..stop) {
            let array_size = Self::parse_into_usize(our_size_chunk)?;
            let mut array = Vec::with_capacity(array_size);
            for _ in 0..array_size {
                if let Some(tsymbol) = self.buffer.get(self.cursor) {
                    // good, there is a tsymbol; move the cursor ahead
                    self.incr_cursor();
                    let ret = match *tsymbol {
                        b'+' => FlatElement::String(self.parse_next_string()?),
                        b'?' => FlatElement::Binstr(self.parse_next_binstr()?),
                        b'!' => FlatElement::RespCode(self.parse_next_respcode()?),
                        b':' => FlatElement::UnsignedInt(self.parse_next_u64()?),
                        _ => return Err(ParseError::UnknownDatatype),
                    };
                    array.push(ret);
                } else {
                    return Err(ParseError::NotEnough);
                }
            }
            Ok(array)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// The tsymbol `&` should have been passed!
    fn parse_next_array(&mut self) -> ParseResult<Vec<Element>> {
        let (start, stop) = self.read_line();
        if let Some(our_size_chunk) = self.buffer.get(start..stop) {
            let array_size = Self::parse_into_usize(our_size_chunk)?;
            let mut array = Vec::with_capacity(array_size);
            for _ in 0..array_size {
                array.push(self.parse_next_element()?);
            }
            Ok(array)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// Parse a query and return the [`Query`] and an `usize` indicating the number of bytes that
    /// can be safely discarded from the buffer. It will otherwise return errors if they are found.
    ///
    /// This object will drop `Self`
    pub fn parse(mut self) -> Result<(RawResponse, usize), ParseError> {
        let number_of_queries = self.parse_metaframe_get_datagroup_count()?;
        if number_of_queries == 0 {
            // how on earth do you expect us to execute 0 queries? waste of bandwidth
            return Err(ParseError::BadPacket);
        }
        if number_of_queries == 1 {
            // This is a simple query
            let single_group = self.parse_next_element()?;
            // The below line defaults to false if no item is there in the buffer
            // or it checks if the next time is a \r char; if it is, then it is the beginning
            // of the next query
            #[allow(clippy::blocks_in_if_conditions)]
            // this lint is pointless here, just some optimizations
            if self
                .will_cursor_give_char(b'*', true)
                .unwrap_or_else(|_| unsafe {
                    // This will never be the case because we'll always get a result and no error value
                    // as we've passed true which will yield Ok(true) even if there is no byte ahead
                    unreachable_unchecked()
                })
            {
                Ok((RawResponse::SimpleQuery(single_group), self.cursor))
            } else {
                // the next item isn't the beginning of a query but something else?
                // that doesn't look right!
                Err(ParseError::UnexpectedByte)
            }
        } else {
            // This is a pipelined query
            // We'll first make space for all the actiongroups
            let mut queries = Vec::with_capacity(number_of_queries);
            for _ in 0..number_of_queries {
                queries.push(self.parse_next_element()?);
            }
            if self.will_cursor_give_char(b'*', true)? {
                Ok((RawResponse::PipelinedQuery(queries), self.cursor))
            } else {
                Err(ParseError::UnexpectedByte)
            }
        }
    }
}

#[test]
fn test_typed_str_array() {
    let typed_array_packet = "*1\n@+3\n3\nthe\n3\ncat\n6\nmeowed\n".as_bytes();
    let (parsed, forward) = Parser::new(typed_array_packet).parse().unwrap();
    assert_eq!(forward, typed_array_packet.len());
    assert_eq!(
        parsed,
        RawResponse::SimpleQuery(Element::Array(Array::Str(vec![
            Some("the".to_owned()),
            Some("cat".to_owned()),
            Some("meowed".to_owned())
        ])))
    );
}

#[test]
fn test_typed_bin_array() {
    let typed_array_packet = "*1\n@?3\n3\nthe\n3\ncat\n6\nmeowed\n".as_bytes();
    let (parsed, forward) = Parser::new(typed_array_packet).parse().unwrap();
    assert_eq!(forward, typed_array_packet.len());
    assert_eq!(
        parsed,
        RawResponse::SimpleQuery(Element::Array(Array::Bin(vec![
            Some(Vec::from("the")),
            Some(Vec::from("cat")),
            Some(Vec::from("meowed"))
        ])))
    );
}

#[test]
fn test_typed_bin_array_null() {
    let typed_array_packet = "*1\n@?3\n3\nthe\n3\ncat\n\0\n".as_bytes();
    let (parsed, forward) = Parser::new(typed_array_packet).parse().unwrap();
    assert_eq!(forward, typed_array_packet.len());
    assert_eq!(
        parsed,
        RawResponse::SimpleQuery(Element::Array(Array::Bin(vec![
            Some(Vec::from("the")),
            Some(Vec::from("cat")),
            None
        ])))
    );
}

#[test]
fn test_typed_str_array_null() {
    let typed_array_packet = "*1\n@+3\n3\nthe\n3\ncat\n\0\n".as_bytes();
    let (parsed, forward) = Parser::new(typed_array_packet).parse().unwrap();
    assert_eq!(forward, typed_array_packet.len());
    assert_eq!(
        parsed,
        RawResponse::SimpleQuery(Element::Array(Array::Str(vec![
            Some("the".to_owned()),
            Some("cat".to_owned()),
            None
        ])))
    );
}
