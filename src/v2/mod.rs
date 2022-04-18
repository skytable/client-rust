/*
 * Copyright 2022, Sayan Nandan <nandansayan@outlook.com>
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

use crate::{
    types::{Array, FlatElement},
    Element, RespCode,
};
use core::{
    num::{ParseFloatError, ParseIntError},
    slice,
    str::{self, Utf8Error},
};

/// A generic result to indicate parsing errors thorugh the [`ParseError`] enum
pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Debug, PartialEq)]
#[non_exhaustive]
#[repr(u8)]
/// # Parser Errors
///
/// Several errors can arise during parsing and this enum accounts for them
pub enum ParseError {
    /// Didn't get the number of expected bytes
    NotEnough,
    /// The packet simply contains invalid data
    BadPacket,
    /// The query contains an unexpected byte
    UnexpectedByte,
    /// A data type was given but the parser failed to serialize it into this type
    DataTypeError,
    /// A data type that the client doesn't know was passed into the query
    ///
    /// This is a frequent problem that can arise between different server editions as more data types
    /// can be added with changing server versions
    UnknownDatatype,
}

impl From<ParseIntError> for ParseError {
    fn from(_: ParseIntError) -> Self {
        Self::DataTypeError
    }
}

impl From<Utf8Error> for ParseError {
    fn from(_: Utf8Error) -> Self {
        Self::DataTypeError
    }
}

impl From<ParseFloatError> for ParseError {
    fn from(_: ParseFloatError) -> Self {
        Self::DataTypeError
    }
}

#[derive(Debug, PartialEq)]
/// # Response types
///
/// A simple response carries the response for a simple query while a pipelined response carries the response
/// for pipelined queries
pub enum RawResponse {
    /// A simple query will just hold one element
    SimpleQuery(Element),
    /// A pipelined/batch query will hold multiple elements
    PipelinedQuery(Vec<Element>),
}

pub struct Parser<'a> {
    slice: &'a [u8],
    cursor: usize,
}

impl<'a> Parser<'a> {
    pub fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            cursor: 0usize,
        }
    }
    fn remaining(&self) -> usize {
        self.slice.len() - self.cursor
    }
    fn has_remaining(&self, c: usize) -> bool {
        self.remaining() >= c
    }
    fn not_exhausted(&self) -> bool {
        self.cursor < self.slice.len()
    }
    unsafe fn direct_read(&self, s: usize, c: usize) -> &[u8] {
        slice::from_raw_parts(self.slice.as_ptr().add(s), c)
    }
    // mut refs
    fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.has_remaining(by), "Buffer overflow");
        self.cursor += by;
    }
    fn decr_cursor_by(&mut self, by: usize) {
        debug_assert!(
            self.cursor != 0 && self.cursor.checked_sub(by).is_some(),
            "Size underflow"
        );
        self.cursor -= 1;
    }
    fn decr_cursor(&mut self) {
        self.decr_cursor_by(1)
    }
    fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    unsafe fn get_byte_at_cursor(&self) -> u8 {
        debug_assert!(self.not_exhausted(), "Buffer overflow");
        *self.slice.as_ptr().add(self.cursor)
    }
    fn read_until(&mut self, c: usize) -> ParseResult<&[u8]> {
        if self.has_remaining(c) {
            let cursor = self.cursor;
            self.incr_cursor_by(c);
            let slice = unsafe {
                // UNSAFE(@ohsayan): Just verified length
                self.direct_read(cursor, c)
            };
            Ok(slice)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    fn read_line(&mut self) -> ParseResult<&[u8]> {
        let cursor = self.cursor;
        while self.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first condition ensures
                // that the current byte is present in the allocation
                self.get_byte_at_cursor()
            } != b'\n'
        {
            self.incr_cursor();
        }
        if self.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first condition ensures
                // that the current byte is present in the allocation
                self.get_byte_at_cursor()
            } == b'\n'
        {
            let len = self.cursor - cursor;
            self.incr_cursor(); // skip LF
            Ok(unsafe {
                // UNSAFE(@ohsayan): Just verified length
                self.direct_read(cursor, len)
            })
        } else {
            Err(ParseError::NotEnough)
        }
    }
    fn read_line_pedantic(&mut self) -> ParseResult<&[u8]> {
        let cursor = self.cursor;
        while self.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first condition ensures
                // that the current byte is present in the allocation
                self.get_byte_at_cursor()
            } != b'\n'
        {
            self.incr_cursor();
        }
        let len = self.cursor - cursor;
        let has_lf = self.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first condition ensures
                // that the current byte is present in the allocation
                self.get_byte_at_cursor()
            } == b'\n';
        if self.not_exhausted() && has_lf && len != 0 {
            self.incr_cursor(); // skip LF
            Ok(unsafe {
                // UNSAFE(@ohsayan): Just verified lengths
                self.direct_read(cursor, len)
            })
        } else {
            let r = if has_lf {
                ParseError::BadPacket
            } else {
                ParseError::NotEnough
            };
            Err(r)
        }
    }
    fn try_read_cursor(&mut self) -> ParseResult<u8> {
        if self.not_exhausted() {
            let r = unsafe {
                // UNSAFE(@ohsayan): Just checked len
                self.get_byte_at_cursor()
            };
            self.incr_cursor();
            Ok(r)
        } else {
            Err(ParseError::NotEnough)
        }
    }
}

// higher level abstractions
impl<'a> Parser<'a> {
    fn read_u64(&mut self) -> ParseResult<u64> {
        let line = self.read_line_pedantic()?;
        let r = str::from_utf8(line)?.parse()?;
        Ok(r)
    }
    fn read_usize(&mut self) -> ParseResult<usize> {
        let line = self.read_line_pedantic()?;
        let r = str::from_utf8(line)?.parse()?;
        Ok(r)
    }
    fn read_usize_nullck(&mut self) -> ParseResult<Option<usize>> {
        match self.try_read_cursor()? {
            b'\0' => {
                // null
                Ok(None)
            }
            _ => {
                self.decr_cursor();
                let usz = self.read_usize()?;
                Ok(Some(usz))
            }
        }
    }
    fn read_string(&mut self) -> ParseResult<String> {
        let size = self.read_usize()?;
        let line = self.read_until(size)?;
        let r = str::from_utf8(line)?.to_owned();
        Ok(r)
    }
    fn read_string_nullck(&mut self) -> ParseResult<Option<String>> {
        if let Some(size) = self.read_usize_nullck()? {
            Ok(Some(str::from_utf8(self.read_until(size)?)?.to_owned()))
        } else {
            Ok(None)
        }
    }
    fn read_binary_nullck(&mut self) -> ParseResult<Option<Vec<u8>>> {
        if let Some(size) = self.read_usize_nullck()? {
            Ok(Some(self.read_until(size)?.to_owned()))
        } else {
            Ok(None)
        }
    }
    fn read_binary(&mut self) -> ParseResult<Vec<u8>> {
        let size = self.read_usize()?;
        Ok(self.read_until(size)?.to_owned())
    }
    fn read_respcode(&mut self) -> ParseResult<RespCode> {
        let line = self.read_line()?;
        let st = str::from_utf8(line)?;
        Ok(RespCode::from_str(st))
    }
    fn read_float(&mut self) -> ParseResult<f32> {
        let line = self.read_line()?;
        let st = str::from_utf8(line)?;
        Ok(st.parse()?)
    }
    fn read_flat_array(&mut self) -> ParseResult<Vec<FlatElement>> {
        let array_len = self.read_usize()?;
        let mut data = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            match self.try_read_cursor()? {
                b'+' => data.push(FlatElement::String(self.read_string()?)),
                b'?' => data.push(FlatElement::Binstr(self.read_binary()?)),
                b'!' => data.push(FlatElement::RespCode(self.read_respcode()?)),
                b':' => data.push(FlatElement::UnsignedInt(self.read_u64()?)),
                b'%' => data.push(FlatElement::Float(self.read_float()?)),
                _ => return Err(ParseError::UnknownDatatype),
            }
        }
        Ok(data)
    }
    fn read_typed_array_string(&mut self) -> ParseResult<Vec<Option<String>>> {
        let size = self.read_usize()?;
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(self.read_string_nullck()?);
        }
        Ok(data)
    }
    fn read_typed_array_binary(&mut self) -> ParseResult<Vec<Option<Vec<u8>>>> {
        let size = self.read_usize()?;
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(self.read_binary_nullck()?);
        }
        Ok(data)
    }
    fn read_typed_array(&mut self) -> ParseResult<Element> {
        let r = match self.try_read_cursor()? {
            b'+' => Element::Array(Array::Str(self.read_typed_array_string()?)),
            b'?' => Element::Array(Array::Bin(self.read_typed_array_binary()?)),
            _ => return Err(ParseError::UnknownDatatype),
        };
        Ok(r)
    }
    fn read_typed_nonnull_array_string(&mut self) -> ParseResult<Vec<String>> {
        let size = self.read_usize()?;
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(self.read_string()?);
        }
        Ok(data)
    }
    fn read_typed_nonnull_array_binary(&mut self) -> ParseResult<Vec<Vec<u8>>> {
        let size = self.read_usize()?;
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(self.read_binary()?);
        }
        Ok(data)
    }
    fn read_typed_nonnull_array(&mut self) -> ParseResult<Element> {
        let r = match self.try_read_cursor()? {
            b'+' => Element::Array(Array::NonNullStr(self.read_typed_nonnull_array_string()?)),
            b'?' => Element::Array(Array::NonNullBin(self.read_typed_nonnull_array_binary()?)),
            _ => return Err(ParseError::UnknownDatatype),
        };
        Ok(r)
    }
}

// response methods
impl<'a> Parser<'a> {
    fn _read_simple_resp(&mut self) -> ParseResult<Element> {
        let r = match self.try_read_cursor()? {
            b'+' => Element::String(self.read_string()?),
            b'?' => Element::Binstr(self.read_binary()?),
            b'!' => Element::RespCode(self.read_respcode()?),
            b':' => Element::UnsignedInt(self.read_u64()?),
            b'%' => Element::Float(self.read_float()?),
            b'@' => self.read_typed_array()?,
            b'^' => self.read_typed_nonnull_array()?,
            b'_' => Element::Array(Array::Flat(self.read_flat_array()?)),
            _ => return Err(ParseError::UnknownDatatype),
        };
        Ok(r)
    }
    fn read_simple_resp(&mut self) -> ParseResult<Element> {
        self._read_simple_resp()
    }
    fn read_pipeline_resp(&mut self) -> ParseResult<Vec<Element>> {
        let size = self.read_usize()?;
        let mut resps = Vec::with_capacity(size);
        for _ in 0..size {
            resps.push(self._read_simple_resp()?);
        }
        Ok(resps)
    }
    fn _parse(&mut self) -> ParseResult<RawResponse> {
        let r = match self.try_read_cursor()? {
            b'*' => RawResponse::SimpleQuery(self.read_simple_resp()?),
            b'$' => RawResponse::PipelinedQuery(self.read_pipeline_resp()?),
            _ => return Err(ParseError::BadPacket),
        };
        Ok(r)
    }
    pub fn parse(buffer: &'a [u8]) -> ParseResult<RawResponse> {
        let mut slf = Self::new(buffer);
        slf._parse()
    }
}

#[test]
fn set_resp() {
    let setresp = b"*!0\n".to_vec();
    assert_eq!(
        Parser::parse(&setresp).unwrap(),
        RawResponse::SimpleQuery(Element::RespCode(RespCode::Okay))
    );
}

#[test]
fn mget_resp() {
    let mgetresp = b"*@+4\n5\nsayan2\nis8\nthinking\0\n".to_vec();
    let ret = Parser::parse(&mgetresp).unwrap();
    assert_eq!(
        ret,
        RawResponse::SimpleQuery(Element::Array(Array::Str(vec![
            Some("sayan".to_owned()),
            Some("is".to_owned()),
            Some("thinking".to_owned()),
            None
        ])))
    )
}

#[test]
fn pipe_resp() {
    let resp = b"$2\n!0\n@+4\n5\nsayan2\nis8\nthinking\0\n".to_vec();
    assert_eq!(
        Parser::parse(&resp).unwrap(),
        RawResponse::PipelinedQuery(vec![
            Element::RespCode(RespCode::Okay),
            Element::Array(Array::Str(vec![
                Some("sayan".to_owned()),
                Some("is".to_owned()),
                Some("thinking".to_owned()),
                None
            ]))
        ])
    )
}

#[test]
fn lskeys_resp() {
    let resp = b"*^+3\n5\nsayan2\nis8\nthinking".to_vec();
    assert_eq!(
        Parser::parse(&resp).unwrap(),
        RawResponse::SimpleQuery(Element::Array(Array::NonNullStr(vec![
            "sayan".to_string(),
            "is".to_string(),
            "thinking".to_string()
        ])))
    );
}
