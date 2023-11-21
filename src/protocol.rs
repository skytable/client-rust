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

use crate::{
    config::Config,
    error::{ClientResult, ConnectionSetupError, Error},
    response::{Response, Row, Value},
};

pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Errors that can happen when handling protocol level encoding and decoding
#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolError {
    /// The server returned an invalid response for the data item
    InvalidServerResponseForData,
    /// The server possibly returned an unknown data type and we can't decode it. Note that this might happen when you use an older client version with
    /// a newer version of Skytable
    InvalidServerResponseUnknownDataType,
}

impl Value {
    fn u64(self) -> u64 {
        match self {
            Self::UInt64(u) => u,
            _ => unreachable!(),
        }
    }
}

/*
    Decode state management
*/

type ValueDecodeStateRaw = ValueDecodeStateAny<ValueState>;
type ValueDecodeState = ValueDecodeStateAny<PendingValue>;

#[derive(Debug, PartialEq)]
enum ValueDecodeStateAny<P, V = Value> {
    Pending(P),
    Decoded(V),
}

#[derive(Debug, PartialEq)]
struct ValueState {
    v: Value,
    meta: ValueStateMeta,
}

impl ValueState {
    fn new(v: Value, meta: ValueStateMeta) -> Self {
        Self { v, meta }
    }
}

#[derive(Debug, PartialEq)]
struct ValueStateMeta {
    start: usize,
    md1: u64,
    md1_flag: bool,
}

impl ValueStateMeta {
    fn zero() -> Self {
        Self {
            start: 0,
            md1: 0,
            md1_flag: false,
        }
    }
    fn new(start: usize, md1: u64, md1_flag: bool) -> Self {
        Self {
            start,
            md1,
            md1_flag,
        }
    }
}

#[derive(Debug, PartialEq)]
struct RowState {
    meta: ValueStateMeta,
    row: Vec<Value>,
    tmp: Option<PendingValue>,
}

impl RowState {
    fn new(meta: ValueStateMeta, row: Vec<Value>, tmp: Option<PendingValue>) -> Self {
        Self { meta, row, tmp }
    }
}

#[derive(Debug, PartialEq)]
enum ResponseState {
    Initial,
    PValue(PendingValue),
    PError,
    PRow(RowState),
}

#[derive(Debug, PartialEq)]
pub enum DecodeState {
    ChangeState(RState),
    Completed(Response),
    Error(ProtocolError),
}

#[derive(Debug, PartialEq)]
pub struct RState(ResponseState);
impl Default for RState {
    fn default() -> Self {
        RState(ResponseState::Initial)
    }
}

/*
    Decoder
*/

#[derive(Debug, PartialEq)]
pub struct Decoder<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Decoder<'a> {
    pub const MIN_READBACK: usize = 1;
    pub fn new(b: &'a [u8], i: usize) -> Self {
        Self { b, i }
    }
    pub fn validate_response(&mut self, RState(state): RState) -> DecodeState {
        match state {
            ResponseState::Initial => self.begin(),
            ResponseState::PError => self.resume_error(),
            ResponseState::PValue(v) => self.resume_value(v),
            ResponseState::PRow(r) => self.resume_row(r),
        }
    }
    pub fn position(&self) -> usize {
        self.i
    }
    fn begin(&mut self) -> DecodeState {
        match self._cursor_next() {
            // TODO(@ohsayan): this is reserved!
            0x0F => return DecodeState::Error(ProtocolError::InvalidServerResponseUnknownDataType),
            0x10 => self.resume_error(),
            0x11 => self.resume_row(RowState::new(ValueStateMeta::zero(), vec![], None)),
            0x12 => return DecodeState::Completed(Response::Empty),
            code => match self.start_decode(true, code, vec![], None) {
                Ok(ValueDecodeStateAny::Decoded(v)) => DecodeState::Completed(Response::Value(v)),
                Ok(ValueDecodeStateAny::Pending(pv)) => {
                    DecodeState::ChangeState(RState(ResponseState::PValue(pv)))
                }
                Err(e) => DecodeState::Error(e),
            },
        }
    }
    fn resume_error(&mut self) -> DecodeState {
        if self._remaining() < 2 {
            return DecodeState::ChangeState(RState(ResponseState::PError));
        }
        let bytes: [u8; 2] = [self._cursor_next(), self._cursor_next()];
        DecodeState::Completed(Response::Error(u16::from_le_bytes(bytes)))
    }
    fn resume_value(&mut self, PendingValue { state, tmp, stack }: PendingValue) -> DecodeState {
        match self.resume_decode(true, state, stack, tmp) {
            Ok(ValueDecodeStateAny::Pending(pv)) => {
                DecodeState::ChangeState(RState(ResponseState::PValue(pv)))
            }
            Ok(ValueDecodeStateAny::Decoded(v)) => DecodeState::Completed(Response::Value(v)),
            Err(e) => DecodeState::Error(e),
        }
    }
    fn resume_row(&mut self, mut row_state: RowState) -> DecodeState {
        if !row_state.meta.md1_flag {
            match self.__resume_decode(row_state.meta.md1, ValueStateMeta::zero()) {
                Ok(ValueDecodeStateAny::Pending(ValueState { v, .. })) => {
                    row_state.meta.md1 = v.u64();
                    return DecodeState::ChangeState(RState(ResponseState::PRow(row_state)));
                }
                Ok(ValueDecodeStateAny::Decoded(v)) => {
                    row_state.meta.md1 = v.u64();
                    row_state.meta.md1_flag = true;
                }
                Err(e) => return DecodeState::Error(e),
            }
        }
        while row_state.row.len() as u64 != row_state.meta.md1 {
            let r = match row_state.tmp.take() {
                None => {
                    if self._cursor_eof() {
                        return DecodeState::ChangeState(RState(ResponseState::PRow(row_state)));
                    }
                    let code = self._cursor_next();
                    let stack = vec![];
                    self.start_decode(true, code, stack, None)
                }
                Some(PendingValue { state, tmp, stack }) => {
                    self.resume_decode(true, state, stack, tmp)
                }
            };
            let r = match r {
                Ok(r) => r,
                Err(e) => return DecodeState::Error(e),
            };
            match r {
                ValueDecodeStateAny::Pending(pv) => {
                    row_state.tmp = Some(pv);
                    return DecodeState::ChangeState(RState(ResponseState::PRow(row_state)));
                }
                ValueDecodeStateAny::Decoded(v) => {
                    row_state.row.push(v);
                }
            }
        }
        DecodeState::Completed(Response::Row(Row::new(row_state.row)))
    }
}

impl<'a> Decoder<'a> {
    fn __resume_decode<T: DecodeDelimited>(
        &mut self,
        mut value: T,
        meta: ValueStateMeta,
    ) -> ProtocolResult<ValueDecodeStateRaw> {
        let mut okay = true;
        while !(self._cursor_eof() | self._creq(b'\n')) & okay {
            okay &= value.update(self._cursor_next());
        }
        let lf = self._creq(b'\n');
        self._cursor_incr_if(lf);
        okay &= !(lf & (self._cursor() == meta.start));
        if okay & lf {
            let start = meta.start;
            value
                .pack_completed(meta, &self.b[start..self._cursor() - 1])
                .map(ValueDecodeStateRaw::Decoded)
        } else {
            if okay {
                Ok(ValueDecodeStateAny::Pending(value.pack_pending(meta)))
            } else {
                Err(ProtocolError::InvalidServerResponseForData)
            }
        }
    }
    fn __resume_psize<T: DecodePsize>(
        &mut self,
        mut meta: ValueStateMeta,
    ) -> ProtocolResult<ValueDecodeStateRaw> {
        if !meta.md1_flag {
            match self.__resume_decode(meta.md1, ValueStateMeta::zero())? {
                ValueDecodeStateAny::Decoded(s) => {
                    let s = s.u64();
                    meta.md1_flag = true;
                    meta.md1 = s;
                }
                ValueDecodeStateAny::Pending(ValueState { v, .. }) => {
                    meta.md1 = v.u64();
                    return Ok(ValueDecodeStateRaw::Pending(ValueState::new(
                        T::empty(),
                        meta,
                    )));
                }
            }
        }
        meta.start = self._cursor();
        if self._remaining() as u64 >= meta.md1 {
            let buf = &self.b[meta.start..self._cursor() + meta.md1 as usize];
            self._cursor_incr_by(meta.md1 as usize);
            T::finish(buf).map(ValueDecodeStateAny::Decoded)
        } else {
            Ok(ValueDecodeStateAny::Pending(ValueState::new(
                T::empty(),
                meta,
            )))
        }
    }
}

impl<'a> Decoder<'a> {
    fn _cursor(&self) -> usize {
        self.i
    }
    fn _cursor_value(&self) -> u8 {
        self.b[self._cursor()]
    }
    fn _cursor_incr(&mut self) {
        self._cursor_incr_by(1)
    }
    fn _cursor_incr_by(&mut self, b: usize) {
        self.i += b;
    }
    fn _cursor_incr_if(&mut self, iff: bool) {
        self._cursor_incr_by(iff as _)
    }
    fn _cursor_next(&mut self) -> u8 {
        let r = self._cursor_value();
        self._cursor_incr();
        r
    }
    fn _remaining(&self) -> usize {
        self.b.len() - self.i
    }
    fn _cursor_eof(&self) -> bool {
        self._remaining() == 0
    }
    fn _creq(&self, b: u8) -> bool {
        (self.b[core::cmp::min(self.i, self.b.len() - 1)] == b) & !self._cursor_eof()
    }
}

trait DecodeDelimited {
    fn update(&mut self, _: u8) -> bool {
        true
    }
    fn pack_completed(self, meta: ValueStateMeta, full_buffer: &[u8]) -> ProtocolResult<Value>;
    fn pack_pending(self, meta: ValueStateMeta) -> ValueState;
}

trait DecodePsize {
    fn finish(b: &[u8]) -> ProtocolResult<Value>;
    fn empty() -> Value;
}

impl DecodePsize for Vec<u8> {
    fn finish(b: &[u8]) -> ProtocolResult<Value> {
        Ok(Value::Binary(b.to_owned()))
    }
    fn empty() -> Value {
        Value::Binary(vec![])
    }
}

impl DecodePsize for String {
    fn finish(b: &[u8]) -> ProtocolResult<Value> {
        core::str::from_utf8(b)
            .map(String::from)
            .map(Value::String)
            .map_err(|_| ProtocolError::InvalidServerResponseForData)
    }
    fn empty() -> Value {
        Value::String(String::new())
    }
}

macro_rules! impl_uint {
    ($($ty:ty as $variant:ident),*) => {
        $(impl DecodeDelimited for $ty {
            fn update(&mut self, b: u8) -> bool {
                let mut okay = true; let (r1, of_1) = self.overflowing_mul(10);
                okay &= !of_1; let (r2, of_2) = r1.overflowing_add((b & 0x0f) as $ty);
                okay &= !of_2;
                okay &= b.is_ascii_digit(); *self = r2; okay
            }
            fn pack_pending(self, meta: ValueStateMeta) -> ValueState { ValueState::new(Value::$variant(self), meta) }
            fn pack_completed(self, _: ValueStateMeta, _: &[u8]) -> ProtocolResult<Value> { Ok(Value::$variant(self)) }
        })*
    }
}

macro_rules! impl_fstr {
    ($($ty:ty as $variant:ident),*) => {
        $(impl DecodeDelimited for $ty {
            fn pack_pending(self, meta: ValueStateMeta) -> ValueState { ValueState::new(Value::$variant(self), meta) }
            fn pack_completed(self, _: ValueStateMeta, b: &[u8]) -> ProtocolResult<Value> {
                core::str::from_utf8(b).map_err(|_| ProtocolError::InvalidServerResponseForData)?.parse().map(Value::$variant).map_err(|_| ProtocolError::InvalidServerResponseForData)
            }
        })*
    };
}

impl_uint!(u8 as UInt8, u16 as UInt16, u32 as UInt32, u64 as UInt64);
impl_fstr!(
    i8 as SInt8,
    i16 as SInt16,
    i32 as SInt32,
    i64 as SInt64,
    f32 as Float32,
    f64 as Float64
);

#[derive(Debug, PartialEq)]
struct PendingValue {
    state: ValueState,
    tmp: Option<ValueState>,
    stack: Vec<(Vec<Value>, ValueStateMeta)>,
}

impl PendingValue {
    fn new(
        state: ValueState,
        tmp: Option<ValueState>,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> Self {
        Self { state, tmp, stack }
    }
}

impl<'a> Decoder<'a> {
    fn parse_list(
        &mut self,
        mut stack: Vec<(Vec<Value>, ValueStateMeta)>,
        mut last: Option<ValueState>,
    ) -> ProtocolResult<ValueDecodeStateAny<PendingValue, Value>> {
        let (mut current_list, mut current_meta) = stack.pop().unwrap();
        loop {
            if !current_meta.md1_flag {
                match self.__resume_decode(current_meta.md1, ValueStateMeta::zero())? {
                    ValueDecodeStateAny::Decoded(v) => {
                        current_meta.md1 = v.u64();
                        current_meta.md1_flag = true;
                    }
                    ValueDecodeStateAny::Pending(ValueState { v, .. }) => {
                        current_meta.md1 = v.u64();
                        stack.push((current_list, current_meta));
                        return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                            ValueState::new(Value::List(vec![]), ValueStateMeta::zero()),
                            None,
                            stack,
                        )));
                    }
                }
            }
            if current_list.len() as u64 == current_meta.md1 {
                match stack.pop() {
                    None => {
                        return Ok(ValueDecodeStateAny::Decoded(Value::List(current_list)));
                    }
                    Some((mut parent, parent_meta)) => {
                        parent.push(Value::List(current_list));
                        current_list = parent;
                        current_meta = parent_meta;
                        continue;
                    }
                }
            }
            let v = match last.take() {
                None => {
                    // nothing present, we need to decode
                    if self._cursor_eof() {
                        // wow, nothing here
                        stack.push((current_list, current_meta));
                        return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                            ValueState::new(Value::List(vec![]), ValueStateMeta::zero()),
                            None,
                            stack,
                        )));
                    }
                    match self._cursor_next() {
                        0x0E => {
                            // that's a list
                            stack.push((current_list, current_meta));
                            current_list = vec![];
                            current_meta = ValueStateMeta::zero();
                            continue;
                        }
                        code => self.start_decode(false, code, vec![], None),
                    }
                }
                Some(v) => self.resume_decode(false, v, vec![], None),
            }?;
            let v = match v {
                ValueDecodeStateAny::Pending(pv) => {
                    stack.push((current_list, current_meta));
                    return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                        ValueState::new(Value::List(vec![]), ValueStateMeta::zero()),
                        Some(pv.state),
                        stack,
                    )));
                }
                ValueDecodeStateAny::Decoded(v) => v,
            };
            current_list.push(v);
        }
    }
}

impl<'a> Decoder<'a> {
    fn start_decode(
        &mut self,
        root: bool,
        code: u8,
        mut stack: Vec<(Vec<Value>, ValueStateMeta)>,
        last: Option<ValueState>,
    ) -> ProtocolResult<ValueDecodeState> {
        let md = ValueStateMeta::new(self._cursor(), 0, false);
        let v = match code {
            0x00 => return Ok(ValueDecodeStateAny::Decoded(Value::Null)),
            0x01 => return self.parse_bool(stack),
            0x02 => self.__resume_decode(0u8, md),
            0x03 => self.__resume_decode(0u16, md),
            0x04 => self.__resume_decode(0u32, md),
            0x05 => self.__resume_decode(0u64, md),
            0x06 => self.__resume_decode(0i8, md),
            0x07 => self.__resume_decode(0i16, md),
            0x08 => self.__resume_decode(0i32, md),
            0x09 => self.__resume_decode(0i64, md),
            0x0A => self.__resume_decode(0f32, md),
            0x0B => self.__resume_decode(0f64, md),
            0x0C => self.__resume_psize::<Vec<u8>>(md),
            0x0D => self.__resume_psize::<String>(md),
            0x0E => {
                if !root {
                    unreachable!("recursive structure not captured by root");
                }
                stack.push((vec![], ValueStateMeta::zero()));
                return self.parse_list(stack, last);
            }
            _ => return Err(ProtocolError::InvalidServerResponseUnknownDataType),
        }?;
        Self::check_pending(v, stack)
    }
    fn resume_decode(
        &mut self,
        root: bool,
        ValueState { v, meta }: ValueState,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
        last: Option<ValueState>,
    ) -> ProtocolResult<ValueDecodeState> {
        let r = match v {
            Value::Null => unreachable!(),
            Value::Bool(_) => return self.parse_bool(stack),
            Value::UInt8(l) => self.__resume_decode(l, meta),
            Value::UInt16(l) => self.__resume_decode(l, meta),
            Value::UInt32(l) => self.__resume_decode(l, meta),
            Value::UInt64(l) => self.__resume_decode(l, meta),
            Value::SInt8(l) => self.__resume_decode(l, meta),
            Value::SInt16(l) => self.__resume_decode(l, meta),
            Value::SInt32(l) => self.__resume_decode(l, meta),
            Value::SInt64(l) => self.__resume_decode(l, meta),
            Value::Float32(l) => self.__resume_decode(l, meta),
            Value::Float64(l) => self.__resume_decode(l, meta),
            Value::Binary(_) => self.__resume_psize::<Vec<u8>>(meta),
            Value::String(_) => self.__resume_psize::<String>(meta),
            Value::List(_) => {
                if !root {
                    unreachable!("recursive structure not captured by root");
                }
                return self.parse_list(stack, last);
            }
        }?;
        Self::check_pending(r, stack)
    }
    fn parse_bool(
        &mut self,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> ProtocolResult<ValueDecodeState> {
        if self._cursor_eof() {
            return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                ValueState::new(Value::Bool(false), ValueStateMeta::zero()),
                None,
                stack,
            )));
        }
        let nx = self._cursor_next();
        if nx < 2 {
            return Ok(ValueDecodeStateAny::Decoded(Value::Bool(nx == 1)));
        } else {
            return Err(ProtocolError::InvalidServerResponseForData);
        }
    }
    fn check_pending(
        r: ValueDecodeStateAny<ValueState, Value>,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> Result<ValueDecodeStateAny<PendingValue, Value>, ProtocolError> {
        match r {
            ValueDecodeStateAny::Pending(p) => Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                p, None, stack,
            ))),
            ValueDecodeStateAny::Decoded(v) => Ok(ValueDecodeStateAny::Decoded(v)),
        }
    }
}

pub struct ClientHandshake(Box<[u8]>);
impl ClientHandshake {
    pub(crate) fn new(cfg: &Config) -> Self {
        let mut v = Vec::with_capacity(6 + cfg.username().len() + cfg.password().len() + 5);
        v.extend(b"H\x00\x00\x00\x00\x00");
        pushlen!(v, cfg.username().len());
        pushlen!(v, cfg.password().len());
        v.extend(cfg.username().as_bytes());
        v.extend(cfg.password().as_bytes());
        Self(v.into_boxed_slice())
    }
    pub(crate) fn inner(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug)]
pub enum ServerHandshake {
    Okay(u8),
    Error(u8),
}
impl ServerHandshake {
    pub fn parse(v: [u8; 4]) -> ClientResult<Self> {
        Ok(match v {
            [b'H', 0, 0, msg] => Self::Okay(msg),
            [b'H', 0, 1, msg] => Self::Error(msg),
            _ => {
                return Err(Error::ConnectionSetupErr(
                    ConnectionSetupError::InvalidServerHandshake,
                ))
            }
        })
    }
}

#[test]
fn t_row() {
    let mut decoder = Decoder::new(b"\x115\n\x00\x01\x01\x0D5\nsayan\x0220\n\x0E0\n", 0);
    assert_eq!(
        decoder.validate_response(RState::default()),
        DecodeState::Completed(Response::Row(Row::new(vec![
            Value::Null,
            Value::Bool(true),
            Value::String("sayan".into()),
            Value::UInt8(20),
            Value::List(vec![])
        ])))
    );
}
