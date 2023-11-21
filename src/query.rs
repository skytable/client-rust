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

use std::{
    io::{self, Write},
    num::{
        NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroIsize, NonZeroU16, NonZeroU32,
        NonZeroU64, NonZeroU8, NonZeroUsize,
    },
};

/*
    query impl
*/

#[derive(Debug, PartialEq, Clone)]
/// A [`Query`] represents a Skyhash query. This is the "standard query" that you will normally use for almost all operations.
///
/// Specification: `QTDEX-A/BQL-S1`
pub struct Query {
    dataframe_q: Box<[u8]>,
    dataframe_p: Vec<u8>,
    param_cnt: usize,
}

impl From<String> for Query {
    fn from(q: String) -> Self {
        Self::new_string(q)
    }
}

impl<'a> From<&'a str> for Query {
    fn from(q: &'a str) -> Self {
        Self::new(q)
    }
}

impl Query {
    pub fn new(query: &str) -> Self {
        Self::_new(query.to_owned())
    }
    pub fn new_string(query: String) -> Self {
        Self::_new(query)
    }
    fn _new(query: String) -> Self {
        Self {
            dataframe_q: query.into_bytes().into_boxed_slice(),
            dataframe_p: vec![],
            param_cnt: 0,
        }
    }
    pub fn query_str(&self) -> &str {
        unsafe { core::str::from_utf8_unchecked(&self.dataframe_q) }
    }
    pub fn push_param(&mut self, param: impl SQParam) -> &mut Self {
        param.append_param(&mut self.dataframe_p);
        self.param_cnt += 1;
        self
    }
    pub fn param_cnt(&self) -> usize {
        self.param_cnt
    }
    #[inline(always)]
    pub(crate) fn write_packet(&self, buf: &mut impl Write) -> io::Result<()> {
        /*
            [[total packet size][query window]][[dataframe][qframe]]
            ^meta1            ^meta2           ^payload
        */
        // compute the total packet size
        // q window
        let mut query_window_buffer = itoa::Buffer::new();
        let query_window_str = query_window_buffer.format(self.dataframe_q.len());
        // full packet
        let total_packet_size =
            query_window_str.len() + 1 + self.dataframe_q.len() + self.dataframe_p.len();
        let mut total_packet_size_buffer = itoa::Buffer::new();
        let total_packet_size_str = total_packet_size_buffer.format(total_packet_size);
        // segment 1: meta
        buf.write_all(b"S")?;
        buf.write_all(total_packet_size_str.as_bytes())?;
        buf.write_all(b"\n")?;
        // segment 2: variable meta
        buf.write_all(query_window_str.as_bytes())?;
        buf.write_all(b"\n")?;
        // segment 3: payload
        buf.write_all(&self.dataframe_q)?;
        buf.write_all(&self.dataframe_p)?;
        Ok(())
    }
    #[inline(always)]
    pub fn debug_encode_packet(&self) -> Vec<u8> {
        let mut v = vec![];
        self.write_packet(&mut v).unwrap();
        v
    }
}

/*
    Query parameters
*/

/// An [`SQParam`] should be implemented by any type that is expected to be used as a parameter
pub trait SQParam {
    /// Append this element to the raw parameter buffer
    fn append_param(self, buf: &mut Vec<u8>);
}
// null
impl<T> SQParam for Option<T>
where
    T: SQParam,
{
    fn append_param(self, buf: &mut Vec<u8>) {
        match self {
            None => buf.push(0),
            Some(e) => e.append_param(buf),
        }
    }
}
// bool
impl SQParam for bool {
    fn append_param(self, buf: &mut Vec<u8>) {
        let a = [1, self as u8];
        buf.extend(a)
    }
}
macro_rules! imp_number {
    ($($code:literal => $($ty:ty as $base:ty),*),* $(,)?) => {
        $($(impl SQParam for $ty { fn append_param(self, b: &mut Vec<u8>) {
            let mut buf = ::itoa::Buffer::new();
            let str = buf.format(<$base>::from(self));
            b.push($code); b.extend(str.as_bytes()); b.push(b'\n');
        } })*)*
    }
}

macro_rules! imp_terminated_str_type {
    ($($code:literal => $($ty:ty),*),* $(,)?) => {
        $($(impl SQParam for $ty { fn append_param(self, buf: &mut Vec<u8>) { buf.push($code); buf.extend(self.to_string().as_bytes()); buf.push(b'\n'); } })*)*
    }
}

// uint, sint, float
imp_number!(
    2 => u8 as u8, NonZeroU8 as u8, u16 as u16, NonZeroU16 as u16, u32 as u32, NonZeroU32 as u32, u64 as u64, NonZeroU64 as u64, usize as usize, NonZeroUsize as usize,
    3 => i8 as i8, NonZeroI8 as i8, i16 as i16, NonZeroI16 as i16, i32 as i32, NonZeroI32 as i32, i64 as i64, NonZeroI64 as i64, isize as isize, NonZeroIsize as isize,
);

imp_terminated_str_type!(
    4 => f32, f64
);

// bin
impl<'a> SQParam for &'a [u8] {
    fn append_param(self, buf: &mut Vec<u8>) {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(self);
    }
}
impl<const N: usize> SQParam for [u8; N] {
    fn append_param(self, buf: &mut Vec<u8>) {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(self);
    }
}
impl<'a, const N: usize> SQParam for &'a [u8; N] {
    fn append_param(self, buf: &mut Vec<u8>) {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(self);
    }
}
impl SQParam for Vec<u8> {
    fn append_param(self, buf: &mut Vec<u8>) {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(self);
    }
}
// str
impl<'a> SQParam for &'a str {
    fn append_param(self, buf: &mut Vec<u8>) {
        buf.push(6);
        pushlen!(buf, self.len());
        buf.extend(self.as_bytes());
    }
}
impl SQParam for String {
    fn append_param(self, buf: &mut Vec<u8>) {
        self.as_str().append_param(buf)
    }
}
