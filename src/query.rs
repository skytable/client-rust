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
}

impl Query {
    pub fn new(query: &str) -> Self {
        Self {
            dataframe_q: query.as_bytes().to_owned().into_boxed_slice(),
            dataframe_p: vec![],
        }
    }
    pub fn push_param(&mut self, param: impl SQParam) -> &mut Self {
        param.push(&mut self.dataframe_p);
        self
    }
    #[inline(always)]
    pub(crate) fn write_packet(&self, buf: &mut impl Write) -> io::Result<()> {
        /*
            [[total packet size][query window]][[dataframe][qframe]]
            ^meta1            ^meta2           ^payload
        */
        // compute the total packet size
        let query_window_str = self.dataframe_q.len().to_string();
        let total_packet_size =
            query_window_str.len() + 1 + self.dataframe_q.len() + self.dataframe_p.len();
        // segment 1: meta
        buf.write_all(b"S")?;
        buf.write_all(&total_packet_size.to_string().as_bytes())?;
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
    fn push(self, buf: &mut Vec<u8>);
}
// null
impl<T> SQParam for Option<T>
where
    T: SQParam,
{
    fn push(self, buf: &mut Vec<u8>) {
        match self {
            None => buf.push(0),
            Some(e) => e.push(buf),
        }
    }
}
// bool
impl SQParam for bool {
    fn push(self, buf: &mut Vec<u8>) {
        let a = [1, self as u8];
        buf.extend(a)
    }
}
macro_rules! imp_number {
    ($($code:literal => $($ty:ty),*),* $(,)?) => {
        $($(impl SQParam for $ty { fn push(self, buf: &mut Vec<u8>) { buf.push($code); buf.extend(self.to_string().as_bytes()); buf.push(b'\n'); } })*)*
    }
}
// uint, sint, float
imp_number!(
    2 => u8, NonZeroU8, u16, NonZeroU16, u32, NonZeroU32, u64, NonZeroU64, usize, NonZeroUsize,
    3 => i8, NonZeroI8, i16, NonZeroI16, i32, NonZeroI32, i64, NonZeroI64, isize, NonZeroIsize,
    4 => f32, f64,
);
// bin
impl<'a> SQParam for &'a [u8] {
    fn push(self, buf: &mut Vec<u8>) {
        buf.push(5);
        buf.extend(self.len().to_string().into_bytes());
        buf.push(b'\n');
        buf.extend(self);
    }
}
impl<const N: usize> SQParam for [u8; N] {
    fn push(self, buf: &mut Vec<u8>) {
        buf.push(5);
        buf.extend(self.len().to_string().into_bytes());
        buf.push(b'\n');
        buf.extend(self);
    }
}
impl<'a, const N: usize> SQParam for &'a [u8; N] {
    fn push(self, buf: &mut Vec<u8>) {
        buf.push(5);
        buf.extend(self.len().to_string().into_bytes());
        buf.push(b'\n');
        buf.extend(self);
    }
}
// str
impl<'a> SQParam for &'a str {
    fn push(self, buf: &mut Vec<u8>) {
        buf.push(6);
        buf.extend(self.len().to_string().into_bytes());
        buf.push(b'\n');
        buf.extend(self.as_bytes());
    }
}
impl SQParam for String {
    fn push(self, buf: &mut Vec<u8>) {
        self.as_str().push(buf)
    }
}
