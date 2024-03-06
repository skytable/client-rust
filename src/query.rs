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

//! # Queries
//!
//! This module provides the basic tools needed to create and run queries.
//!
//! ## Example
//! ```no_run
//! use skytable::{Config, Query};
//!
//! let mut db = Config::new_default("username", "password").connect().unwrap();
//! let mut query = Query::new("select * from myspace where username = ?");
//! query.push_param(".... get user name from somewhere ...");
//!
//! let ret = db.query(&query).unwrap();
//! ```
//!

use std::{
    io::{self, Write},
    iter::FromIterator,
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
    buf: Vec<u8>,
    param_cnt: usize,
    q_window: usize,
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
    /// Create a new query from a [`str`]
    pub fn new(query: &str) -> Self {
        Self::_new(query.to_owned())
    }
    /// Create a new query from a [`String`]
    pub fn new_string(query: String) -> Self {
        Self::_new(query)
    }
    fn _new(query: String) -> Self {
        let l = query.len();
        Self {
            buf: query.into_bytes(),
            param_cnt: 0,
            q_window: l,
        }
    }
    /// Returns a reference to the query string
    pub fn query_str(&self) -> &str {
        unsafe { core::str::from_utf8_unchecked(&self.buf[..self.q_window]) }
    }
    /// Add a new parameter to the query
    pub fn push_param(&mut self, param: impl SQParam) -> &mut Self {
        self.param_cnt += param.append_param(&mut self.buf);
        self
    }
    /// Get the number of parameters
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
        let query_window_str = query_window_buffer.format(self.q_window);
        // full packet
        let total_packet_size = query_window_str.len() + 1 + self.buf.len();
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
        buf.write_all(&self.buf)?;
        Ok(())
    }
    #[inline(always)]
    /// Encodes the packet using Skyhash and returns a raw packet for debugging purposes
    pub fn debug_encode_packet(&self) -> Vec<u8> {
        let mut v = vec![];
        self.write_packet(&mut v).unwrap();
        v
    }
}

/// # Pipeline
///
/// A pipeline can be used to send multiple queries at once to the server. Queries in a pipeline are executed independently
/// of one another, but they are executed serially unless otherwise configured
pub struct Pipeline {
    cnt: usize,
    buf: Vec<u8>,
}

impl Pipeline {
    /// Create a new pipeline
    pub const fn new() -> Self {
        Self {
            cnt: 0,
            buf: Vec::new(),
        }
    }
    pub(crate) fn buf(&self) -> &[u8] {
        &self.buf
    }
    /// Returns the number of queries that were appended to this pipeline
    pub fn query_count(&self) -> usize {
        self.cnt
    }
    /// Add a query to this pipeline
    ///
    /// Note: It's not possible to get the query back from the pipeline since it's not indexed (and doing so would be an unnecessary
    /// waste of space and time). That's why we take a reference which allows the caller to continue owning the [`Query`] item
    pub fn add_query(&mut self, q: &Query) {
        // qlen
        self.buf
            .extend(itoa::Buffer::new().format(q.q_window).as_bytes());
        self.buf.push(b'\n');
        // plen
        self.buf.extend(
            itoa::Buffer::new()
                .format(q.buf.len() - q.q_window)
                .as_bytes(),
        );
        self.buf.push(b'\n');
        // body
        self.buf.extend(&q.buf);
        self.cnt += 1;
    }
}

impl<Q: AsRef<Query>, I> From<I> for Pipeline
where
    I: Iterator<Item = Q>,
{
    fn from(iter: I) -> Self {
        let mut pipeline = Pipeline::new();
        iter.into_iter()
            .for_each(|q| pipeline.add_query(q.as_ref()));
        pipeline
    }
}

impl<Q: AsRef<Query>> Extend<Q> for Pipeline {
    fn extend<T: IntoIterator<Item = Q>>(&mut self, iter: T) {
        iter.into_iter().for_each(|q| self.add_query(q.as_ref()))
    }
}

impl<Q: AsRef<Query>> FromIterator<Q> for Pipeline {
    fn from_iter<T: IntoIterator<Item = Q>>(iter: T) -> Self {
        let mut pipe = Pipeline::new();
        iter.into_iter().for_each(|q| pipe.add_query(q.as_ref()));
        pipe
    }
}

impl AsRef<Query> for Query {
    fn as_ref(&self) -> &Query {
        self
    }
}

/*
    Query parameters
*/

/// An [`SQParam`] should be implemented by any type that is expected to be used as a parameter
///
/// ## Example implementation
///
/// Say you have a custom type which has to store `<username>-<id>` in your database and your database schema looks like
/// `create model myspace.mymodel(username: string, id: uint64, password: string)`. You can do that directly:
///
/// ```
/// use skytable::{query, query::SQParam};
///
/// struct MyType {
///     username: String,
///     id: u64,
///     password: String,
/// }
///
/// impl MyType {
///     fn new(username: String, id: u64, password: String) -> Self {
///         Self { username, id, password }
///     }
/// }
///
/// impl SQParam for MyType {
///     fn append_param(&self, buf: &mut Vec<u8>) -> usize {
///         self.username.append_param(buf) +
///         self.id.append_param(buf) +
///         self.password.append_param(buf)
///     }
/// }
///
/// // You can now directly do this!
/// let query = query!("insert into myspace.mymodel(?, ?, ?)", MyType::new("sayan".to_owned(), 0, "pass123".to_owned()));
/// assert_eq!(query.param_cnt(), 3);
/// // You can also used it beside normal params
/// // assume schema is `create model mymomdel2(uname: string, id: uint64, pass: string, age: uint8)`
/// let query = query!("insert into myspace.mymodel(?, ?, ?, ?)", MyType::new("sayan".to_owned(), 0, "pass123".to_owned()), 101);
/// assert_eq!(query.param_cnt(), 4);
/// ```
pub trait SQParam {
    /// Append this element to the raw parameter buffer
    ///
    /// Return the number of parameters appended (see example above)
    fn append_param(&self, q: &mut Vec<u8>) -> usize;
}
// null
impl<T> SQParam for Option<T>
where
    T: SQParam,
{
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            None => {
                buf.push(0);
                1
            }
            Some(e) => e.append_param(buf),
        }
    }
}

/// Use this when you need to use `null`
pub struct Null;
impl SQParam for Null {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        buf.push(0);
        1
    }
}
// bool
impl SQParam for bool {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        let a = [1, *self as u8];
        buf.extend(a);
        1
    }
}
macro_rules! imp_number {
    ($($code:literal => $($ty:ty as $base:ty),*),* $(,)?) => {
        $($(impl SQParam for $ty { fn append_param(&self, b: &mut Vec<u8>) -> usize {
            let mut buf = ::itoa::Buffer::new();
            let str = buf.format(<$base>::from(*self));
            b.push($code); b.extend(str.as_bytes()); b.push(b'\n');
            1
        } })*)*
    }
}

macro_rules! imp_terminated_str_type {
    ($($code:literal => $($ty:ty),*),* $(,)?) => {
        $($(impl SQParam for $ty { fn append_param(&self, buf: &mut Vec<u8>) -> usize { buf.push($code); buf.extend(self.to_string().as_bytes()); buf.push(b'\n'); 1} })*)*
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
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(*self);
        1
    }
}
impl<const N: usize> SQParam for [u8; N] {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(self);
        1
    }
}
impl<'a, const N: usize> SQParam for &'a [u8; N] {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(*self);
        1
    }
}
impl SQParam for Vec<u8> {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        buf.push(5);
        pushlen!(buf, self.len());
        buf.extend(self);
        1
    }
}
// str
impl<'a> SQParam for &'a str {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        buf.push(6);
        pushlen!(buf, self.len());
        buf.extend(self.as_bytes());
        1
    }
}
impl<'a> SQParam for &'a String {
    fn append_param(&self, q: &mut Vec<u8>) -> usize {
        self.as_str().append_param(q)
    }
}
impl SQParam for String {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        self.as_str().append_param(buf)
    }
}
