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

//! # Responses
//!
//! This module provides everything that you need to handle responses from the server.
//!
//! ## Example
//!
//! This example shows how you can directly use tuples to get data from the server. Assume that the model is declared as
//! `create model myspace.mymodel(username: string, password: string, null email: string)`
//!
//! ```no_run
//! use skytable::{Config, query};
//!
//! let mut db = Config::new_default("username", "password").connect().unwrap();
//! let q = query!("select username, password, email FROM myspace.mymodel WHERE username = ?", "some_user");
//! let (username, password, email): (String, String, Option<String>) = db.query_parse(&q).unwrap();
//! ```
//!

use {
    crate::error::{ClientResult, Error, ParseError},
    std::ops::Deref,
};

/// The value directly returned by the server without any additional type parsing and/or casting
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    /// A null value
    Null,
    /// A [`bool`]
    Bool(bool),
    /// An [`u8`]
    UInt8(u8),
    /// An [`u16`]
    UInt16(u16),
    /// An [`u32`]
    UInt32(u32),
    /// An [`u64`]
    UInt64(u64),
    /// An [`i8`]
    SInt8(i8),
    /// An [`i16`]
    SInt16(i16),
    /// An [`i32`]
    SInt32(i32),
    /// An [`i64`]
    SInt64(i64),
    /// A [`f32`]
    Float32(f32),
    /// A [`f64`]
    Float64(f64),
    /// A [`Vec<u8>`]
    Binary(Vec<u8>),
    /// A [`String`]
    String(String),
    /// A nested list
    List(Vec<Self>),
}

impl FromValue for Value {
    fn from_value(v: Value) -> ClientResult<Self> {
        Ok(v)
    }
}

impl Value {
    /// Attempt to parse this value into a different type
    pub fn parse<T: FromValue>(self) -> ClientResult<T> {
        T::from_value(self)
    }
    /// Attempt to parse this value into a different type, by cloning the value first
    pub fn parse_cloned<T: FromValue>(&self) -> ClientResult<T> {
        T::from_value(self.clone())
    }
}

#[derive(Debug, PartialEq, Clone)]
/// A row returned by the server
pub struct Row {
    values: Vec<Value>,
}

impl Deref for Row {
    type Target = [Value];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl Row {
    pub(crate) fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
    /// Get a slice of the values in this [`Row`]
    pub fn values(&self) -> &[Value] {
        &self.values
    }
    /// Consume the [`Row`], returning a vector of the [`Value`]s in this row
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
    /// Returns the first [`Value`] in the [`Row`] if present
    pub fn into_first(mut self) -> ClientResult<Value> {
        if self.values.is_empty() {
            Err(Error::ParseError(ParseError::ResponseMismatch))
        } else {
            Ok(self.values.remove(0))
        }
    }
    /// Returns the first [`Value`] in the [`Row`] if present, as the given type
    pub fn into_first_as<T: FromValue>(self) -> ClientResult<T> {
        self.into_first().and_then(FromValue::from_value)
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Self { values }
    }
}

#[derive(Debug, PartialEq, Clone)]
/// A response returned by the server
pub enum Response {
    /// The server returned an empty response, which usually suggests that the query was executed successfully but the server had nothing appropriate to return
    Empty,
    /// The server returned a value
    Value(Value),
    /// The server returned a row
    Row(Row),
    /// A list of rows
    Rows(Vec<Row>),
    /// The server returned an error code
    Error(u16),
}

impl Response {
    /// Attempt to convert the response into the given type
    pub fn parse<T: FromResponse>(self) -> ClientResult<T> {
        T::from_response(self)
    }
}

/*
    Response traits
*/

/// Types that can be parsed from a [`Response`]
///
/// ## Example implementation
///
/// Assume that our schema looks like `create model mymodel(username: string, password: string, null email: string)`. Here's
/// how we can directly get this without any fuss:
///
/// ```no_run
/// use skytable::{
///     ClientResult, Config, query,
///     response::{FromResponse, Response}
/// };
///
/// struct User {
///     username: String,
///     password: String,
///     email: Option<String>,
/// }
///
/// impl FromResponse for User {
///     fn from_response(resp: Response) -> ClientResult<Self> {
///         let (username, password, email): (String, String, Option<String>) = FromResponse::from_response(resp)?;
///         Ok(Self { username, password, email })
///     }
/// }
///
/// let mut db = Config::new_default("username", "password").connect().unwrap();
/// let myuser: User = db.query_parse(
///     &query!("select username, password, email FROM myspace.mymodel WHERE username = ?", "username")
/// ).unwrap();
/// assert_eq!(myuser.username, "bob");
/// ```
pub trait FromResponse: Sized {
    /// Decode the target type from the [`Response`]
    fn from_response(resp: Response) -> ClientResult<Self>;
}

impl FromResponse for () {
    fn from_response(resp: Response) -> ClientResult<Self> {
        match resp {
            Response::Empty => Ok(()),
            Response::Error(e) => Err(Error::ServerError(e)),
            _ => Err(Error::ParseError(ParseError::ResponseMismatch)),
        }
    }
}

/// Any type that can be parsed from a [`Value`]. This is generally meant for use with [`FromResponse`].
pub trait FromValue: Sized {
    /// Attempt to use the value to create an instance of `Self` or throw an error
    fn from_value(v: Value) -> ClientResult<Self>;
}

impl<V: FromValue> FromResponse for V {
    fn from_response(resp: Response) -> ClientResult<Self> {
        match resp {
            Response::Value(v) => V::from_value(v),
            Response::Row(_) | Response::Empty | Response::Rows(_) => {
                Err(Error::ParseError(ParseError::ResponseMismatch))
            }
            Response::Error(e) => Err(Error::ServerError(e)),
        }
    }
}

impl<V: FromValue> FromValue for Option<V> {
    fn from_value(v: Value) -> ClientResult<Self> {
        match v {
            Value::Null => Ok(None),
            v => FromValue::from_value(v).map(|v| Some(v)),
        }
    }
}

macro_rules! from_response_direct {
    ($($ty:ty as $var:ident),* $(,)?) => {
        $(impl FromValue for $ty {
            fn from_value(v: Value) -> ClientResult<Self> {
                match v {
                    Value::$var(capture) => Ok(From::from(capture)),
                    _ => Err(Error::ParseError(ParseError::TypeMismatch)),
                }
            }
        })*
    }
}

from_response_direct!(
    bool as Bool,
    u8 as UInt8,
    u16 as UInt16,
    u32 as UInt32,
    u64 as UInt64,
    i8 as SInt8,
    i16 as SInt16,
    i32 as SInt32,
    i64 as SInt64,
    f32 as Float32,
    f64 as Float64,
    Vec<u8> as Binary,
    Box<[u8]> as Binary,
    String as String,
    Box<str> as String,
    Vec<Value> as List,
);

macro_rules! from_response_row {
    ($(($($elem:ident),*) as $size:literal),* $(,)?) => {
        $(
            impl<$($elem: FromValue),*> FromResponse for ($($elem),*,) {
                fn from_response(resp: Response) -> ClientResult<Self> {
                    let row = match resp {
                        Response::Row(r) => r.into_values(),
                        Response::Empty | Response::Value(_) | Response::Rows(_) => return Err(Error::ParseError(ParseError::ResponseMismatch)),
                        Response::Error(e) => return Err(Error::ServerError(e)),
                    };
                    if row.len() != $size {
                        return Err(Error::ParseError(ParseError::TypeMismatch));
                    }
                    let mut values = row.into_iter();
                    Ok(($($elem::from_value(values.next().unwrap())?),*,))
                }
            }
            impl<$($elem: FromValue),*> FromRow for ($($elem),*,) {
                fn from_row(row: Row) -> ClientResult<Self> {
                    if row.values().len() != $size {
                        return Err(Error::ParseError(ParseError::TypeMismatch));
                    }
                    let mut values = row.into_values().into_iter();
                    Ok(($($elem::from_value(values.next().unwrap())?),*,))
                }
            }
        )*
    }
}

/*
    I know a very easy macro hack to tackle this (which I've used across several other codebases) but I'm just going to leave it like this
    because it's easier on the compiler (and doesn't require me to mess with proc macros which will hence need to be added as a separate dependency),
    but just look at how beautiful this pyramid is; doesn't it remind you of the stuff we used to do back in middle school when learning looping?
    What was it called, "printing patterns" maybe? good ol' days! -- @ohsayan
*/
from_response_row!(
    (A) as 1,
    (A, B) as 2,
    (A, B, C) as 3,
    (A, B, C, D) as 4,
    (A, B, C, D, E) as 5,
    (A, B, C, D, E, F) as 6,
    (A, B, C, D, E, F, G) as 7,
    (A, B, C, D, E, F, G, H) as 8,
    (A, B, C, D, E, F, G, H, I) as 9,
    (A, B, C, D, E, F, G, H, I, J) as 10,
    (A, B, C, D, E, F, G, H, I, J, K) as 11,
    (A, B, C, D, E, F, G, H, I, J, K, L) as 12,
    (A, B, C, D, E, F, G, H, I, J, K, L, M) as 13,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N) as 14,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) as 15,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) as 16,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) as 17,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) as 18,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) as 19,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) as 20,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) as 21,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) as 22,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W) as 23,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X) as 24,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y) as 25,
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z) as 26,
);

impl FromResponse for Row {
    fn from_response(resp: Response) -> ClientResult<Self> {
        match resp {
            Response::Row(r) => Ok(r),
            Response::Error(e) => Err(Error::ServerError(e)),
            _ => Err(Error::ParseError(ParseError::ResponseMismatch)),
        }
    }
}

impl FromResponse for Vec<Row> {
    fn from_response(resp: Response) -> ClientResult<Self> {
        match resp {
            Response::Rows(rows) => Ok(rows),
            Response::Error(e) => Err(Error::ServerError(e)),
            _ => Err(Error::ParseError(ParseError::ResponseMismatch)),
        }
    }
}

/// Trait for parsing a row into a custom type
pub trait FromRow: Sized {
    /// Parse a row into a custom type
    fn from_row(row: Row) -> ClientResult<Self>;
}

impl FromRow for Row {
    fn from_row(row: Row) -> ClientResult<Self> {
        Ok(row)
    }
}

#[derive(Debug, PartialEq)]
/// A collection of rows
///
/// ## Example
/// ```no_run
/// use skytable::{response::Rows, Config, Response, query};
///
/// #[derive(Response)]
/// struct User {
///     username: String,
///     password: String,
/// }
///
/// let mut db = Config::new_default("user", "pass").connect().unwrap();
/// let users: Rows<User> = db.query_parse(&query!("select all * from myapp.users limit ?", 1000u64)).unwrap();
/// assert_eq!(users[0].username, "sayan");
/// ```
pub struct Rows<T: FromRow = Row>(Vec<T>);

impl<T: FromRow> Rows<T> {
    /// Consume the [`Rows`] object and get all the rows as a vector
    pub fn into_rows(self) -> Vec<T> {
        self.0
    }
}

impl<T: FromRow> FromResponse for Rows<T> {
    fn from_response(resp: Response) -> ClientResult<Self> {
        match resp {
            Response::Rows(rows) => {
                let mut ret = vec![];
                for row in rows {
                    ret.push(T::from_row(row)?);
                }
                Ok(Self(ret))
            }
            Response::Error(e) => Err(Error::ServerError(e)),
            _ => Err(Error::ParseError(ParseError::ResponseMismatch)),
        }
    }
}

impl<T: FromRow> Deref for Rows<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A list received from a response
#[derive(Debug, PartialEq, Clone)]
pub struct RList<T = Value>(Vec<T>);

impl<T: FromValue> From<Vec<T>> for RList<T> {
    fn from(values: Vec<T>) -> Self {
        Self(values)
    }
}

impl<T: FromValue> RList<T> {
    /// Returns the values of the list
    pub fn into_values(self) -> Vec<T> {
        self.0
    }
}

impl<T: FromValue> Deref for RList<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: FromValue> FromValue for RList<T> {
    fn from_value(v: Value) -> ClientResult<Self> {
        match v {
            Value::List(l) => {
                let mut ret = Vec::new();
                for value in l {
                    ret.push(T::from_value(value)?);
                }
                Ok(Self(ret))
            }
            _ => Err(Error::ParseError(ParseError::TypeMismatch)),
        }
    }
}

#[test]
fn resp_list_parse() {
    let response_list = Response::Row(Row::new(vec![
        Value::String("sayan".to_owned()),
        Value::List(vec![
            Value::String("c".to_owned()),
            Value::String("assembly".to_owned()),
            Value::String("rust".to_owned()),
        ]),
    ]));
    let (name, languages) = response_list.parse::<(String, RList<String>)>().unwrap();
    assert_eq!(name, "sayan");
    assert_eq!(languages.as_ref(), vec!["c", "assembly", "rust"]);
}
