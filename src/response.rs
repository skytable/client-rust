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

/*
    Raw response handling
*/

use crate::error::{ClientResult, Error, ParseError};

/// The value directly returned by the server without any additional type parsing and/or casting
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    SInt8(i8),
    SInt16(i16),
    SInt32(i32),
    SInt64(i64),
    Float32(f32),
    Float64(f64),
    Binary(Vec<u8>),
    String(String),
    List(Vec<Self>),
}

#[derive(Debug, PartialEq, Clone)]
/// A row returned by the server
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
    pub fn values(&self) -> &[Value] {
        &self.values
    }
    pub fn into_values(self) -> Vec<Value> {
        self.values
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

/*
    Response traits
*/

pub trait FromResponse: Sized {
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

pub trait FromValue: Sized {
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
