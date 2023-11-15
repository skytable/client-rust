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

// internal modules
#[macro_use]
mod macros;
mod protocol;
// public modules
pub mod aio;
pub mod config;
pub mod error;
pub mod pool;
pub mod query;
pub mod response;
pub mod sync;
// re-exports
pub use {
    aio::{ConnectionAsync, ConnectionTlsAsync},
    config::Config,
    query::Query,
    sync::{Connection, ConnectionTls},
};

/// we use a 4KB read buffer by default; allow this to be changed
const BUFSIZE: usize = 4 * 1024;
