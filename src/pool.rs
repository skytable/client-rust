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

//! # Connection pooling
//!
//! This module provides ways to create connection pools. For async connections we use [`bb8`] while for sync
//! connections we used [`r2d2`].
//!
//! ## Examples
//! ### Creating an async pool
//! ```no_run
//! use skytable::{pool, Config};
//!
//! const POOL_SIZE: u32 = 32; // we'll have atmost 32 connections in the pool
//! async fn pool() {
//!     let pool = pool::get_async(POOL_SIZE, Config::new_default("username", "password")).await.unwrap();
//!     let mut db = pool.get().await.unwrap();
//! }
//! ```
//! ### Creating a async pool
//! ```no_run
//! use skytable::{pool, Config};
//!
//! const POOL_SIZE: u32 = 32; // we'll have atmost 32 connections in the pool
//! fn pool() {
//!     let pool = pool::get(POOL_SIZE, Config::new_default("username", "password")).unwrap();
//!     let mut db = pool.get().unwrap();
//! }
//! ```
//!
//! To create a pool of TLS connections you can use the [`get_tls`] and [`get_tls_async`] methods, passing a PEM certificate
//! as a string.
//!

use crate::{error::Error, Config, Connection, ConnectionAsync, ConnectionTls, ConnectionTlsAsync};

const QUERY_SYSCTL_STATUS: &str = "sysctl report status";

/// Returns a TCP (skyhash/TCP) connection pool using [`r2d2`]'s default settings and the given maximum pool size
pub fn get(pool_size: u32, config: Config) -> Result<r2d2::Pool<ConnectionMgrTcp>, r2d2::Error> {
    let mgr = ConnectionMgrTcp::new(config);
    r2d2::Pool::builder().max_size(pool_size).build(mgr)
}
/// Returns an async TCP (skyhash/TCP) connection pool using [`bb8`]'s default settings and the given maximum pool size
pub async fn get_async(
    pool_size: u32,
    config: Config,
) -> Result<bb8::Pool<ConnectionMgrTcp>, Error> {
    let mgr = ConnectionMgrTcp::new(config);
    bb8::Pool::builder().max_size(pool_size).build(mgr).await
}
/// Returns a TLS (skyhash/TLS) connection pool using [`r2d2`]'s default settings and the given maximum pool size
pub fn get_tls(
    pool_size: u32,
    config: Config,
    pem_cert: &str,
) -> Result<r2d2::Pool<ConnectionMgrTls>, r2d2::Error> {
    let mgr = ConnectionMgrTls::new(config, pem_cert.into());
    r2d2::Pool::builder().max_size(pool_size).build(mgr)
}
/// Returns an async TLS (skyhash/TCP) connection pool using [`bb8`]'s default settings and the given maximum pool size
pub async fn get_tls_async(
    pool_size: u32,
    config: Config,
    pem_cert: &str,
) -> Result<bb8::Pool<ConnectionMgrTls>, Error> {
    let mgr = ConnectionMgrTls::new(config, pem_cert.into());
    bb8::Pool::builder().max_size(pool_size).build(mgr).await
}

#[derive(Debug, Clone, PartialEq)]
/// A connection manager for Skyhash/TCP connections
pub struct ConnectionMgrTcp {
    config: Config,
}

impl ConnectionMgrTcp {
    /// Create a new connection manager for Skyhash/TCP connections
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl r2d2::ManageConnection for ConnectionMgrTcp {
    type Connection = Connection;
    type Error = Error;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.config.connect()
    }
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.query_parse::<()>(&query!(QUERY_SYSCTL_STATUS))
    }
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for ConnectionMgrTcp {
    type Connection = ConnectionAsync;
    type Error = Error;
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.config.connect_async().await
    }
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.query_parse::<()>(&query!(QUERY_SYSCTL_STATUS)).await
    }
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A connection manager for Skyhash/TLS connections
pub struct ConnectionMgrTls {
    config: Config,
    pem_cert: String,
}

impl ConnectionMgrTls {
    /// Create a new connection manager for Skyhash/TLS connections.
    ///
    /// The `pem_cert` argument must contain your TLS certificate in a PEM format.
    /// **NOTE: The `pem_cert` argument does NOT accept a file path!**
    pub fn new(config: Config, pem_cert: String) -> Self {
        Self { config, pem_cert }
    }
}

impl r2d2::ManageConnection for ConnectionMgrTls {
    type Connection = ConnectionTls;
    type Error = Error;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.config.connect_tls(&self.pem_cert)
    }
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.query_parse::<()>(&query!(QUERY_SYSCTL_STATUS))
    }
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for ConnectionMgrTls {
    type Connection = ConnectionTlsAsync;
    type Error = Error;
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.config.connect_tls_async(&self.pem_cert).await
    }
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.query_parse::<()>(&query!(QUERY_SYSCTL_STATUS)).await
    }
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
