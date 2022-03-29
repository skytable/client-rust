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

//! # Connection pooling
//!
//! This module provides utilities to use connection pooling. As we already know, it is far more
//! efficient to maintain a number of live connections to a database and share them across multiple
//! "worker threads", using a "connection pool" because creating individual connections whenever
//! a worker receives a task is slow while maintaining a connection per worker might be cumbersome
//! to implement.
//!
//! To provide connection pooling, we use [`r2d2`] for a sync connection pool while we use
//! [`bb8`](https://docs.rs/bb8) to provide an async connection pool.
//!
//! ## Basic usage
//!
//! For your convenience, we have provided defaults for you to build connection pools.
//! - [`get()`]: Returns a sync TCP pool
//! - [`get_tls()`]: Returns a sync TLS pool
//! - [`get_async()`]: Returns an async TCP pool
//! - [`get_tls_async()`]: Returns an async TLS pool
//!
//! Below, we have created TCP/TLS pools with a size of 10 (you can choose anything that you need)
//! and run some actions for demonstration.
//!
//! ### Sync Usage
//!
//! ```no_run
//! use skytable::{pool, actions::Actions};
//!
//! let notls_pool = pool::get("127.0.0.1", 2003, 10).unwrap();
//! notls_pool.get().unwrap().set("x", "100").unwrap();
//!
//! let tls_pool = pool::get_tls("127.0.0.1", 2004, "cert.pem", 10).unwrap();
//! let ret: u8 = tls_pool.get().unwrap().get("x").unwrap();
//!
//! assert_eq!(ret, 100);
//! ```
//!
//! ### Async Usage
//!
//! ```no_run
//! use skytable::pool;
//! use skytable::actions::AsyncActions;
//!
//! async fn run() {
//!     let notls_pool = pool::get_async("127.0.0.1", 2003, 10).await.unwrap();
//!     notls_pool.get().await.unwrap().set("x", "100").await.unwrap();
//!
//!     let tls_pool = pool::get_tls_async("127.0.0.1", 2004, "cert.pem", 10).await.unwrap();
//!     let ret: u8 = tls_pool.get().await.unwrap().get("x").await.unwrap();
//!
//!     assert_eq!(ret, 100);
//! }
//! ```
//!
//! ## Advanced usage
//! If you want to configure a pool with custom settings, then you can use
//! [r2d2's `Builder`](https://docs.rs/r2d2/0.8.9/r2d2/struct.Builder.html) or
//! [bb8's `Builder`](https://docs.rs/bb8/0.7.1/bb8/struct.Builder.html) to configure your pool.
//!
//! ### Sync usage
//!
//! Example usage for TLS and non-TLS connection pools are given below.
//!
//! ```no_run
//! use skytable::pool::{ConnectionManager, Pool, TlsPool};
//! use skytable::sync::{Connection, TlsConnection};
//!
//! // non-TLS (TCP pool)
//! let notls_manager = ConnectionManager::new_notls("127.0.0.1", 2003);
//! let notls_pool = Pool::builder()
//!    .max_size(10)
//!    .build(notls_manager)
//!    .unwrap();
//!
//! // TLS pool
//! let tls_manager = ConnectionManager::new_tls("127.0.0.1", 2003, "cert.pem");
//! let notls_pool = TlsPool::builder()
//!    .max_size(10)
//!    .build(tls_manager)
//!    .unwrap();
//!```
//!
//! ### Async usage
//!
//! Example usage for TLS and non-TLS connection pools are given below.
//!
//! ```no_run
//! use skytable::pool::{ConnectionManager, AsyncPool, AsyncTlsPool};
//! use skytable::aio::{Connection, TlsConnection};
//! async fn run() {
//!     // non-TLS (TCP pool)
//!     let notls_manager = ConnectionManager::new_notls("127.0.0.1", 2003);
//!     let notls_pool = AsyncPool::builder()
//!        .max_size(10)
//!        .build(notls_manager)
//!        .await
//!        .unwrap();
//!
//!     // TLS pool
//!     let tls_manager = ConnectionManager::new_tls("127.0.0.1", 2003, "cert.pem");
//!     let notls_pool = AsyncTlsPool::builder()
//!        .max_size(10)
//!        .build(tls_manager)
//!        .await
//!        .unwrap();
//! }
//!```
//!

// re-exports
// sync
cfg_sync! {
    /// [`r2d2`](https://docs.rs/r2d2)'s error type
    pub use r2d2::Error as r2d2Error;
    pub use self::sync_impls::Pool;
    /// Returns a TCP pool of the specified size and provided settings
    pub fn get(host: impl ToString, port: u16, max_size: u32) -> Result<Pool, r2d2Error> {
        Pool::builder()
            .max_size(max_size)
            .build(ConnectionManager::new_notls(host.to_string(), port))
    }
}
cfg_sync_ssl_any! {
    pub use self::sync_impls::TlsPool;
    /// Returns a TLS pool of the specified size and provided settings
    pub fn get_tls(host: impl ToString, port: u16, cert: impl ToString, max_size: u32) -> Result<TlsPool, r2d2Error> {
        TlsPool::builder()
            .max_size(max_size)
            .build(
                ConnectionManager::new_tls(host.to_string(), port, cert)
            )
    }
}

// async
cfg_async! {
    /// [`bb8`](https://docs.rs/bb8)'s error type
    pub use bb8::RunError as bb8Error;
    pub use self::async_impls::Pool as AsyncPool;
    use crate::error::Error;
    /// Returns an async TCP pool of the specified size and provided settings
    pub async fn get_async(host: impl ToString, port: u16, max_size: u32) -> Result<AsyncPool, Error> {
        AsyncPool::builder()
            .max_size(max_size)
            .build(ConnectionManager::new_notls(host.to_string(), port)).await
    }
}
cfg_async_ssl_any! {
    pub use self::async_impls::TlsPool as AsyncTlsPool;
    /// Returns an async TLS pool of the specified size and provided settings
    pub async fn get_tls_async(host: impl ToString, port: u16, cert: impl ToString, max_size: u32) -> Result<AsyncTlsPool, Error> {
        AsyncTlsPool::builder()
            .max_size(max_size)
            .build(ConnectionManager::new_tls(host.to_string(), port, cert)).await
    }
}

use core::marker::PhantomData;

#[derive(Debug, Clone)]
/// A [`ConnectionManager`] for connection pools. See the [module level documentation](crate::pool)
/// for examples and more information
pub struct ConnectionManager<C> {
    host: String,
    port: u16,
    cert: Option<String>,
    _m: PhantomData<C>,
}

impl<C> ConnectionManager<C> {
    fn _new(host: String, port: u16, cert: Option<String>) -> Self {
        Self {
            host,
            port,
            cert,
            _m: PhantomData,
        }
    }
}

impl<C> ConnectionManager<C> {
    /// Create a new `ConnectionManager` that can be used to configure a non-TLS connection pool
    pub fn new_notls(host: impl ToString, port: u16) -> ConnectionManager<C> {
        Self::_new(host.to_string(), port, None)
    }
    /// Create a new `ConnectionManager` that can be used to configure a TLS connection pool
    pub fn new_tls(host: impl ToString, port: u16, cert: impl ToString) -> ConnectionManager<C> {
        Self::_new(host.to_string(), port, Some(cert.to_string()))
    }
}

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
mod sync_impls {
    use super::ConnectionManager;
    use crate::sync::Connection as SyncConnection;
    cfg_sync_ssl_any! {
        use crate::sync::TlsConnection as SyncTlsConnection;
    }
    use crate::{
        error::{Error, SkyhashError},
        Element, Query, SkyQueryResult, SkyResult,
    };
    use r2d2::ManageConnection;

    /// A non-TLS connection pool to Skytable
    pub type Pool = r2d2::Pool<ConnectionManager<SyncConnection>>;
    cfg_sync_ssl_any! {
        /// A TLS connection pool to Skytable
        pub type TlsPool = r2d2::Pool<ConnectionManager<SyncTlsConnection>>;
    }

    pub trait PoolableConnection: Send + Sync + Sized {
        fn get_connection(host: &str, port: u16, tls_cert: Option<&String>) -> SkyResult<Self>;
        fn run_query(&mut self, q: Query) -> SkyQueryResult;
    }

    impl PoolableConnection for SyncConnection {
        fn get_connection(host: &str, port: u16, _tls_cert: Option<&String>) -> SkyResult<Self> {
            let c = Self::new(host, port)?;
            Ok(c)
        }
        fn run_query(&mut self, q: Query) -> SkyQueryResult {
            self.run_query_raw(&q)
        }
    }

    cfg_sync_ssl_any! {
        impl PoolableConnection for SyncTlsConnection {
            fn get_connection(host: &str, port: u16, tls_cert: Option<&String>) -> SkyResult<Self> {
                let c = Self::new(
                    host,
                    port,
                    tls_cert.ok_or(Error::ConfigurationError(
                        "Expected TLS certificate in `ConnectionManager`",
                    ))?,
                )?;
                Ok(c)
            }
            fn run_query(&mut self, q: Query) -> SkyQueryResult {
                self.run_query_raw(&q)
            }
        }
    }
    impl<C: PoolableConnection + 'static> ManageConnection for ConnectionManager<C> {
        type Error = Error;
        type Connection = C;
        fn connect(&self) -> Result<Self::Connection, Self::Error> {
            C::get_connection(self.host.as_ref(), self.port, self.cert.as_ref())
        }
        fn is_valid(&self, con: &mut Self::Connection) -> Result<(), Self::Error> {
            let q = crate::query!("HEYA");
            match con.run_query(q)? {
                Element::String(st) if st.eq("HEY!") => Ok(()),
                _ => Err(Error::SkyError(SkyhashError::UnexpectedResponse)),
            }
        }
        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }
}

#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
mod async_impls {
    use super::ConnectionManager;
    use crate::aio::Connection as AsyncConnection;
    cfg_async_ssl_any! {
        use crate::aio::TlsConnection as AsyncTlsConnection;
    }
    use crate::{
        error::{Error, SkyhashError},
        Element, Query, SkyQueryResult, SkyResult,
    };
    use async_trait::async_trait;
    use bb8::{ManageConnection, PooledConnection};

    /// An asynchronous non-TLS connection pool to Skytable
    pub type Pool = bb8::Pool<ConnectionManager<AsyncConnection>>;
    cfg_async_ssl_any! {
        /// An asynchronous TLS connection pool to Skytable
        pub type TlsPool = bb8::Pool<ConnectionManager<AsyncTlsConnection>>;
    }
    #[async_trait]
    pub trait PoolableConnection: Send + Sync + Sized {
        async fn get_connection(
            host: &str,
            port: u16,
            tls_cert: Option<&String>,
        ) -> SkyResult<Self>;
        async fn run_query(&mut self, q: Query) -> SkyQueryResult;
    }

    #[async_trait]
    impl PoolableConnection for AsyncConnection {
        async fn get_connection(
            host: &str,
            port: u16,
            _tls_cert: Option<&String>,
        ) -> SkyResult<Self> {
            let con = AsyncConnection::new(host, port).await?;
            Ok(con)
        }
        async fn run_query(&mut self, q: Query) -> SkyQueryResult {
            self.run_query_raw(&q).await
        }
    }

    cfg_async_ssl_any! {
        #[async_trait]
        impl PoolableConnection for AsyncTlsConnection {
            async fn get_connection(
                host: &str,
                port: u16,
                tls_cert: Option<&String>,
            ) -> SkyResult<Self> {
                let con = AsyncTlsConnection::new(
                    host,
                    port,
                    tls_cert.ok_or(Error::ConfigurationError(
                        "Expected TLS certificate in `ConnectionManager`",
                    ))?,
                )
                .await?;
                Ok(con)
            }
            async fn run_query(&mut self, q: Query) -> SkyQueryResult {
                self.run_query_raw(&q).await
            }
        }
    }

    #[async_trait]
    impl<C: PoolableConnection + 'static> ManageConnection for ConnectionManager<C> {
        type Connection = C;
        type Error = Error;
        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            C::get_connection(&self.host, self.port, self.cert.as_ref()).await
        }
        async fn is_valid(&self, con: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
            match con.run_query(crate::query!("HEYA")).await? {
                Element::String(st) if st.eq("HEY!") => Ok(()),
                _ => Err(Error::SkyError(SkyhashError::UnexpectedResponse)),
            }
        }
        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }
}
