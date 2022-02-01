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
//! ## Sync usage
//!
//! Example usage for TLS and non-TLS connection pools are given below.
//!
//! ```no_run
//! use skytable::pool::{ConnectionManager, Pool, TlsPool};
//! use skytable::sync::{Connection, TlsConnection};
//!
//! // non-TLS (TCP pool)
//! let notls_manager = ConnectionManager::new_notls("127.0.0.1".into(), 2003);
//! let notls_pool = Pool::builder()
//!    .max_size(10)
//!    .build(notls_manager)
//!    .unwrap();
//!
//! // TLS pool
//! let tls_manager = ConnectionManager::new_tls(
//!     "127.0.0.1".into(), 2003, "cert.pem".into()
//! );
//! let notls_pool = TlsPool::builder()
//!    .max_size(10)
//!    .build(tls_manager)
//!    .unwrap();
//!```
//!
//! ## Async usage
//!
//! Example usage for TLS and non-TLS connection pools are given below.
//!
//! ```no_run
//! use skytable::pool::{ConnectionManager, AsyncPool, AsyncTlsPool};
//! use skytable::aio::{Connection, TlsConnection};
//! async fn run() {
//!     // non-TLS (TCP pool)
//!     let notls_manager = ConnectionManager::new_notls("127.0.0.1".into(), 2003);
//!     let notls_pool = AsyncPool::builder()
//!        .max_size(10)
//!        .build(notls_manager)
//!        .await
//!        .unwrap();
//!
//!     // TLS pool
//!     let tls_manager = ConnectionManager::new_tls(
//!         "127.0.0.1".into(), 2003, "cert.pem".into()
//!     );
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
#[cfg(any(feature = "sync", feature = "pool"))]
pub use self::sync_impls::{Pool, TlsPool};
#[cfg(any(feature = "sync", feature = "pool"))]
/// [`r2d2`](https://docs.rs/r2d2)'s error type
pub use r2d2::Error as r2d2Error;
// async
#[cfg(any(feature = "aio", feature = "aio-pool"))]
pub use self::async_impls::{Pool as AsyncPool, TlsPool as AsyncTlsPool};
#[cfg(any(feature = "aio", feature = "aio-pool"))]
/// [`bb8`](https://docs.rs/bb8)'s error type
pub use bb8::RunError as bb8Error;

// imports
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
    pub fn new_notls(host: String, port: u16) -> ConnectionManager<C> {
        Self::_new(host, port, None)
    }
    /// Create a new `ConnectionManager` that can be used to configure a TLS connection pool
    pub fn new_tls(host: String, port: u16, cert: String) -> ConnectionManager<C> {
        Self::_new(host, port, Some(cert))
    }
}

#[cfg(any(feature = "sync", feature = "pool"))]
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
            self.run_simple_query(&q)
        }
    }

    cfg_sync_ssl_any! {
        impl PoolableConnection for SyncTlsConnection {
            fn get_connection(host: &str, port: u16, tls_cert: Option<&String>) -> SkyResult<Self> {
                let c = Self::new(
                    &host,
                    port,
                    tls_cert.ok_or(Error::ConfigurationError(
                        "Expected TLS certificate in `ConnectionManager`",
                    ))?,
                )?;
                Ok(c)
            }
            fn run_query(&mut self, q: Query) -> SkyQueryResult {
                self.run_simple_query(&q)
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

#[cfg(any(feature = "aio", feature = "aio-pool"))]
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
            let con = AsyncConnection::new(&host, port).await?;
            Ok(con)
        }
        async fn run_query(&mut self, q: Query) -> SkyQueryResult {
            self.run_simple_query(&q).await
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
                    &host,
                    port,
                    tls_cert.ok_or(Error::ConfigurationError(
                        "Expected TLS certificate in `ConnectionManager`",
                    ))?,
                )
                .await?;
                Ok(con)
            }
            async fn run_query(&mut self, q: Query) -> SkyQueryResult {
                self.run_simple_query(&q).await
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
