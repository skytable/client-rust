## Examples

This directory contains multiple examples to help you get started with the Skytable client driver
for Rust.

Example projects include:

- The [`actions`](./actions) project provides examples of how you can use actions
- The [`aio`](./aio) project provides an usage of async features
- The [`basic`](./basic) project provides basic usage of the sync API
- The [`custom-queries`](./custom-queries) project provides examples of running custom queries
- The [`ddl`](./ddl) project provides examples of using DDL queries
- The [`custom-types`](./custom-types) project provides an example of using custom types (like `struct`s with the driver)

## Building

All the examples are part of a workspace and can be simply built by running `cargo build`. (You may need OpenSSL if you want to use TLS features).

> **Important note:** You should **change** the version of the dependency **from this**:
>
> ```toml
> skytable = { git="https://github.com/skytable/client-rust.git", features = ["const-gen"] }
> ```
>
> **to this**:
>
> ```toml
> skytable = { version="*", features = ["const-gen"] }
> ```
>
> (or the version you want to use)
