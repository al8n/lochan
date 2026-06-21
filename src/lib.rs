#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
//! Single-threaded (`!Send`), `no_std` + `alloc`, no-atomics async channels for
//! thread-per-core runtimes.
//!
//! `lochan` provides [`mpsc`] (multi-producer single-consumer) and [`oneshot`]
//! channels built from `Rc`/`Cell` + wakers — never atomics — so they are strictly
//! lighter than their `Send` counterparts on a single-threaded executor (compio,
//! monoio, glommio, embassy, a tokio `LocalSet`, …). Because they hold `Rc`, every
//! handle is `!Send`: producer and consumer always live on one thread.
//!
//! Each channel exposes a non-blocking **sync** surface (`try_send` / `try_recv`) and
//! an awaitable **async** surface (`recv().await`, bounded `send().await`). The async
//! methods return named `Unpin` + [`FusedFuture`](futures_core::future::FusedFuture)
//! types, so they drop into `select_biased!` without `.fuse()` and can be held in a
//! hand-rolled driver state machine.

extern crate alloc;

pub mod mpsc;
pub mod oneshot;
