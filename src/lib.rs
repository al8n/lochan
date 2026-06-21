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
//!
//! # Panic safety
//!
//! Every operation is panic-safe with respect to the channel's invariants: a panic in
//! user code — a [`Waker`](core::task::Waker)'s `clone`/`wake`/`drop`, or a payload's
//! `Drop` — never double-borrows internal state, strands a parked waiter, or leaks, and
//! the async `send`/`recv` futures commit their outcome *before* waking, so a panicking
//! waker can neither lose a message nor hang the future. One residual remains: `no_std`
//! has no `catch_unwind`, so a *panicking sender waker* (a contract violation — `Waker`
//! vtable ops are expected to be infallible) drops the item that a synchronous
//! [`try_recv`](mpsc::Receiver::try_recv), or an `Unpin` `recv` future, just popped —
//! failing loudly and leaving the channel consistent. The unbounded channel further
//! assumes the global allocator *aborts* on failure (the Rust default and near-universal
//! convention); an allocator that *unwinds* on OOM is unsupported.

extern crate alloc;

mod cell;

pub mod mpsc;
pub mod oneshot;
