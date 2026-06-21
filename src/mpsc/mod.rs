//! Multi-producer, single-consumer channel.
//!
//! `!Send`, no-atomics; [`bounded`] and [`unbounded`] flavors. Senders are `Clone`
//! (multi-producer); the receiver is single-consumer. Each flavor offers a
//! non-blocking `try_*` surface and an awaitable `recv` (and, when bounded, `send`)
//! whose futures are `Unpin` + [`FusedFuture`](futures_core::future::FusedFuture).

mod chan;
mod channel;
mod error;
mod list;
mod recv;
mod ring;
mod send;

pub use channel::{Receiver, Sender};
pub use error::{SendError, TryRecvError, TrySendError};
pub use recv::Recv;
pub use send::Send;

use chan::Chan;

/// Creates a bounded channel that holds at most `cap` queued items.
///
/// [`Sender::try_send`] reports [`TrySendError::Full`] when the queue is at capacity.
/// `cap` must be non-zero: a zero-capacity rendezvous cannot be represented on a
/// single thread, where the sole sender and receiver share it and a hand-off that
/// parked the thread would deadlock.
///
/// # Panics
///
/// Panics if `cap == 0`.
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
  assert!(
    cap > 0,
    "lochan::mpsc::bounded requires a non-zero capacity"
  );
  let chan = Chan::bounded(cap);
  (Sender::new(chan.clone()), Receiver::new(chan))
}

/// Creates an unbounded channel: [`Sender::try_send`] never reports
/// [`TrySendError::Full`], growing the queue a block at a time as needed.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
  let chan = Chan::unbounded();
  (Sender::new(chan.clone()), Receiver::new(chan))
}

#[cfg(test)]
mod tests;
