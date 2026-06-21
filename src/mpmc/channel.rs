//! The `mpmc` channel handles, [`Sender`] and [`Receiver`], shared by both flavors.

use core::{
  cell::Cell,
  pin::Pin,
  task::{Context, Poll},
};
use std::rc::Rc;

use futures_core::stream::{FusedStream, Stream};

use super::{
  chan::Chan,
  error::{TryRecvError, TrySendError},
  recv::Recv,
  send::Send,
};

/// The sending half of an `mpmc` channel. Cloneable — every clone is another
/// producer.
pub struct Sender<T> {
  chan: Rc<Chan<T>>,
}

impl<T> Sender<T> {
  pub(super) fn new(chan: Rc<Chan<T>>) -> Self {
    Self { chan }
  }

  /// Pushes an item without waiting. Returns [`TrySendError::Full`] when the channel
  /// is at capacity, or [`TrySendError::Closed`] when every receiver is gone; either
  /// way the item is carried back.
  #[inline(always)]
  pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    if !self.chan.receiver_alive() {
      return Err(TrySendError::Closed(item));
    }
    match self.chan.try_push(item) {
      Ok(()) => {
        self.chan.wake_receivers();
        Ok(())
      }
      Err(item) => Err(TrySendError::Full(item)),
    }
  }

  /// The channel's capacity, or `None` if the channel is unbounded.
  ///
  /// A bounded channel buffers at most this many items. A receive interrupted by a
  /// panicking sender waker keeps its one recovered item in transit until the next
  /// receive, so the channel can momentarily hold `capacity() + 1` and
  /// [`len`](Self::len) can briefly exceed this — never by more than one.
  pub fn capacity(&self) -> Option<usize> {
    self.chan.cap()
  }

  /// The number of items currently in the channel.
  ///
  /// Counts a recovered in-transit item left by a receive interrupted by a panicking
  /// sender waker, so for a bounded channel this can briefly be `capacity() + 1`.
  pub fn len(&self) -> usize {
    self.chan.len()
  }

  /// Returns `true` if the channel has no items left to receive.
  pub fn is_empty(&self) -> bool {
    self.chan.is_empty()
  }

  /// Returns `true` if the channel is at capacity.
  pub fn is_full(&self) -> bool {
    self.chan.is_full()
  }

  /// Returns `true` once every receiver has been dropped.
  pub fn is_closed(&self) -> bool {
    !self.chan.receiver_alive()
  }

  /// Returns a future that sends `item`, awaiting capacity when a bounded channel is
  /// full. Resolves to [`SendError`](super::SendError) (carrying the item) if every
  /// receiver is gone. The future is `FusedFuture` (and `Unpin` when `T: Unpin`).
  #[inline(always)]
  pub fn send(&self, item: T) -> Send<'_, T> {
    Send::new(self, item)
  }

  pub(super) fn chan(&self) -> &Chan<T> {
    &self.chan
  }
}

impl<T> Clone for Sender<T> {
  fn clone(&self) -> Self {
    self.chan.incr_senders();
    Self {
      chan: self.chan.clone(),
    }
  }
}

impl<T> Drop for Sender<T> {
  fn drop(&mut self) {
    if self.chan.decr_senders() == 1 {
      // Last sender gone: wake every parked receiver so each observes disconnect.
      self.chan.wake_receivers();
    }
  }
}

/// The receiving half of an `mpmc` channel. Cloneable — every clone is another
/// consumer, and the channel stays open for sends until the last one drops.
pub struct Receiver<T> {
  chan: Rc<Chan<T>>,
  /// This receiver's stream-registration id, used by the [`Stream`] impl so repeated
  /// `poll_next` calls replace the registration rather than accumulate entries. Each
  /// clone is an independent consumer with its own slot.
  stream_waker_id: Cell<Option<u64>>,
}

impl<T> Receiver<T> {
  pub(super) fn new(chan: Rc<Chan<T>>) -> Self {
    Self {
      chan,
      stream_waker_id: Cell::new(None),
    }
  }

  /// Pops an item without waiting. Returns [`TryRecvError::Empty`] when nothing is
  /// queued, or [`TryRecvError::Disconnected`] when the queue is empty and every
  /// sender has dropped.
  #[inline(always)]
  pub fn try_recv(&self) -> Result<T, TryRecvError> {
    match self.chan.try_take() {
      Some(item) => Ok(item),
      None if self.chan.senders() == 0 => Err(TryRecvError::Disconnected),
      None => Err(TryRecvError::Empty),
    }
  }

  /// Returns a future that resolves to the next item, or `None` once the channel is
  /// empty and every sender has dropped. The future is `Unpin` + `FusedFuture`.
  ///
  /// Several consumers may await concurrently: a delivered item goes to exactly one of
  /// them, while the rest stay parked.
  #[inline(always)]
  pub fn recv(&self) -> Recv<'_, T> {
    Recv::new(self)
  }

  /// Returns an iterator over the items currently queued, without waiting: it yields
  /// each ready item and stops at the first empty-or-disconnected poll.
  #[inline]
  pub fn try_iter(&self) -> TryIter<'_, T> {
    TryIter(self)
  }

  /// The shared recv state machine, used by the [`Recv`] future and the [`Stream`]
  /// impl. Pops an item (waking parked senders), reports `None` once disconnected, or
  /// registers `cx`'s waker (threading the caller's `waker_id` slot so a re-poll
  /// replaces rather than accumulates) and rechecks — `Recv`'s golden panic-safe path.
  #[inline]
  pub(super) fn poll_recv(
    &self,
    cx: &mut Context<'_>,
    waker_id: &Cell<Option<u64>>,
  ) -> Poll<Option<T>> {
    let chan = &self.chan;
    // Drop a stale registration from a prior park BEFORE popping: its waker drop runs
    // user code, but no item has been read out yet, so a panic loses nothing (the
    // future simply re-polls). Guarded so the common first-poll ready path (no prior
    // registration) skips the list scan.
    if let Some(old) = waker_id.take() {
      chan.remove_recv_waker(old);
    }
    if let Some(item) = chan.try_take() {
      // `try_take` pops then wakes parked senders with the item parked in the redelivery
      // slot, so a panicking sender waker re-queues it rather than dropping a stack local.
      return Poll::Ready(Some(item));
    }
    if chan.senders() == 0 {
      // Empty and every sender is gone — the channel is disconnected.
      return Poll::Ready(None);
    }
    let id = chan.add_recv_waker(cx.waker());
    // Record the id through the shared `Cell` BEFORE the recheck below, so even if a later
    // step unwinds, the caller still knows which registration to remove — the id is never
    // orphaned in `recv_wakers`.
    waker_id.set(Some(id));
    // RECHECK: add_recv_waker's clone/drop callbacks may have pushed an item or dropped
    // the last sender; re-check so a wake delivered during registration is not lost. On the
    // ready-item branch we do NOT remove the just-registered waker — dropping it could
    // panic and lose the popped item — so it is stale only on that rare re-entrant path and
    // is removed at a safe point (the next poll's top, the future's `Drop`, or the
    // receiver's `Drop`). On the disconnect branch, however, NO item is in flight, so we
    // remove it here: a side-effecting waker clone can drop the last sender during
    // registration, and otherwise a terminal `Recv`/`FusedStream` would retain the waker.
    if let Some(item) = chan.try_take() {
      return Poll::Ready(Some(item));
    }
    if chan.senders() == 0 {
      if let Some(id) = waker_id.take() {
        chan.remove_recv_waker(id);
      }
      return Poll::Ready(None);
    }
    Poll::Pending
  }

  pub(super) fn chan(&self) -> &Chan<T> {
    &self.chan
  }

  /// The number of items currently in the channel.
  ///
  /// Counts a recovered in-transit item left by a receive interrupted by a panicking
  /// sender waker, so for a bounded channel this can briefly be `capacity() + 1`.
  pub fn len(&self) -> usize {
    self.chan.len()
  }

  /// Returns `true` if the channel has no items left to receive.
  pub fn is_empty(&self) -> bool {
    self.chan.is_empty()
  }
}

impl<T> Clone for Receiver<T> {
  fn clone(&self) -> Self {
    self.chan.incr_receivers();
    Self {
      chan: self.chan.clone(),
      stream_waker_id: Cell::new(None),
    }
  }
}

impl<T> Drop for Receiver<T> {
  fn drop(&mut self) {
    // Decrement the receiver count FIRST, before the stream-waker drop below (user
    // code). Two reasons: once this is the last receiver, `receiver_alive()` is false,
    // so a re-entrant `try_send` from a panicking waker drop fails closed instead of
    // queuing an item the drain would silently discard; and a panic in that drop cannot
    // skip the decrement, which would leave senders believing a receiver is still alive.
    let was_last = self.chan.decr_receivers() == 1;
    if !was_last {
      // Other receivers remain: the channel stays open and they own the queue. Remove
      // only this receiver's own stream registration.
      if let Some(id) = self.stream_waker_id.take() {
        self.chan.remove_recv_waker(id);
      }
      return;
    }
    // Last receiver gone. Arm a guard that frees the queue AND removes this receiver's
    // stream registration, so an unwind from the `wake_senders` below skips NEITHER: a
    // queued payload may own a `Sender` (an `Rc` cycle through `Chan`), and the stream
    // registration would otherwise be retained in `recv_wakers` while a live `Sender`
    // keeps `Chan` alive. The id is captured BEFORE the wake so the wake cannot skip it.
    // (A second panic during the guard — a payload, or the stream waker drop itself —
    // aborts: the same multi-panic limit documented on `wake_all`.)
    struct Cleanup<'a, T> {
      chan: &'a Chan<T>,
      stream_id: Option<u64>,
    }
    impl<T> Drop for Cleanup<'_, T> {
      fn drop(&mut self) {
        // Remove the stream registration in its OWN guard, armed BEFORE `drain` below, so
        // a panicking payload drop inside `drain` (which `drain` resumes after continuing)
        // still removes it on the unwind rather than leaking it in `recv_wakers`.
        struct RemoveStream<'a, T> {
          chan: &'a Chan<T>,
          id: Option<u64>,
        }
        impl<T> Drop for RemoveStream<'_, T> {
          fn drop(&mut self) {
            if let Some(id) = self.id.take() {
              self.chan.remove_recv_waker(id);
            }
          }
        }
        let _remove = RemoveStream {
          chan: self.chan,
          id: self.stream_id.take(),
        };
        // Drain the queue; its own guard continues past a panicking payload, so the `Rc`
        // cycle is always broken. `_remove` then removes the stream registration — on the
        // normal path or on a drain unwind.
        self.chan.drain();
      }
    }
    let cleanup = Cleanup {
      chan: &self.chan,
      stream_id: self.stream_waker_id.take(),
    };
    // Wake parked senders so each observes disconnect — before the guard's payload/waker
    // drops, so a panicking one cannot strand them parked.
    self.chan.wake_senders();
    drop(cleanup);
  }
}

impl<T> Stream for Receiver<T> {
  type Item = T;

  /// Polls for the next item — equivalent to polling [`Receiver::recv`]; yields `None`
  /// once the channel is empty and every sender has dropped.
  #[inline]
  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
    let this = self.get_mut();
    // Pass the `Cell` straight through so `poll_recv` records the registration id into it
    // directly — no copy-out then write-back that a panic in between could skip, which
    // would orphan the just-registered waker in `recv_wakers`.
    this.poll_recv(cx, &this.stream_waker_id)
  }
}

impl<T> FusedStream for Receiver<T> {
  /// Terminated once the channel is drained and every sender has dropped — past that,
  /// `poll_next` only returns `None`.
  ///
  /// Stays non-terminal while a stream-waker registration is still outstanding, even when
  /// drained and sender-less: a recheck-Ready poll can deliver the final item while leaving
  /// its waker registered (it cannot be removed there without risking the popped item), so
  /// reporting termination then would let a `FusedStream` consumer stop polling and strand
  /// that waker. One more poll clears it at the poll-top stale-remove and returns `None`.
  #[inline]
  fn is_terminated(&self) -> bool {
    self.stream_waker_id.get().is_none() && self.chan.senders() == 0 && self.chan.is_empty()
  }
}

/// A non-blocking iterator over the currently-available items of a [`Receiver`],
/// returned by [`Receiver::try_iter`]. `next` yields each queued item and stops at the
/// first empty-or-disconnected poll.
pub struct TryIter<'a, T>(&'a Receiver<T>);

impl<T> Iterator for TryIter<'_, T> {
  type Item = T;

  #[inline]
  fn next(&mut self) -> Option<T> {
    self.0.try_recv().ok()
  }
}
