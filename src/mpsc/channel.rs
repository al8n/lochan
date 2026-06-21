//! The `mpsc` channel handles, [`Sender`] and [`Receiver`], shared by both flavors.

use core::{
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

/// The sending half of an `mpsc` channel. Cloneable — every clone is another
/// producer.
pub struct Sender<T> {
  chan: Rc<Chan<T>>,
}

impl<T> Sender<T> {
  pub(super) fn new(chan: Rc<Chan<T>>) -> Self {
    Self { chan }
  }

  /// Pushes an item without waiting. Returns [`TrySendError::Full`] when the channel
  /// is at capacity, or [`TrySendError::Closed`] when the receiver is gone; either
  /// way the item is carried back.
  #[inline(always)]
  pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    if !self.chan.receiver_alive() {
      return Err(TrySendError::Closed(item));
    }
    match self.chan.try_push(item) {
      Ok(()) => {
        self.chan.wake_receiver();
        Ok(())
      }
      Err(item) => Err(TrySendError::Full(item)),
    }
  }

  /// The channel's capacity, or `None` if the channel is unbounded.
  pub fn capacity(&self) -> Option<usize> {
    self.chan.cap()
  }

  /// The number of currently-queued items.
  pub fn len(&self) -> usize {
    self.chan.len()
  }

  /// Returns `true` if no items are queued.
  pub fn is_empty(&self) -> bool {
    self.chan.is_empty()
  }

  /// Returns `true` if the channel is at capacity.
  pub fn is_full(&self) -> bool {
    self.chan.is_full()
  }

  /// Returns `true` once the receiver has been dropped.
  pub fn is_closed(&self) -> bool {
    !self.chan.receiver_alive()
  }

  /// Returns a future that sends `item`, awaiting capacity when a bounded channel is
  /// full. Resolves to [`SendError`](super::SendError) (carrying the item) if the
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
      // Last sender gone: wake a parked receiver so it observes disconnect.
      self.chan.wake_receiver();
    }
  }
}

/// The receiving half of an `mpsc` channel. Single-consumer — not `Clone`, so there is
/// exactly one receiver by construction. Its methods take `&self` (the queue lives
/// behind an `Rc`), so a held [`recv`](Self::recv) future and a synchronous
/// [`try_recv`](Self::try_recv) drain can run against the same receiver. As the sole
/// consumer, do not hold two `recv` futures at once — that would race their wakers.
pub struct Receiver<T> {
  chan: Rc<Chan<T>>,
}

impl<T> Receiver<T> {
  pub(super) fn new(chan: Rc<Chan<T>>) -> Self {
    Self { chan }
  }

  /// Pops an item without waiting. Returns [`TryRecvError::Empty`] when nothing is
  /// queued, or [`TryRecvError::Disconnected`] when the queue is empty and every
  /// sender has dropped.
  #[inline(always)]
  pub fn try_recv(&self) -> Result<T, TryRecvError> {
    match self.chan.pop() {
      Some(item) => {
        self.chan.wake_senders();
        Ok(item)
      }
      None if self.chan.senders() == 0 => Err(TryRecvError::Disconnected),
      None => Err(TryRecvError::Empty),
    }
  }

  /// Returns a future that resolves to the next item, or `None` once the channel is
  /// empty and every sender has dropped. The future is `Unpin` + `FusedFuture`.
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
  /// registers `cx`'s waker and rechecks — `Recv`'s golden panic-safe path.
  #[inline]
  pub(super) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
    let chan = &self.chan;
    if let Some(item) = chan.pop() {
      // A slot freed — wake parked senders (bounded backpressure).
      chan.wake_senders();
      return Poll::Ready(Some(item));
    }
    if chan.senders() == 0 {
      // Empty and every sender is gone — the channel is disconnected.
      return Poll::Ready(None);
    }
    chan.register_recv_waker(cx.waker());
    // RECHECK: register_recv_waker's clone/drop callbacks may have pushed an item or
    // dropped the last sender; re-check so a wake delivered during registration is not
    // lost. We do NOT clear the just-registered waker on the Ready branches — dropping
    // it could panic and lose the popped item. It is stale only on this rare re-entrant
    // path, and is dropped at a safe point (the next registration, wake, or receiver
    // drop) where no popped item is in flight.
    if let Some(item) = chan.pop() {
      chan.wake_senders();
      return Poll::Ready(Some(item));
    }
    if chan.senders() == 0 {
      return Poll::Ready(None);
    }
    Poll::Pending
  }

  pub(super) fn chan(&self) -> &Chan<T> {
    &self.chan
  }

  /// The number of currently-queued items.
  pub fn len(&self) -> usize {
    self.chan.len()
  }

  /// Returns `true` if no items are queued.
  pub fn is_empty(&self) -> bool {
    self.chan.is_empty()
  }
}

impl<T> Drop for Receiver<T> {
  fn drop(&mut self) {
    self.chan.clear_receiver();
    // Arm the drain guard so the queue is freed even if anything below panics: a queued
    // payload may own a `Sender` (an `Rc` cycle through `Chan`), so a skipped drain would
    // leak it. Senders are woken before any payload `Drop` runs, so a panicking payload
    // cannot strand them parked.
    struct DrainOnDrop<'a, T>(&'a Chan<T>);
    impl<T> Drop for DrainOnDrop<'_, T> {
      fn drop(&mut self) {
        self.0.drain();
      }
    }
    let drain = DrainOnDrop(&self.chan);
    self.chan.wake_senders();
    // Clear any waker left registered by a recheck-Ready completion, so it is not
    // retained while a `Sender` keeps `Chan` alive. The guard still drains if this
    // (waker drop) panics.
    self.chan.clear_recv_waker();
    drop(drain);
  }
}

impl<T> Stream for Receiver<T> {
  type Item = T;

  /// Polls for the next item — equivalent to polling [`Receiver::recv`]; yields `None`
  /// once the channel is empty and every sender has dropped.
  #[inline]
  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
    self.get_mut().poll_recv(cx)
  }
}

impl<T> FusedStream for Receiver<T> {
  /// Terminated once the channel is drained and every sender has dropped — past that,
  /// `poll_next` only returns `None`.
  #[inline]
  fn is_terminated(&self) -> bool {
    self.chan.senders() == 0 && self.chan.is_empty()
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
