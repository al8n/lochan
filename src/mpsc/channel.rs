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
  #[inline(always)]
  pub(super) fn new(chan: Rc<Chan<T>>) -> Self {
    Self { chan }
  }

  /// Pushes an item without waiting. Returns [`TrySendError::Full`] when the channel
  /// is at capacity, or [`TrySendError::Closed`] when the receiver is gone; either
  /// way the item is carried back.
  #[inline(always)]
  pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    if !self.chan.receiver_alive() || self.chan.is_closed() {
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
  #[inline(always)]
  pub fn capacity(&self) -> Option<usize> {
    self.chan.cap()
  }

  /// The number of currently-queued items.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.chan.len()
  }

  /// Returns `true` if no items are queued.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.chan.is_empty()
  }

  /// Returns `true` if the channel is at capacity.
  #[inline(always)]
  pub fn is_full(&self) -> bool {
    self.chan.is_full()
  }

  /// Returns `true` once the receiver has been dropped or the channel has been
  /// [`close`](Self::close)d.
  #[inline(always)]
  pub fn is_closed(&self) -> bool {
    !self.chan.receiver_alive() || self.chan.is_closed()
  }

  /// Closes the channel. Every subsequent send fails closed, and the receiver observes
  /// disconnect once it has drained what is already queued. Returns `true` if this call
  /// closed a still-open channel — `false` if it was already closed, whether by a prior
  /// `close` or by the receiver dropping. Callable through any clone.
  #[inline(always)]
  pub fn close(&self) -> bool {
    if self.is_closed() {
      return false;
    }
    self.chan.close();
    true
  }

  /// Returns a future that sends `item`, awaiting capacity when a bounded channel is
  /// full. Resolves to [`SendError`](super::SendError) (carrying the item) if the
  /// receiver is gone. The future is `FusedFuture` (and `Unpin` when `T: Unpin`).
  #[inline(always)]
  pub fn send(&self, item: T) -> Send<'_, T> {
    Send::new(self, item)
  }

  #[inline(always)]
  pub(super) fn chan(&self) -> &Chan<T> {
    &self.chan
  }
}

impl<T> Clone for Sender<T> {
  #[inline]
  fn clone(&self) -> Self {
    self.chan.incr_senders();
    Self {
      chan: self.chan.clone(),
    }
  }
}

impl<T> Drop for Sender<T> {
  #[inline]
  fn drop(&mut self) {
    if self.chan.decr_senders() == 1 {
      // Last sender gone: wake a parked receiver so it observes disconnect.
      self.chan.wake_receiver();
    }
  }
}

/// The receiving half of an `mpsc` channel. Single-consumer — not `Clone`.
pub struct Receiver<T> {
  chan: Rc<Chan<T>>,
}

impl<T> Receiver<T> {
  #[inline(always)]
  pub(super) fn new(chan: Rc<Chan<T>>) -> Self {
    Self { chan }
  }

  /// Pops an item without waiting. Returns [`TryRecvError::Empty`] when nothing is
  /// queued, or [`TryRecvError::Disconnected`] when the queue is empty and every
  /// sender has dropped.
  #[inline(always)]
  pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
    match self.chan.pop() {
      Some(item) => {
        self.chan.wake_senders();
        Ok(item)
      }
      None if self.chan.senders() == 0 || self.chan.is_closed() => Err(TryRecvError::Disconnected),
      None => Err(TryRecvError::Empty),
    }
  }

  /// Returns a future that resolves to the next item, or `None` once the channel is
  /// empty and every sender has dropped. The future is `Unpin` + `FusedFuture`.
  #[inline(always)]
  pub fn recv(&mut self) -> Recv<'_, T> {
    Recv::new(self)
  }

  /// Returns an iterator over the items currently queued, without waiting: it yields
  /// each ready item and stops at the first empty-or-disconnected poll.
  #[inline]
  pub fn try_iter(&mut self) -> TryIter<'_, T> {
    TryIter(self)
  }

  /// The shared recv state machine, used by the [`Recv`] future and the [`Stream`]
  /// impl. Pops an item (waking parked senders), reports `None` once disconnected, or
  /// registers `cx`'s waker and rechecks — `Recv`'s golden panic-safe path.
  #[inline]
  pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
    let chan = &self.chan;
    if let Some(item) = chan.pop() {
      // A slot freed — wake parked senders (bounded backpressure).
      chan.wake_senders();
      return Poll::Ready(Some(item));
    }
    if chan.senders() == 0 || chan.is_closed() {
      // Empty and disconnected (every sender gone, or the channel closed). Clear any waker
      // a prior spurious poll left registered so a terminal recv/stream does not retain it.
      chan.clear_recv_waker();
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
    if chan.senders() == 0 || chan.is_closed() {
      // Disconnected during/after registration — a re-entrant waker clone may have closed
      // the channel or dropped the last sender. No item is in flight here, so clearing the
      // just-registered waker cannot lose one; leaving it would strand the waker on a
      // terminal recv/stream until the receiver drops (mpmc clears its mirror slot here).
      chan.clear_recv_waker();
      return Poll::Ready(None);
    }
    Poll::Pending
  }

  #[inline(always)]
  pub(super) fn chan(&self) -> &Chan<T> {
    &self.chan
  }

  /// The number of currently-queued items.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.chan.len()
  }

  /// Returns `true` if no items are queued.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.chan.is_empty()
  }

  /// Returns `true` once every sender has been dropped or the channel has been
  /// [`close`](Self::close)d. Items may still be queued for draining.
  #[inline(always)]
  pub fn is_closed(&self) -> bool {
    self.chan.senders() == 0 || self.chan.is_closed()
  }

  /// Closes the channel. Every subsequent send fails closed; this receiver can still
  /// drain what is already queued, after which it observes disconnect. Returns `true` if
  /// this call closed a still-open channel — `false` if it was already closed, whether by
  /// a prior `close` or by every sender dropping.
  #[inline(always)]
  pub fn close(&self) -> bool {
    if self.is_closed() {
      return false;
    }
    self.chan.close();
    true
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
    (self.chan.senders() == 0 || self.chan.is_closed()) && self.chan.is_empty()
  }
}

/// A non-blocking iterator over the currently-available items of a [`Receiver`],
/// returned by [`Receiver::try_iter`]. `next` yields each queued item and stops at the
/// first empty-or-disconnected poll.
pub struct TryIter<'a, T>(&'a mut Receiver<T>);

impl<T> Iterator for TryIter<'_, T> {
  type Item = T;

  #[inline]
  fn next(&mut self) -> Option<T> {
    self.0.try_recv().ok()
  }
}
