//! Bounded `mpsc`: [`Sender`] and [`Receiver`].

use alloc::rc::Rc;

use super::{
  chan::Chan,
  error::{TryRecvError, TrySendError},
};

/// The sending half of a bounded channel. Cloneable — every clone is another
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

/// The receiving half of a bounded channel. Single-consumer — not `Clone`.
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
  pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
    match self.chan.pop() {
      Some(item) => {
        self.chan.wake_senders();
        Ok(item)
      }
      None if self.chan.senders() == 0 => Err(TryRecvError::Disconnected),
      None => Err(TryRecvError::Empty),
    }
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
    // Free queued-but-unreceived payloads now; a live sender keeps `Chan` alive.
    self.chan.drain();
    // Wake parked senders so their `send` observes the close.
    self.chan.wake_senders();
  }
}
