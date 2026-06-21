//! The shared, `!Send`, no-atomics channel core.

use alloc::{rc::Rc, vec::Vec};
use core::{
  cell::{Cell, RefCell},
  task::Waker,
};

use super::ring::Ring;

/// Shared state behind every `mpsc` handle. Holds `Rc`/`Cell`/`RefCell` — never
/// atomics — so it is `!Send`: a single thread owns both ends and cannot race
/// itself, which is what lets `poll` register-then-return without a recheck.
pub(super) struct Chan<T> {
  ring: RefCell<Ring<T>>,
  /// Live sender count. Once it hits zero the receiver drains what is queued and
  /// then reports disconnect.
  senders: Cell<usize>,
  /// Cleared when the single receiver drops; sends then fail closed.
  receiver_alive: Cell<bool>,
  /// The receiver's waker, woken when an item is pushed or the last sender drops.
  recv_waker: RefCell<Option<Waker>>,
  /// Wakers of senders parked on a full bounded channel, woken (all) when a slot
  /// frees or the receiver drops.
  send_wakers: RefCell<Vec<Waker>>,
}

impl<T> Chan<T> {
  pub(super) fn bounded(cap: usize) -> Rc<Self> {
    Rc::new(Self {
      ring: RefCell::new(Ring::with_capacity(cap)),
      senders: Cell::new(1),
      receiver_alive: Cell::new(true),
      recv_waker: RefCell::new(None),
      send_wakers: RefCell::new(Vec::new()),
    })
  }

  pub(super) fn cap(&self) -> usize {
    self.ring.borrow().cap()
  }

  pub(super) fn len(&self) -> usize {
    self.ring.borrow().len()
  }

  pub(super) fn is_empty(&self) -> bool {
    self.ring.borrow().is_empty()
  }

  pub(super) fn is_full(&self) -> bool {
    self.ring.borrow().is_full()
  }

  pub(super) fn receiver_alive(&self) -> bool {
    self.receiver_alive.get()
  }

  pub(super) fn senders(&self) -> usize {
    self.senders.get()
  }

  pub(super) fn incr_senders(&self) {
    self.senders.set(self.senders.get() + 1);
  }

  /// Decrements the sender count, returning the value *before* the decrement so the
  /// caller can detect the last sender leaving.
  pub(super) fn decr_senders(&self) -> usize {
    let n = self.senders.get();
    self.senders.set(n - 1);
    n
  }

  pub(super) fn clear_receiver(&self) {
    self.receiver_alive.set(false);
  }

  pub(super) fn wake_receiver(&self) {
    if let Some(waker) = self.recv_waker.borrow_mut().take() {
      waker.wake();
    }
  }

  pub(super) fn wake_senders(&self) {
    let wakers: Vec<Waker> = self.send_wakers.borrow_mut().drain(..).collect();
    for waker in wakers {
      waker.wake();
    }
  }

  /// Pushes if there is room; returns `Err(item)` when the channel is full.
  pub(super) fn try_push(&self, item: T) -> Result<(), T> {
    self.ring.borrow_mut().push(item)
  }

  pub(super) fn pop(&self) -> Option<T> {
    self.ring.borrow_mut().pop()
  }

  /// Drops every queued item, releasing their memory at once. Used on receiver drop:
  /// the items are unreachable, but a live sender keeps `Chan` alive.
  pub(super) fn drain(&self) {
    self.ring.borrow_mut().clear();
  }
}
