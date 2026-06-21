//! The shared, `!Send`, no-atomics channel core.

use alloc::{rc::Rc, vec::Vec};
use core::{
  cell::{Cell, RefCell},
  task::Waker,
};

use super::{list::BlockList, ring::Ring};

/// The storage backing a channel: a fixed ring (bounded) or a segmented block-list
/// (unbounded). Dispatch is a single match per operation.
enum Flavor<T> {
  Bounded(Ring<T>),
  Unbounded(BlockList<T>),
}

impl<T> Flavor<T> {
  fn len(&self) -> usize {
    match self {
      Self::Bounded(r) => r.len(),
      Self::Unbounded(l) => l.len(),
    }
  }

  fn is_empty(&self) -> bool {
    match self {
      Self::Bounded(r) => r.is_empty(),
      Self::Unbounded(l) => l.is_empty(),
    }
  }

  /// The capacity, or `None` when unbounded.
  fn cap(&self) -> Option<usize> {
    match self {
      Self::Bounded(r) => Some(r.cap()),
      Self::Unbounded(_) => None,
    }
  }

  fn is_full(&self) -> bool {
    match self {
      Self::Bounded(r) => r.is_full(),
      Self::Unbounded(_) => false,
    }
  }

  /// Pushes, or hands the item back via `Err` when a bounded channel is full.
  /// Unbounded never fails.
  fn try_push(&mut self, item: T) -> Result<(), T> {
    match self {
      Self::Bounded(r) => r.push(item),
      Self::Unbounded(l) => {
        l.push(item);
        Ok(())
      }
    }
  }

  fn pop(&mut self) -> Option<T> {
    match self {
      Self::Bounded(r) => r.pop(),
      Self::Unbounded(l) => l.pop(),
    }
  }

  fn clear(&mut self) {
    match self {
      Self::Bounded(r) => r.clear(),
      Self::Unbounded(l) => l.clear(),
    }
  }
}

/// Shared state behind every `mpsc` handle. Holds `Rc`/`Cell`/`RefCell` — never
/// atomics — so it is `!Send`: a single thread owns both ends and cannot race
/// itself, which is what lets `poll` register-then-return without a recheck.
pub(super) struct Chan<T> {
  flavor: RefCell<Flavor<T>>,
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
    Self::new(Flavor::Bounded(Ring::with_capacity(cap)))
  }

  pub(super) fn unbounded() -> Rc<Self> {
    Self::new(Flavor::Unbounded(BlockList::new()))
  }

  fn new(flavor: Flavor<T>) -> Rc<Self> {
    Rc::new(Self {
      flavor: RefCell::new(flavor),
      senders: Cell::new(1),
      receiver_alive: Cell::new(true),
      recv_waker: RefCell::new(None),
      send_wakers: RefCell::new(Vec::new()),
    })
  }

  pub(super) fn cap(&self) -> Option<usize> {
    self.flavor.borrow().cap()
  }

  pub(super) fn len(&self) -> usize {
    self.flavor.borrow().len()
  }

  pub(super) fn is_empty(&self) -> bool {
    self.flavor.borrow().is_empty()
  }

  pub(super) fn is_full(&self) -> bool {
    self.flavor.borrow().is_full()
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
    for waker in self.send_wakers.borrow_mut().drain(..) {
      waker.wake();
    }
  }

  pub(super) fn register_recv_waker(&self, waker: &Waker) {
    *self.recv_waker.borrow_mut() = Some(waker.clone());
  }

  pub(super) fn register_send_waker(&self, waker: &Waker) {
    let mut wakers = self.send_wakers.borrow_mut();
    if !wakers.iter().any(|w| w.will_wake(waker)) {
      wakers.push(waker.clone());
    }
  }

  /// Pushes if there is room; returns `Err(item)` when a bounded channel is full.
  pub(super) fn try_push(&self, item: T) -> Result<(), T> {
    self.flavor.borrow_mut().try_push(item)
  }

  pub(super) fn pop(&self) -> Option<T> {
    self.flavor.borrow_mut().pop()
  }

  /// Drops every queued item, releasing their memory at once. Used on receiver drop:
  /// the items are unreachable, but a live sender keeps `Chan` alive.
  pub(super) fn drain(&self) {
    self.flavor.borrow_mut().clear();
  }
}
