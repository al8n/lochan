//! The shared, `!Send`, no-atomics channel core.

use core::{cell::Cell, task::Waker};
use std::{rc::Rc, vec::Vec};

use crate::cell::LocalCell;

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
}

/// Shared state behind every `mpsc` handle. Holds `Rc`/`Cell`/`LocalCell` — never
/// atomics — so it is `!Send`: a single thread owns both ends and cannot race
/// itself, which is what lets `poll` register-then-recheck without a lock.
pub(super) struct Chan<T> {
  flavor: LocalCell<Flavor<T>>,
  /// Live sender count. Once it hits zero the receiver drains what is queued and
  /// then reports disconnect.
  senders: Cell<usize>,
  /// Cleared when the single receiver drops; sends then fail closed.
  receiver_alive: Cell<bool>,
  /// The receiver's waker, woken when an item is pushed or the last sender drops.
  recv_waker: LocalCell<Option<Waker>>,
  /// Wakers of senders parked on a full bounded channel, each tagged with a stable
  /// registration id so a future can remove exactly its own entry (rather than rely
  /// on the best-effort `Waker::will_wake`).
  send_wakers: LocalCell<Vec<(u64, Waker)>>,
  /// Source of stable send-waker registration ids.
  next_send_id: Cell<u64>,
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
      flavor: LocalCell::new(flavor),
      senders: Cell::new(1),
      receiver_alive: Cell::new(true),
      recv_waker: LocalCell::new(None),
      send_wakers: LocalCell::new(Vec::new()),
      next_send_id: Cell::new(0),
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
    // Fast path: the receiver is not parked (it is actively draining, or this is a
    // producer still buffering) — nothing to wake. Skips the take + write-back.
    if self.recv_waker.borrow().is_none() {
      return;
    }
    // Take the waker out and drop the borrow BEFORE waking: a synchronous waker may
    // re-enter and register again, which would double-borrow `recv_waker`.
    let waker = self.recv_waker.borrow_mut().take();
    if let Some(waker) = waker {
      waker.wake();
    }
  }

  pub(super) fn register_recv_waker(&self, waker: &Waker) {
    // Tombstone: once the receiver is gone (or mid-drop), ignore registration — the
    // only caller then is a re-entrant waker drop, which must not repopulate
    // `recv_waker` and have it retained while a `Sender` keeps `Chan` alive.
    if !self.receiver_alive.get() {
      return;
    }
    // Clone OUTSIDE the borrow, and drop any replaced waker only AFTER it is released:
    // a raw-waker clone/drop callback may re-enter the channel.
    let waker = waker.clone();
    let old = self.recv_waker.borrow_mut().replace(waker);
    drop(old);
  }

  /// Clears the receiver's registered waker — used when a pending `recv` is canceled,
  /// so its waker is not retained until a later send or channel drop. Drops the old
  /// waker after releasing the borrow.
  pub(super) fn clear_recv_waker(&self) {
    let old = self.recv_waker.borrow_mut().take();
    drop(old);
  }

  /// Registers a parked sender's waker, returning a stable id for later removal.
  /// Clones outside the borrow.
  pub(super) fn add_send_waker(&self, waker: &Waker) -> u64 {
    let waker = waker.clone();
    let id = self.next_send_id.get();
    self.next_send_id.set(id.wrapping_add(1));
    self.send_wakers.borrow_mut().push((id, waker));
    id
  }

  /// Removes the send-waker registered under `id`, if still present, dropping it only
  /// AFTER the borrow is released (its drop callback may re-enter the channel).
  pub(super) fn remove_send_waker(&self, id: u64) {
    let removed = {
      let mut wakers = self.send_wakers.borrow_mut();
      wakers
        .iter()
        .position(|(wid, _)| *wid == id)
        .map(|pos| wakers.swap_remove(pos))
    };
    drop(removed);
  }

  pub(super) fn wake_senders(&self) {
    // Fast path: no parked sender to wake — always the case for an unbounded channel,
    // and for a bounded one that is not full. Skips the take + guard below, which are
    // pure overhead on the hot recv path.
    if self.send_wakers.borrow().is_empty() {
      return;
    }
    // Move the buffer out — dropping the borrow BEFORE waking, since a waker may
    // re-enter and re-borrow `send_wakers` — then wake every parked sender even if one
    // waker panics: a panicking wake must not strand the others (their futures would
    // stay Pending forever). A guard wakes whatever is left on unwind.
    let wakers = core::mem::take(&mut *self.send_wakers.borrow_mut());
    struct WakeRest(Vec<(u64, Waker)>);
    impl Drop for WakeRest {
      fn drop(&mut self) {
        for (_id, waker) in core::mem::take(&mut self.0) {
          waker.wake();
        }
      }
    }
    let mut rest = WakeRest(wakers);
    while let Some((_id, waker)) = rest.0.pop() {
      waker.wake();
    }
    // `rest` drops here (or on unwind): its `Drop` wakes whatever remains — nothing on
    // the normal path — and frees the buffer. (No `forget`: that would leak the `Vec`.)
  }

  /// Pushes if there is room; returns `Err(item)` when a bounded channel is full.
  pub(super) fn try_push(&self, item: T) -> Result<(), T> {
    self.flavor.borrow_mut().try_push(item)
  }

  pub(super) fn pop(&self) -> Option<T> {
    self.flavor.borrow_mut().pop()
  }

  /// Drops every queued item. Used on receiver drop: the items are unreachable, but a
  /// live sender keeps `Chan` alive. Each payload's `Drop` runs OUTSIDE the flavor
  /// borrow (a re-entrant payload destructor may re-borrow the channel). A guard
  /// re-enters on unwind, so a single panicking payload `Drop` cannot strand the rest
  /// of the queue — remaining items may own `Sender`s (an `Rc` cycle).
  pub(super) fn drain(&self) {
    struct ContinueOnUnwind<'a, T>(&'a Chan<T>);
    impl<T> Drop for ContinueOnUnwind<'_, T> {
      fn drop(&mut self) {
        self.0.drain();
      }
    }
    let guard = ContinueOnUnwind(self);
    while let Some(item) = self.pop() {
      drop(item);
    }
    core::mem::forget(guard);
  }
}
