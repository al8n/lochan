//! The shared, `!Send`, no-atomics channel core.

use core::{cell::Cell, task::Waker};
use std::{rc::Rc, vec::Vec};

use crate::{cell::LocalCell, queue::Flavor};

/// Shared state behind every `mpmc` handle. Holds `Rc`/`Cell`/`LocalCell` — never
/// atomics — so it is `!Send`: a single thread owns every end and cannot race
/// itself, which is what lets `poll` register-then-recheck without a lock.
pub(super) struct Chan<T> {
  flavor: LocalCell<Flavor<T>>,
  /// An item popped from `flavor` but not yet handed to the receiver: it is parked here
  /// across `wake_senders` so a panicking sender waker re-queues it (the next recv drains
  /// it as the FIFO head) instead of dropping a stack local. Empty except on that rare
  /// unwound-wake path — and only ever touched when a sender is actually parked.
  redelivery: LocalCell<Option<T>>,
  /// Live sender count. Once it hits zero the receivers drain what is queued and
  /// then report disconnect.
  senders: Cell<usize>,
  /// Live receiver count. Once it hits zero sends fail closed and the last receiver
  /// drains the queue.
  receivers: Cell<usize>,
  /// Set by an explicit [`close`](Self::close) from either handle. Independent of the
  /// refcounts: sends then fail closed and receivers drain then disconnect, even while
  /// both halves are still alive.
  closed: Cell<bool>,
  /// Wakers of receivers parked on an empty channel, each tagged with a stable
  /// registration id so a future can remove exactly its own entry. A push wakes every
  /// parked receiver; whichever polls first pops the item, the rest re-park.
  recv_wakers: LocalCell<Vec<(u64, Waker)>>,
  /// Source of stable recv-waker registration ids.
  next_recv_id: Cell<u64>,
  /// Wakers of senders parked on a full bounded channel, each tagged with a stable
  /// registration id so a future can remove exactly its own entry (rather than rely
  /// on the best-effort `Waker::will_wake`).
  send_wakers: LocalCell<Vec<(u64, Waker)>>,
  /// Source of stable send-waker registration ids.
  next_send_id: Cell<u64>,
}

impl<T> Chan<T> {
  #[inline(always)]
  pub(super) fn bounded(cap: usize) -> Rc<Self> {
    Self::new(Flavor::bounded(cap))
  }

  #[inline(always)]
  pub(super) fn unbounded() -> Rc<Self> {
    Self::new(Flavor::unbounded())
  }

  #[inline(always)]
  fn new(flavor: Flavor<T>) -> Rc<Self> {
    Rc::new(Self {
      flavor: LocalCell::new(flavor),
      redelivery: LocalCell::new(None),
      senders: Cell::new(1),
      receivers: Cell::new(1),
      closed: Cell::new(false),
      recv_wakers: LocalCell::new(Vec::new()),
      next_recv_id: Cell::new(0),
      send_wakers: LocalCell::new(Vec::new()),
      next_send_id: Cell::new(0),
    })
  }

  #[inline(always)]
  pub(super) fn cap(&self) -> Option<usize> {
    self.flavor.borrow().cap()
  }

  #[inline(always)]
  pub(super) fn len(&self) -> usize {
    // Count the in-transit redelivery item too: it is logically queued (delivered next),
    // just parked outside `flavor` across a sender wake.
    self.flavor.borrow().len() + usize::from(self.redelivery.borrow().is_some())
  }

  #[inline(always)]
  pub(super) fn is_empty(&self) -> bool {
    // A parked redelivery item is still pending delivery, so the channel is NOT empty.
    // Public `is_empty` and `FusedStream::is_terminated` both route through here, so this
    // keeps stream termination from declaring done while a recovered item is in flight.
    self.redelivery.borrow().is_none() && self.flavor.borrow().is_empty()
  }

  #[inline(always)]
  pub(super) fn is_full(&self) -> bool {
    // The redelivery item was popped out of `flavor`, so it does not occupy queue
    // capacity; a sender's push view is the backing queue alone.
    self.flavor.borrow().is_full()
  }

  /// Returns `true` while at least one receiver is alive.
  #[inline(always)]
  pub(super) fn receiver_alive(&self) -> bool {
    self.receivers.get() > 0
  }

  #[inline(always)]
  pub(super) fn senders(&self) -> usize {
    self.senders.get()
  }

  #[inline(always)]
  pub(super) fn incr_senders(&self) {
    self.senders.set(self.senders.get() + 1);
  }

  /// Decrements the sender count, returning the value *before* the decrement so the
  /// caller can detect the last sender leaving.
  #[inline(always)]
  pub(super) fn decr_senders(&self) -> usize {
    let n = self.senders.get();
    self.senders.set(n - 1);
    n
  }

  #[inline(always)]
  pub(super) fn incr_receivers(&self) {
    self.receivers.set(self.receivers.get() + 1);
  }

  /// Decrements the receiver count, returning the value *before* the decrement so the
  /// caller can detect the last receiver leaving.
  #[inline(always)]
  pub(super) fn decr_receivers(&self) -> usize {
    let n = self.receivers.get();
    self.receivers.set(n - 1);
    n
  }

  /// Marks the channel explicitly closed, then wakes both halves so parked receivers
  /// observe disconnect and parked sends fail closed. Idempotent: a repeat is a no-op
  /// that skips the redundant wakes.
  #[inline(always)]
  pub(super) fn close(&self) {
    if self.closed.replace(true) {
      return;
    }
    // Wake BOTH halves, isolating wake panics. Once `closed` is set a retry skips the
    // wakes, so a panicking receiver waker must neither leave parked senders unwoken nor
    // turn a second panicking sender waker into a double-panic abort. Under `std`, catch
    // each phase, wake both, then resume only the first panic. Under `no_std` (no
    // `catch_unwind`) a guard wakes the senders on an unwind from the receiver wake: a
    // single panicking waker is recovered; two abort — the limit `wake_all` documents.
    #[cfg(feature = "std")]
    {
      let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.wake_receivers()));
      let s = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.wake_senders()));
      match (r, s) {
        // Both phases panicked: resume the first and FORGET the second. Dropping a panic
        // payload whose own `Drop` panics (an adversarial `panic_any` value) would
        // double-panic into an abort while this call is already unwinding.
        (Err(first), Err(second)) => {
          core::mem::forget(second);
          std::panic::resume_unwind(first);
        }
        (Err(panic), Ok(())) | (Ok(()), Err(panic)) => std::panic::resume_unwind(panic),
        (Ok(()), Ok(())) => {}
      }
    }
    #[cfg(not(feature = "std"))]
    {
      struct WakeSendersOnUnwind<'a, T>(&'a Chan<T>);
      impl<T> Drop for WakeSendersOnUnwind<'_, T> {
        fn drop(&mut self) {
          self.0.wake_senders();
        }
      }
      let guard = WakeSendersOnUnwind(self);
      self.wake_receivers();
      core::mem::forget(guard);
      self.wake_senders();
    }
  }

  /// Whether [`close`](Self::close) has been called.
  #[inline(always)]
  pub(super) fn is_closed(&self) -> bool {
    self.closed.get()
  }

  /// Registers a parked receiver's waker, returning a stable id for later removal.
  /// Clones outside the borrow.
  #[inline]
  pub(super) fn add_recv_waker(&self, waker: &Waker) -> u64 {
    let id = self.next_recv_id.get();
    self.next_recv_id.set(id.wrapping_add(1));
    // Skip storing once the channel is terminal — every sender gone, the channel closed,
    // or every receiver gone. The only caller then is a re-entrant waker callback during
    // disconnect cleanup; storing would orphan the waker (a terminal recv/stream never
    // removes it). The id is still consumed, so a caller's later remove is a clean no-op.
    if self.senders.get() == 0 || self.closed.get() || self.receivers.get() == 0 {
      return id;
    }
    let waker = waker.clone();
    // The clone callback may have just made the channel terminal (pushed the final item and
    // closed, or dropped the last sender). Re-check before storing so poll_recv's recheck can
    // deliver that item via `Ready(Some)` without leaving an orphaned waker behind.
    if self.senders.get() == 0 || self.closed.get() || self.receivers.get() == 0 {
      return id;
    }
    self.recv_wakers.borrow_mut().push((id, waker));
    id
  }

  /// Removes the recv-waker registered under `id`, if still present, dropping it only
  /// AFTER the borrow is released (its drop callback may re-enter the channel).
  #[inline]
  pub(super) fn remove_recv_waker(&self, id: u64) {
    let _ = {
      let mut wakers = self.recv_wakers.borrow_mut();
      wakers
        .iter()
        .position(|(wid, _)| *wid == id)
        .map(|pos| wakers.swap_remove(pos))
    };
  }

  /// Wakes every parked receiver. Called when an item is pushed or the last sender
  /// drops: each woken receiver re-polls, and the first to run pops the item (or
  /// observes disconnect) while the rest re-park.
  #[inline(always)]
  pub(super) fn wake_receivers(&self) {
    // Fast path: no parked receiver to wake. Skips the take + wake below, which are pure
    // overhead on the hot send path.
    if self.recv_wakers.borrow().is_empty() {
      return;
    }
    // Take the buffer out in its OWN scope so the `LocalCell` borrow is released before
    // any waker runs. Inlining the `borrow_mut()` into the `wake_all(...)` call would keep
    // that temporary borrow alive across the wakes (it drops at the end of the statement),
    // and a re-entrant waker would then double-borrow `recv_wakers`.
    let wakers = {
      let mut list = self.recv_wakers.borrow_mut();
      core::mem::take(&mut *list)
    };
    wake_all(wakers);
  }

  /// Registers a parked sender's waker, returning a stable id for later removal.
  /// Clones outside the borrow.
  #[inline]
  pub(super) fn add_send_waker(&self, waker: &Waker) -> u64 {
    let waker = waker.clone();
    let id = self.next_send_id.get();
    self.next_send_id.set(id.wrapping_add(1));
    self.send_wakers.borrow_mut().push((id, waker));
    id
  }

  /// Removes the send-waker registered under `id`, if still present, dropping it only
  /// AFTER the borrow is released (its drop callback may re-enter the channel).
  #[inline]
  pub(super) fn remove_send_waker(&self, id: u64) {
    let _ = {
      let mut wakers = self.send_wakers.borrow_mut();
      wakers
        .iter()
        .position(|(wid, _)| *wid == id)
        .map(|pos| wakers.swap_remove(pos))
    };
  }

  /// The number of registered send-wakers — used by tests to assert a completed send did
  /// not leave its registration behind.
  #[cfg(all(test, feature = "std"))]
  pub(super) fn send_wakers_len(&self) -> usize {
    self.send_wakers.borrow().len()
  }

  /// The number of registered recv-wakers — used by tests to assert a registration is not
  /// left behind.
  #[cfg(all(test, feature = "std"))]
  pub(super) fn recv_wakers_len(&self) -> usize {
    self.recv_wakers.borrow().len()
  }

  #[inline(always)]
  pub(super) fn wake_senders(&self) {
    // Fast path: no parked sender to wake — always the case for an unbounded channel, and
    // for a bounded one that is not full. Skips the take + wake below, which are pure
    // overhead on the hot recv path.
    if self.send_wakers.borrow().is_empty() {
      return;
    }
    // Take the buffer out in its OWN scope so the `LocalCell` borrow is released before
    // any waker runs. Inlining the `borrow_mut()` into the `wake_all(...)` call would keep
    // that temporary borrow alive across the wakes (it drops at the end of the statement),
    // and a re-entrant waker would then double-borrow `send_wakers`.
    let wakers = {
      let mut list = self.send_wakers.borrow_mut();
      core::mem::take(&mut *list)
    };
    wake_all(wakers);
  }

  /// Pushes if there is room; returns `Err(item)` when a bounded channel is full.
  #[inline(always)]
  pub(super) fn try_push(&self, item: T) -> Result<(), T> {
    self.flavor.borrow_mut().try_push(item)
  }

  #[inline(always)]
  pub(super) fn pop(&self) -> Option<T> {
    self.flavor.borrow_mut().pop()
  }

  /// Returns `true` while at least one sender is parked on a full bounded channel. A
  /// cheap peek used by [`try_take`](Self::try_take) to skip the redelivery dance when
  /// `wake_senders` would be a no-op (no panic risk, nothing to protect).
  #[inline(always)]
  pub(super) fn has_parked_senders(&self) -> bool {
    !self.send_wakers.borrow().is_empty()
  }

  /// Pops the next item, parking it across `wake_senders` so a panicking sender waker
  /// cannot drop it. Returns `None` when the queue is empty — or when a re-entrant
  /// consumer drained the parked item mid-wake, in which case it was delivered to them.
  #[inline]
  pub(super) fn try_take(&self) -> Option<T> {
    loop {
      // A prior wake may have unwound with the item still parked here; drain it first — it
      // was the FIFO head when popped, so it precedes everything still in the queue.
      if let Some(item) = self.redelivery.borrow_mut().take() {
        return Some(item);
      }
      let item = self.flavor.borrow_mut().pop()?;
      if !self.has_parked_senders() {
        // No parked sender, so `wake_senders` is a no-op and nothing can strand the item —
        // skip the slot entirely (the hot path).
        return Some(item);
      }
      // Park the item, free the slot for a woken sender, then reclaim it. Setting the slot
      // and taking it back run no user code; only `wake_senders` between them does, and an
      // unwind there leaves the item parked for the next recv.
      *self.redelivery.borrow_mut() = Some(item);
      self.wake_senders();
      if let Some(item) = self.redelivery.borrow_mut().take() {
        return Some(item);
      }
      // A re-entrant consumer (a side-effecting sender waker acting as a receiver) drained
      // the parked item during the wake. Tail items may still be queued — and the woken
      // sender may have queued more — so loop to re-check rather than falsely report empty.
    }
  }

  /// Drops every queued item. Used on the last receiver's drop: the items are
  /// unreachable, but a live sender keeps `Chan` alive. Each payload's `Drop` runs
  /// OUTSIDE the flavor borrow (a re-entrant payload destructor may re-borrow the
  /// channel). A guard re-enters on unwind, so a single panicking payload `Drop`
  /// cannot strand the rest of the queue — remaining items may own `Sender`s (an `Rc`
  /// cycle).
  pub(super) fn drain(&self) {
    struct ContinueOnUnwind<'a, T>(&'a Chan<T>);
    impl<T> Drop for ContinueOnUnwind<'_, T> {
      fn drop(&mut self) {
        self.0.drain();
      }
    }
    let guard = ContinueOnUnwind(self);
    // Free any in-transit redelivery item too — it may own a `Sender`, the same `Rc`
    // cycle the queue drain breaks. Taken OUT before its `Drop` runs, so a re-entrant
    // destructor cannot re-borrow the slot; the guard re-enters if that `Drop` panics.
    let parked = self.redelivery.borrow_mut().take();
    drop(parked);
    while let Some(item) = self.pop() {
      drop(item);
    }
    core::mem::forget(guard);
  }
}

/// Wakes every waker in `wakers`, isolating panics so a misbehaving waker cannot strand
/// the rest.
///
/// Under `std` each `wake()` runs inside `catch_unwind` and the first panic is resumed
/// only after every waker has been woken, so a second panicking waker can never
/// double-panic into a process abort. Under `no_std` (no `catch_unwind`) a `Drop` guard
/// wakes the rest on unwind: that recovers a single panicking waker, but two panicking
/// wakers double-panic and abort — a documented `no_std`-with-unwind limitation (an
/// embedded `panic = "abort"` build aborts on the first panic regardless, so this path is
/// moot there).
///
/// The wakers are already moved out of their `LocalCell` by the caller, so a re-entrant
/// waker is free to re-borrow it.
fn wake_all(wakers: Vec<(u64, Waker)>) {
  #[cfg(feature = "std")]
  {
    let mut first_panic = None;
    for (_id, waker) in wakers {
      if let Err(panic) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || waker.wake()))
      {
        if first_panic.is_none() {
          first_panic = Some(panic);
        } else {
          // Only the first panic is resumed; forget the rest. Dropping a `panic_any`
          // payload whose `Drop` panics would abort while unwinding.
          core::mem::forget(panic);
        }
      }
    }
    if let Some(panic) = first_panic {
      std::panic::resume_unwind(panic);
    }
  }
  #[cfg(not(feature = "std"))]
  {
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
  }
}
