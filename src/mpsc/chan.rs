//! The shared, `!Send`, no-atomics channel core.

use core::{cell::Cell, task::Waker};
use std::{rc::Rc, vec::Vec};

use crate::{cell::LocalCell, queue::Flavor};

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
  /// Set by an explicit [`close`](Self::close) from either handle. Independent of the
  /// refcounts: sends then fail closed and the receiver drains then disconnects, even
  /// while both halves are still alive.
  closed: Cell<bool>,
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
    Self::new(Flavor::bounded(cap))
  }

  pub(super) fn unbounded() -> Rc<Self> {
    Self::new(Flavor::unbounded())
  }

  #[inline(always)]
  fn new(flavor: Flavor<T>) -> Rc<Self> {
    Rc::new(Self {
      flavor: LocalCell::new(flavor),
      senders: Cell::new(1),
      receiver_alive: Cell::new(true),
      closed: Cell::new(false),
      recv_waker: LocalCell::new(None),
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
    self.flavor.borrow().len()
  }

  #[inline(always)]
  pub(super) fn is_empty(&self) -> bool {
    self.flavor.borrow().is_empty()
  }

  #[inline(always)]
  pub(super) fn is_full(&self) -> bool {
    self.flavor.borrow().is_full()
  }

  #[inline(always)]
  pub(super) fn receiver_alive(&self) -> bool {
    self.receiver_alive.get()
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
  pub(super) fn clear_receiver(&self) {
    self.receiver_alive.set(false);
  }

  /// Marks the channel explicitly closed, then wakes both halves so a parked recv
  /// observes disconnect and parked sends fail closed. Idempotent: a repeat is a no-op
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
      let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.wake_receiver()));
      let s = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.wake_senders()));
      match (r, s) {
        // Both phases panicked: resume the first and dispose of the second without dropping
        // it directly — a payload whose own `Drop` panics (an adversarial `panic_any` value)
        // would otherwise double-panic into an abort while this call is already unwinding.
        (Err(first), Err(second)) => {
          crate::drop_panic_payload(second);
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
      self.wake_receiver();
      core::mem::forget(guard);
      self.wake_senders();
    }
  }

  /// Whether [`close`](Self::close) has been called.
  #[inline(always)]
  pub(super) fn is_closed(&self) -> bool {
    self.closed.get()
  }

  #[inline(always)]
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

  #[inline(always)]
  pub(super) fn register_recv_waker(&self, waker: &Waker) {
    // Tombstone: ignore registration once the channel is terminal — the receiver is gone,
    // every sender has dropped, or it was closed. The only caller then is a re-entrant
    // waker callback (clone or drop) during disconnect cleanup, which must not repopulate
    // `recv_waker` and have it retained (a terminal `recv`/stream never clears it).
    if !self.receiver_alive.get() || self.senders.get() == 0 || self.closed.get() {
      return;
    }
    // Clone OUTSIDE the borrow, and drop any replaced waker only AFTER it is released:
    // a raw-waker clone/drop callback may re-enter the channel.
    let waker = waker.clone();
    // The clone callback may have just made the channel terminal (pushed the final item and
    // closed, or dropped the last sender). Re-check before storing: poll_recv's post-register
    // recheck can then deliver that item via `Ready(Some)` — which intentionally does not
    // clear the waker — without leaving one stranded on the now-closed, drained channel.
    if !self.receiver_alive.get() || self.senders.get() == 0 || self.closed.get() {
      return;
    }
    // Bind the replaced waker so it drops at function exit, AFTER the borrow_mut temporary
    // is released. A bare `let _ = ...replace(...)` drops it while the borrow is still held
    // (reverse drop order), and a re-entrant waker drop callback then double-borrows.
    let replaced = self.recv_waker.borrow_mut().replace(waker);
    drop(replaced);
  }

  /// Clears the receiver's registered waker — used when a pending `recv` is canceled or
  /// completes disconnected, so its waker is not retained until a later send or channel
  /// drop.
  #[inline(always)]
  pub(super) fn clear_recv_waker(&self) {
    // Move the waker out, releasing the borrow, then drop it: a waker drop callback may
    // re-enter and re-borrow `recv_waker`.
    let waker = self.recv_waker.borrow_mut().take();
    drop(waker);
  }

  /// Whether a receiver waker is currently registered — used by tests to assert a
  /// disconnected recv does not leave its waker behind.
  #[cfg(all(test, feature = "std"))]
  pub(super) fn recv_waker_registered(&self) -> bool {
    self.recv_waker.borrow().is_some()
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

  #[inline(always)]
  pub(super) fn wake_senders(&self) {
    // Fast path: no parked sender to wake — always the case for an unbounded channel,
    // and for a bounded one that is not full. Skips the take + guard below, which are
    // pure overhead on the hot recv path.
    if self.send_wakers.borrow().is_empty() {
      return;
    }
    // Move the buffer out — dropping the borrow BEFORE waking, since a waker may re-enter
    // and re-borrow `send_wakers` — then wake every parked sender even if one waker panics:
    // a panicking wake must not strand the others (their futures would stay Pending
    // forever). Under `std` each wake is caught and the first panic is resumed only after
    // the whole buffer is drained, so two panicking wakers cannot double-panic into an
    // abort. Under `no_std` (no `catch_unwind`) a guard wakes the rest on unwind: one
    // panicking waker is recovered, two abort — the documented `no_std`-with-unwind limit.
    let wakers = core::mem::take(&mut *self.send_wakers.borrow_mut());
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
            // Only the first panic is resumed; dispose of the rest. A `panic_any` payload
            // whose `Drop` panics would otherwise abort while unwinding.
            crate::drop_panic_payload(panic);
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

  /// Pushes if there is room; returns `Err(item)` when a bounded channel is full.
  #[inline(always)]
  pub(super) fn try_push(&self, item: T) -> Result<(), T> {
    self.flavor.borrow_mut().try_push(item)
  }

  #[inline(always)]
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
