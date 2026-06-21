//! The `recv` future.

use core::{
  cell::Cell,
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::future::FusedFuture;

use super::channel::Receiver;

/// The future returned by [`Receiver::recv`](super::Receiver::recv). It holds no
/// `T`, so it is `Unpin` regardless of `T`, and implements [`FusedFuture`] so it can
/// be polled in `select_biased!` without `.fuse()`.
pub struct Recv<'a, T> {
  receiver: &'a Receiver<T>,
  done: bool,
  /// This future's recv-waker registration id, if parked. A `Cell` so `poll_recv` can
  /// record it through a shared reference, durably, before any later step can unwind.
  waker_id: Cell<Option<u64>>,
}

impl<'a, T> Recv<'a, T> {
  #[inline(always)]
  pub(super) fn new(receiver: &'a Receiver<T>) -> Self {
    Self {
      receiver,
      done: false,
      waker_id: Cell::new(None),
    }
  }
}

impl<T> Future for Recv<'_, T> {
  type Output = Option<T>;

  #[inline]
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // `Recv` holds no `T` and no self-referential state, so projecting `&mut Self`
    // out of the pinned reference never moves pinned data.
    let this = self.get_mut();
    if this.done {
      // Completed, but a recheck-Ready delivery may have left this future's waker
      // registered (it cannot be removed at delivery without risking the popped item), so
      // `is_terminated` stayed false and a fused consumer re-polls once. Clear it here
      // WITHOUT consuming an item, and wake so the consumer re-evaluates rather than
      // parking with no wake source. Fires at most once — the next poll finds nothing.
      if let Some(id) = this.waker_id.take() {
        this.receiver.chan().remove_recv_waker(id);
        cx.waker().wake_by_ref();
      }
      return Poll::Pending;
    }
    // Delegate to the shared, golden panic-safe recv path; it threads this future's
    // waker-id slot so a re-poll replaces its registration rather than accumulating a
    // second entry. Record completion so the `FusedFuture`/`Drop` (cancellation) logic
    // below can tell a finished recv from a parked one.
    let polled = this.receiver.poll_recv(cx, &this.waker_id);
    if polled.is_ready() {
      this.done = true;
    }
    polled
  }
}

impl<T> FusedFuture for Recv<'_, T> {
  #[inline(always)]
  fn is_terminated(&self) -> bool {
    // Stay non-terminal while a recheck-Ready completion left this future's waker
    // registered — a fused consumer must poll once more to clear it (see `poll`), or it
    // would be stranded until `Drop`. Once cleared, the completed recv is terminal.
    self.done && self.waker_id.get().is_none()
  }
}

impl<T> Drop for Recv<'_, T> {
  #[inline]
  fn drop(&mut self) {
    // Remove a still-registered waker (a future dropped while parked, or one left
    // registered by a recheck-Ready completion) so it is not retained until a later
    // send or channel drop.
    if let Some(id) = self.waker_id.take() {
      self.receiver.chan().remove_recv_waker(id);
    }
  }
}
