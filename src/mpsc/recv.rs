//! The `recv` future.

use core::{
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
  receiver: &'a mut Receiver<T>,
  done: bool,
}

impl<'a, T> Recv<'a, T> {
  #[inline(always)]
  pub(super) fn new(receiver: &'a mut Receiver<T>) -> Self {
    Self {
      receiver,
      done: false,
    }
  }
}

impl<T> Future for Recv<'_, T> {
  type Output = Option<T>;

  #[inline]
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    // Delegate to the shared, golden panic-safe recv path; record completion so the
    // `FusedFuture`/`Drop` (cancellation) logic below can tell a finished recv from a
    // parked one.
    let polled = this.receiver.poll_recv(cx);
    if polled.is_ready() {
      this.done = true;
    }
    polled
  }
}

impl<T> FusedFuture for Recv<'_, T> {
  #[inline(always)]
  fn is_terminated(&self) -> bool {
    self.done
  }
}

impl<T> Drop for Recv<'_, T> {
  #[inline]
  fn drop(&mut self) {
    // A canceled (dropped-while-pending) recv clears its registered waker so it is
    // not retained until a later send or channel drop.
    if !self.done {
      self.receiver.chan().clear_recv_waker();
    }
  }
}
