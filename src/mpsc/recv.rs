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
  pub(super) fn new(receiver: &'a mut Receiver<T>) -> Self {
    Self {
      receiver,
      done: false,
    }
  }
}

impl<T> Future for Recv<'_, T> {
  type Output = Option<T>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let chan = this.receiver.chan();
    if let Some(item) = chan.pop() {
      // A slot freed — wake parked senders (bounded backpressure).
      chan.wake_senders();
      this.done = true;
      return Poll::Ready(Some(item));
    }
    if chan.senders() == 0 {
      // Empty and every sender is gone — the channel is disconnected.
      this.done = true;
      return Poll::Ready(None);
    }
    chan.register_recv_waker(cx.waker());
    Poll::Pending
  }
}

impl<T> FusedFuture for Recv<'_, T> {
  fn is_terminated(&self) -> bool {
    self.done
  }
}
