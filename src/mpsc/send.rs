//! The `send` future.

use core::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::future::FusedFuture;

use super::{bounded::Sender, error::SendError};

/// The future returned by [`Sender::send`](super::Sender::send). It holds the pending
/// item, so it is `Unpin` when `T: Unpin`, and implements [`FusedFuture`] so it can
/// be polled in `select_biased!` without `.fuse()`.
pub struct Send<'a, T> {
  sender: &'a Sender<T>,
  item: Option<T>,
  done: bool,
}

impl<'a, T> Send<'a, T> {
  pub(super) fn new(sender: &'a Sender<T>, item: T) -> Self {
    Self {
      sender,
      item: Some(item),
      done: false,
    }
  }
}

impl<T> Future for Send<'_, T> {
  type Output = Result<(), SendError<T>>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // SAFETY: `Send` holds no self-referential state — `sender` is a reference
    // and `item` is moved out by value — so projecting `&mut Self` out of the pinned
    // reference never moves pinned data. (Generic over `T`, so it cannot rely on
    // `Self: Unpin`.)
    let this = unsafe { self.get_unchecked_mut() };
    let chan = this.sender.chan();
    let item = this
      .item
      .take()
      .expect("send future polled after completion");
    if !chan.receiver_alive() {
      this.done = true;
      return Poll::Ready(Err(SendError::new(item)));
    }
    match chan.try_push(item) {
      Ok(()) => {
        chan.wake_receiver();
        this.done = true;
        Poll::Ready(Ok(()))
      }
      Err(item) => {
        // Full: re-store the item and park until a slot frees.
        this.item = Some(item);
        chan.register_send_waker(cx.waker());
        Poll::Pending
      }
    }
  }
}

impl<T> FusedFuture for Send<'_, T> {
  fn is_terminated(&self) -> bool {
    self.done
  }
}
