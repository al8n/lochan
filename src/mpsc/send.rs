//! The `send` future.

use core::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::future::FusedFuture;

use super::{chan::Chan, channel::Sender, error::SendError};

/// The future returned by [`Sender::send`](super::Sender::send). It holds the pending
/// item (so it is `Unpin` only when `T: Unpin`) and implements [`FusedFuture`] so it
/// can be polled in `select_biased!` without `.fuse()`.
pub struct Send<'a, T> {
  sender: &'a Sender<T>,
  /// The message, kept here across every waker op so a panicking waker never drops it.
  item: Option<T>,
  done: bool,
  /// This future's send-waker registration id, if parked.
  waker_id: Option<u64>,
  /// The committed terminal result, recorded *before* the final wake. If that wake
  /// panics, a re-poll replays this rather than losing the outcome or hanging.
  outcome: Option<Result<(), SendError<T>>>,
}

impl<'a, T> Send<'a, T> {
  #[inline(always)]
  pub(super) fn new(sender: &'a Sender<T>, item: T) -> Self {
    Self {
      sender,
      item: Some(item),
      done: false,
      waker_id: None,
      outcome: None,
    }
  }

  /// Commits `result` as the terminal outcome and runs the post-commit effects
  /// (deregister this future's waker; wake the receiver on success). The outcome is
  /// recorded BEFORE any user code runs, so a panicking waker leaves it recoverable —
  /// a re-poll replays it.
  #[inline]
  fn commit(
    &mut self,
    result: Result<(), SendError<T>>,
    chan: &Chan<T>,
  ) -> Poll<Result<(), SendError<T>>> {
    let wake = result.is_ok();
    self.done = true;
    self.outcome = Some(result);
    if let Some(id) = self.waker_id.take() {
      chan.remove_send_waker(id);
    }
    if wake {
      chan.wake_receiver();
    }
    Poll::Ready(self.outcome.take().expect("committed outcome"))
  }
}

impl<T> Future for Send<'_, T> {
  type Output = Result<(), SendError<T>>;

  #[inline]
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // SAFETY: `Send` holds no self-referential state — `sender` is a reference, and
    // `item`/`outcome` are moved by value — so projecting `&mut Self` out of the
    // pinned reference never moves pinned data. (Generic over `T`, so it cannot rely
    // on `Self: Unpin`.)
    let this = unsafe { self.get_unchecked_mut() };
    // Replay a committed outcome (a prior poll finished the state machine but a waker
    // panicked before it could return).
    if let Some(outcome) = this.outcome.take() {
      return Poll::Ready(outcome);
    }
    if this.done {
      return Poll::Pending;
    }
    let chan = this.sender.chan();
    if !chan.receiver_alive() {
      let item = this.item.take().expect("message present");
      return this.commit(Err(SendError::new(item)), chan);
    }
    // `try_push` runs no user code (it moves the item into storage), so it cannot
    // panic; the message is only ever briefly out of `this.item` across it.
    let item = this.item.take().expect("message present");
    match chan.try_push(item) {
      Ok(()) => this.commit(Ok(()), chan),
      Err(item) => {
        // Full: restore the message, then (re-)register. With the message back in
        // `this.item`, a panicking waker clone/drop below cannot lose it.
        this.item = Some(item);
        if let Some(old_id) = this.waker_id.take() {
          chan.remove_send_waker(old_id);
        }
        let id = chan.add_send_waker(cx.waker());
        this.waker_id = Some(id);
        // RECHECK closure + capacity — the (re-)registration callbacks may have freed
        // a slot or closed the receiver. Closure first, so we never enqueue into a
        // closed channel.
        if !chan.receiver_alive() {
          let item = this.item.take().expect("message present");
          return this.commit(Err(SendError::new(item)), chan);
        }
        let item = this.item.take().expect("message present");
        match chan.try_push(item) {
          Ok(()) => this.commit(Ok(()), chan),
          Err(item) => {
            this.item = Some(item);
            Poll::Pending
          }
        }
      }
    }
  }
}

impl<T> FusedFuture for Send<'_, T> {
  #[inline(always)]
  fn is_terminated(&self) -> bool {
    self.done
  }
}

impl<T> Drop for Send<'_, T> {
  #[inline]
  fn drop(&mut self) {
    // Remove a still-registered waker (a future dropped while parked). A completed
    // future already cleared its registration in `commit`.
    if let Some(id) = self.waker_id.take() {
      self.sender.chan().remove_send_waker(id);
    }
  }
}
