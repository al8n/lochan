//! One-shot channel: a single value sent once from the producer to the consumer.
//!
//! `!Send`, no-atomics. The [`Sender`] delivers one value synchronously (consuming
//! itself); the [`Receiver`] is itself a `Future` (await it) and also offers a
//! non-blocking [`Receiver::try_recv`].

use core::{
  cell::{Cell, UnsafeCell},
  fmt,
  future::Future,
  mem::MaybeUninit,
  pin::Pin,
  task::{Context, Poll, Waker},
};
use std::rc::Rc;

use futures_core::future::FusedFuture;

use crate::cell::LocalCell;

/// Shared state. `!Send`; no atomics. The value lives in a single `MaybeUninit`
/// slot whose presence is tracked by `value_present`.
struct Inner<T> {
  value: UnsafeCell<MaybeUninit<T>>,
  /// The slot holds an initialized, not-yet-taken value.
  value_present: Cell<bool>,
  /// The sender has dropped (so the receiver observes `Canceled` if no value came).
  sender_dropped: Cell<bool>,
  /// The receiver has dropped (so the sender's `send` fails).
  receiver_dropped: Cell<bool>,
  recv_waker: LocalCell<Option<Waker>>,
}

impl<T> Inner<T> {
  fn register_recv_waker(&self, waker: &Waker) {
    // Tombstone: once the receiver is dropping, ignore registration — the only caller
    // then is a re-entrant waker drop, which must not repopulate `recv_waker` and have
    // it retained while the `Sender` keeps `Inner` alive.
    if self.receiver_dropped.get() {
      return;
    }
    // Clone OUTSIDE the borrow, and drop any replaced waker only AFTER it is released:
    // a raw-waker clone/drop callback may re-enter the channel.
    let waker = waker.clone();
    let old = self.recv_waker.borrow_mut().replace(waker);
    drop(old);
  }

  fn wake_receiver(&self) {
    // Take the waker out and drop the borrow BEFORE waking: a synchronous waker may
    // re-enter and register again, which would double-borrow `recv_waker`.
    let waker = self.recv_waker.borrow_mut().take();
    if let Some(waker) = waker {
      waker.wake();
    }
  }

  /// Clears the receiver's registered waker — used when a pending recv is canceled,
  /// so its waker is not retained. Drops the old waker after releasing the borrow.
  fn clear_recv_waker(&self) {
    let old = self.recv_waker.borrow_mut().take();
    drop(old);
  }

  /// Drops the slot value if present, clearing the flag *before* the drop so a
  /// panicking `T::drop` cannot trigger a second drop via another path.
  fn drop_value(&self) {
    if self.value_present.replace(false) {
      // SAFETY: the flag was true, so the slot held an initialized value, and is
      // now cleared before we run the (possibly panicking) `T::drop`.
      unsafe { (*self.value.get()).assume_init_drop() };
    }
  }
}

impl<T> Drop for Inner<T> {
  fn drop(&mut self) {
    // Backstop: a sent-but-never-received value (e.g. a leaked receiver) is dropped
    // here. Normally the receiver has already taken or dropped it.
    self.drop_value();
  }
}

/// Creates a one-shot channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
  let inner = Rc::new(Inner {
    value: UnsafeCell::new(MaybeUninit::uninit()),
    value_present: Cell::new(false),
    sender_dropped: Cell::new(false),
    receiver_dropped: Cell::new(false),
    recv_waker: LocalCell::new(None),
  });
  (
    Sender {
      inner: inner.clone(),
    },
    Receiver { inner, done: false },
  )
}

/// The sending half. Sends exactly one value, consuming itself. Not `Clone`.
pub struct Sender<T> {
  inner: Rc<Inner<T>>,
}

impl<T> Sender<T> {
  /// Sends `value`. Returns `Err(value)` if the receiver has already dropped.
  pub fn send(self, value: T) -> Result<(), T> {
    if self.inner.receiver_dropped.get() {
      return Err(value);
    }
    // SAFETY: the receiver is alive and `send` consumes `self`, so this is the only
    // write; the slot is currently uninitialized.
    unsafe { (*self.inner.value.get()).write(value) };
    self.inner.value_present.set(true);
    self.inner.wake_receiver();
    Ok(())
  }

  /// Returns `true` once the receiver has dropped.
  pub fn is_closed(&self) -> bool {
    self.inner.receiver_dropped.get()
  }
}

impl<T> Drop for Sender<T> {
  fn drop(&mut self) {
    self.inner.sender_dropped.set(true);
    if !self.inner.value_present.get() {
      // No value was sent — wake the receiver so it observes `Canceled`.
      self.inner.wake_receiver();
    }
  }
}

/// The receiving half. It is itself a `Future` resolving to the sent value, or
/// [`Canceled`] if the sender dropped without sending.
pub struct Receiver<T> {
  inner: Rc<Inner<T>>,
  done: bool,
}

impl<T> Receiver<T> {
  /// Takes the value without waiting: `Ok(Some(v))` if it has arrived, `Ok(None)` if
  /// not yet, `Err(Canceled)` if the sender dropped without sending.
  pub fn try_recv(&mut self) -> Result<Option<T>, Canceled> {
    if let Some(value) = self.take_value() {
      return Ok(Some(value));
    }
    // `take_value` returns `None` when already done, when nothing has been sent yet, or
    // when the sender canceled — surface the canceled case here.
    if !self.done && self.inner.sender_dropped.get() {
      self.done = true;
      return Err(Canceled);
    }
    Ok(None)
  }

  /// Takes a delivered value if one is present, WITHOUT observing cancellation, so a
  /// later poll/await still sees [`Canceled`]. This is what [`try_iter`](Self::try_iter)
  /// drains: routing it through [`try_recv`](Self::try_recv) would consume the canceled
  /// terminal (setting `done`) yet report it as `None`, stranding a later await on
  /// `Pending`.
  fn take_value(&mut self) -> Option<T> {
    if self.done {
      return None;
    }
    if self.inner.value_present.replace(false) {
      // SAFETY: the flag was true, so the slot held an initialized value; read it
      // out exactly once (the flag is already cleared).
      let value = unsafe { (*self.inner.value.get()).assume_init_read() };
      self.done = true;
      return Some(value);
    }
    None
  }

  /// Returns an iterator that yields the value if it has arrived (at most one item),
  /// without waiting.
  #[inline]
  pub fn try_iter(&mut self) -> TryIter<'_, T> {
    TryIter(self)
  }
}

impl<T> Future for Receiver<T> {
  type Output = Result<T, Canceled>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // `Receiver` holds `Rc<Inner>`, never `T`, so it is `Unpin`.
    let this = self.get_mut();
    if this.done {
      return Poll::Pending;
    }
    if this.inner.value_present.replace(false) {
      // SAFETY: the flag was true, so the slot held an initialized value; read it
      // out exactly once (the flag is already cleared).
      let value = unsafe { (*this.inner.value.get()).assume_init_read() };
      this.done = true;
      return Poll::Ready(Ok(value));
    }
    if this.inner.sender_dropped.get() {
      this.done = true;
      return Poll::Ready(Err(Canceled));
    }
    this.inner.register_recv_waker(cx.waker());
    // RECHECK: register_recv_waker's clone/drop callbacks may have delivered the value
    // or dropped the sender; re-check so a wake during registration is not lost. We do
    // NOT clear the just-registered waker on the Ready branches — dropping it could
    // panic and lose the value we just read out. The stale waker (only on this rare
    // re-entrant path) is dropped at a safe point: `Receiver::drop` or `Inner::drop`.
    if this.inner.value_present.replace(false) {
      // SAFETY: the flag was true, so the slot held an initialized value; read it
      // out exactly once (the flag is already cleared).
      let value = unsafe { (*this.inner.value.get()).assume_init_read() };
      this.done = true;
      return Poll::Ready(Ok(value));
    }
    if this.inner.sender_dropped.get() {
      this.done = true;
      return Poll::Ready(Err(Canceled));
    }
    Poll::Pending
  }
}

impl<T> FusedFuture for Receiver<T> {
  #[inline(always)]
  fn is_terminated(&self) -> bool {
    self.done
  }
}

impl<T> Drop for Receiver<T> {
  fn drop(&mut self) {
    self.inner.receiver_dropped.set(true);
    // Clear any registered waker (a canceled pending recv) so it is not retained.
    self.inner.clear_recv_waker();
    // The sender may have sent a value we never received; drop it (flag-first, so a
    // panicking `T::drop` cannot double-drop via `Inner::drop`).
    self.inner.drop_value();
  }
}

/// A non-blocking iterator over a [`Receiver`], returned by [`Receiver::try_iter`]. It
/// yields the value if it has arrived (at most one item), then `None`.
pub struct TryIter<'a, T>(&'a mut Receiver<T>);

impl<T> Iterator for TryIter<'_, T> {
  type Item = T;

  #[inline]
  fn next(&mut self) -> Option<T> {
    self.0.take_value()
  }
}

/// Error returned by a [`Receiver`] whose [`Sender`] dropped without sending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Canceled;

impl fmt::Display for Canceled {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("oneshot sender dropped without sending a value")
  }
}

impl core::error::Error for Canceled {}

#[cfg(all(test, feature = "std"))]
mod tests;
