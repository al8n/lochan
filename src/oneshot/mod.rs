//! One-shot channel: a single value sent once from the producer to the consumer.
//!
//! `!Send`, no-atomics. The [`Sender`] delivers one value synchronously (consuming
//! itself); the [`Receiver`] is itself a `Future` (await it) and also offers a
//! non-blocking [`Receiver::try_recv`].

use alloc::rc::Rc;
use core::{
  cell::{Cell, RefCell, UnsafeCell},
  fmt,
  future::Future,
  mem::MaybeUninit,
  pin::Pin,
  task::{Context, Poll, Waker},
};

use futures_core::future::FusedFuture;

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
  recv_waker: RefCell<Option<Waker>>,
}

impl<T> Inner<T> {
  fn register_recv_waker(&self, waker: &Waker) {
    *self.recv_waker.borrow_mut() = Some(waker.clone());
  }

  fn wake_receiver(&self) {
    if let Some(waker) = self.recv_waker.borrow_mut().take() {
      waker.wake();
    }
  }
}

impl<T> Drop for Inner<T> {
  fn drop(&mut self) {
    // Backstop: a sent-but-never-received value (e.g. a leaked receiver) is dropped
    // here. Normally the receiver has already taken or dropped it.
    if self.value_present.get() {
      // SAFETY: `value_present` ⇒ the slot holds an initialized value; drop it once.
      unsafe { (*self.value.get()).assume_init_drop() };
    }
  }
}

/// Creates a one-shot channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
  let inner = Rc::new(Inner {
    value: UnsafeCell::new(MaybeUninit::uninit()),
    value_present: Cell::new(false),
    sender_dropped: Cell::new(false),
    receiver_dropped: Cell::new(false),
    recv_waker: RefCell::new(None),
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
    if self.done {
      return Ok(None);
    }
    if self.inner.value_present.get() {
      // SAFETY: `value_present` ⇒ the slot holds an initialized value; read it once.
      let value = unsafe { (*self.inner.value.get()).assume_init_read() };
      self.inner.value_present.set(false);
      self.done = true;
      return Ok(Some(value));
    }
    if self.inner.sender_dropped.get() {
      self.done = true;
      return Err(Canceled);
    }
    Ok(None)
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
    if this.inner.value_present.get() {
      // SAFETY: `value_present` ⇒ the slot holds an initialized value; read it once.
      let value = unsafe { (*this.inner.value.get()).assume_init_read() };
      this.inner.value_present.set(false);
      this.done = true;
      return Poll::Ready(Ok(value));
    }
    if this.inner.sender_dropped.get() {
      this.done = true;
      return Poll::Ready(Err(Canceled));
    }
    this.inner.register_recv_waker(cx.waker());
    Poll::Pending
  }
}

impl<T> FusedFuture for Receiver<T> {
  fn is_terminated(&self) -> bool {
    self.done
  }
}

impl<T> Drop for Receiver<T> {
  fn drop(&mut self) {
    self.inner.receiver_dropped.set(true);
    if self.inner.value_present.get() {
      // The sender sent a value we never received; drop it now.
      // SAFETY: `value_present` ⇒ the slot holds an initialized value; drop it once.
      unsafe { (*self.inner.value.get()).assume_init_drop() };
      self.inner.value_present.set(false);
    }
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

#[cfg(test)]
mod tests;
