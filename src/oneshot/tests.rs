use super::*;

use core::{
  cell::Cell,
  future::Future,
  pin::Pin,
  sync::atomic::{AtomicUsize, Ordering},
  task::{Context, Poll},
};
use std::{rc::Rc, sync::Arc};

use futures::{
  future::FusedFuture,
  task::{waker, ArcWake},
};

struct CountingWaker(AtomicUsize);

impl ArcWake for CountingWaker {
  fn wake_by_ref(arc: &Arc<Self>) {
    arc.0.fetch_add(1, Ordering::SeqCst);
  }
}

fn counting_waker() -> (core::task::Waker, Arc<CountingWaker>) {
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  (waker(cw.clone()), cw)
}

fn poll_once<F: Future + Unpin>(fut: &mut F, w: &core::task::Waker) -> Poll<F::Output> {
  Pin::new(fut).poll(&mut Context::from_waker(w))
}

#[test]
fn send_then_recv() {
  let (tx, mut rx) = channel::<u32>();
  tx.send(42).unwrap();
  let (w, _cw) = counting_waker();
  assert_eq!(poll_once(&mut rx, &w), Poll::Ready(Ok(42)));
}

#[test]
fn recv_parks_then_wakes_on_send() {
  let (tx, mut rx) = channel::<u32>();
  let (w, cw) = counting_waker();
  assert!(poll_once(&mut rx, &w).is_pending());
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
  tx.send(7).unwrap();
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  assert_eq!(poll_once(&mut rx, &w), Poll::Ready(Ok(7)));
}

#[test]
fn recv_canceled_when_sender_dropped() {
  let (tx, mut rx) = channel::<u32>();
  drop(tx);
  let (w, _cw) = counting_waker();
  assert_eq!(poll_once(&mut rx, &w), Poll::Ready(Err(Canceled)));
}

#[test]
fn send_errors_when_receiver_dropped() {
  let (tx, rx) = channel::<u32>();
  drop(rx);
  assert_eq!(tx.send(9), Err(9));
}

#[test]
fn try_recv_lifecycle() {
  let (tx, mut rx) = channel::<u32>();
  assert_eq!(rx.try_recv(), Ok(None)); // not yet
  tx.send(5).unwrap();
  assert_eq!(rx.try_recv(), Ok(Some(5))); // got it
}

#[test]
fn try_recv_canceled_when_sender_dropped() {
  let (tx, mut rx) = channel::<u32>();
  drop(tx);
  assert_eq!(rx.try_recv(), Err(Canceled));
}

#[test]
fn is_closed_after_receiver_drop() {
  let (tx, rx) = channel::<u32>();
  assert!(!tx.is_closed());
  drop(rx);
  assert!(tx.is_closed());
}

#[test]
fn receiver_future_reports_terminated_after_ready() {
  let (tx, mut rx) = channel::<u32>();
  tx.send(1).unwrap();
  let (w, _cw) = counting_waker();
  assert!(!rx.is_terminated());
  let _ = poll_once(&mut rx, &w);
  assert!(rx.is_terminated());
}

#[derive(Debug)]
struct DropCounter(Rc<Cell<usize>>);

impl Drop for DropCounter {
  fn drop(&mut self) {
    self.0.set(self.0.get() + 1);
  }
}

#[test]
fn drop_releases_unreceived_value() {
  let count = Rc::new(Cell::new(0));
  let (tx, rx) = channel::<DropCounter>();
  tx.send(DropCounter(count.clone())).unwrap();
  assert_eq!(count.get(), 0); // sent, not yet received
  drop(rx); // receiver drops without receiving → the value is released
  assert_eq!(count.get(), 1);
}

#[test]
fn receiver_drop_with_panicking_payload_drops_once() {
  use std::panic::{catch_unwind, AssertUnwindSafe};

  let drops = Rc::new(Cell::new(0));
  #[derive(Debug)]
  struct PanicOnDrop(Rc<Cell<usize>>);
  impl Drop for PanicOnDrop {
    fn drop(&mut self) {
      self.0.set(self.0.get() + 1);
      panic!("payload drop panics");
    }
  }

  let (tx, rx) = channel::<PanicOnDrop>();
  tx.send(PanicOnDrop(drops.clone())).unwrap();
  // Dropping the receiver runs the panicking payload Drop; the panic must NOT
  // cause a second drop of the same slot via Inner::drop.
  let _ = catch_unwind(AssertUnwindSafe(|| drop(rx)));
  assert_eq!(drops.get(), 1);
}

#[test]
fn recv_waker_registration_runs_vtable_outside_borrow() {
  use super::Inner;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A raw waker whose clone AND drop callbacks re-enter recv-waker registration —
  // which double-borrows recv_waker if they run while it is held.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let inner = unsafe { &*(p as *const Inner<u32>) };
    inner.register_recv_waker(&futures::task::noop_waker());
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(p: *const ()) {
    let inner = unsafe { &*(p as *const Inner<u32>) };
    inner.register_recv_waker(&futures::task::noop_waker());
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let (tx, rx) = channel::<u32>();
  let reentrant =
    unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&rx.inner) as *const (), &VT)) };
  rx.inner.register_recv_waker(&reentrant); // clone re-enters register
  rx.inner.register_recv_waker(&futures::task::noop_waker()); // replace drops the clone → re-enters
  let _ = tx;
}

#[test]
fn dropping_pending_receiver_clears_its_waker() {
  let (tx, mut rx) = channel::<u32>();
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let w = waker(cw.clone());
  assert!(poll_once(&mut rx, &w).is_pending()); // parks, registers a waker clone
  assert_eq!(Arc::strong_count(&cw), 3); // cw + w + the registered clone
  drop(rx); // Receiver::drop clears the registered waker
  assert_eq!(Arc::strong_count(&cw), 2); // back to cw + w
  let _ = tx;
}

#[test]
fn receiver_delivers_rechecked_value_despite_a_panicking_recv_waker_drop() {
  use super::Inner;
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};

  // A waker whose CLONE delivers the value (so the recheck finds one) and whose DROP
  // panics — but whose WAKE is a no-op, so teardown can consume it without dropping.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let inner = unsafe { &*(p as *const Inner<u32>) };
    unsafe { (*inner.value.get()).write(42) };
    inner.value_present.set(true);
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(_: *const ()) {
    panic!("recv waker drop panics");
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let (_tx, mut rx) = channel::<u32>();
  let waker = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&rx.inner) as *const (), &VT)) };

  // value absent → register (the clone delivers 42) → recheck → Ok(42). The recheck no
  // longer clears the recv waker inline, so the panicking-drop waker is not dropped here
  // and the value is delivered.
  assert!(matches!(
    Pin::new(&mut rx).poll(&mut Context::from_waker(&waker)),
    Poll::Ready(Ok(42))
  ));
  // Consume the leftover waker clones by waking (a no-op) rather than dropping them.
  rx.inner.wake_receiver();
  waker.wake();
}

#[test]
fn canceled_display_and_debug() {
  assert_eq!(
    format!("{Canceled}"),
    "oneshot sender dropped without sending a value"
  );
  assert_eq!(format!("{Canceled:?}"), "Canceled");
}

#[test]
fn try_recv_after_completion_is_none() {
  let (tx, mut rx) = channel::<u32>();
  tx.send(5).unwrap();
  assert_eq!(rx.try_recv(), Ok(Some(5)));
  // `done` is set after the value is taken — a further try_recv yields Ok(None).
  assert_eq!(rx.try_recv(), Ok(None));
}

#[test]
fn poll_after_ready_is_pending() {
  let (tx, mut rx) = channel::<u32>();
  tx.send(5).unwrap();
  let (w, _cw) = counting_waker();
  assert_eq!(poll_once(&mut rx, &w), Poll::Ready(Ok(5)));
  // A spurious re-poll after completion stays Pending (`done`).
  assert_eq!(poll_once(&mut rx, &w), Poll::Pending);
}

#[test]
fn try_iter_yields_sent_value_then_nothing() {
  let (tx, mut rx) = channel::<u32>();
  tx.send(7).unwrap();
  assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![7]);
  // The value was consumed; a second drain yields nothing.
  assert_eq!(rx.try_iter().count(), 0);
}

#[test]
fn try_iter_preserves_cancellation_for_a_later_poll() {
  let (tx, mut rx) = channel::<u32>();
  drop(tx); // the sender cancels without sending
            // try_iter drains delivered values only; it must NOT swallow the cancellation, so a
            // later poll still observes `Canceled` instead of hanging on `Pending`.
  assert_eq!(rx.try_iter().next(), None);
  let (w, _cw) = counting_waker();
  assert_eq!(poll_once(&mut rx, &w), Poll::Ready(Err(Canceled)));
}
