use super::*;

use alloc::{rc::Rc, sync::Arc};
use core::{
  cell::Cell,
  future::Future,
  pin::Pin,
  sync::atomic::{AtomicUsize, Ordering},
  task::{Context, Poll},
};

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
