use super::*;

use alloc::sync::Arc;
use core::{
  future::Future,
  pin::Pin,
  sync::atomic::{AtomicUsize, Ordering},
  task::{Context, Poll},
};

use futures::{
  future::FusedFuture,
  task::{waker, ArcWake},
};

#[test]
fn bounded_try_send_recv_is_fifo() {
  let (tx, mut rx) = bounded::<u32>(4);
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx.try_recv(), Ok(2));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn bounded_try_send_reports_full() {
  let (tx, _rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap();
  assert!(matches!(tx.try_send(2), Err(TrySendError::Full(2))));
}

#[test]
fn try_send_after_receiver_drop_is_closed() {
  let (tx, rx) = bounded::<u32>(1);
  drop(rx);
  assert!(matches!(tx.try_send(9), Err(TrySendError::Closed(9))));
}

#[test]
fn try_recv_drains_queue_before_reporting_disconnected() {
  let (tx, mut rx) = bounded::<u32>(4);
  tx.try_send(1).unwrap();
  drop(tx);
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn bounded_reports_len_capacity_and_fullness() {
  let (tx, mut rx) = bounded::<u32>(2);
  assert_eq!(tx.capacity(), Some(2));
  assert!(tx.is_empty());
  assert!(!tx.is_closed());
  tx.try_send(1).unwrap();
  assert_eq!(tx.len(), 1);
  assert!(!tx.is_full());
  tx.try_send(2).unwrap();
  assert!(tx.is_full());
  assert_eq!(rx.try_recv(), Ok(1));
  assert!(!tx.is_full());
}

#[test]
fn cloning_a_sender_tracks_live_producers() {
  let (tx, mut rx) = bounded::<u32>(1);
  let tx2 = tx.clone();
  drop(tx); // one producer remains → still open
  tx2.try_send(7).unwrap();
  drop(tx2); // last producer gone
  assert_eq!(rx.try_recv(), Ok(7));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn is_closed_after_receiver_drop() {
  let (tx, rx) = bounded::<u32>(1);
  assert!(!tx.is_closed());
  drop(rx);
  assert!(tx.is_closed());
}

#[test]
fn unbounded_try_send_recv_is_fifo_across_blocks() {
  let (tx, mut rx) = unbounded::<u32>();
  for i in 0..100 {
    tx.try_send(i).unwrap(); // never full
  }
  for i in 0..100 {
    assert_eq!(rx.try_recv(), Ok(i));
  }
  assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn unbounded_has_no_capacity_and_is_never_full() {
  let (tx, _rx) = unbounded::<u32>();
  assert_eq!(tx.capacity(), None);
  assert!(!tx.is_full());
}

#[test]
fn unbounded_try_send_after_receiver_drop_is_closed() {
  let (tx, rx) = unbounded::<u32>();
  drop(rx);
  assert!(matches!(tx.try_send(1), Err(TrySendError::Closed(1))));
}

#[test]
fn unbounded_drains_queue_before_reporting_disconnected() {
  let (tx, mut rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  drop(tx);
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

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
fn recv_parks_then_wakes_on_send() {
  let (tx, mut rx) = bounded::<u32>(1);
  let (w, cw) = counting_waker();
  let mut fut = rx.recv();
  assert!(poll_once(&mut fut, &w).is_pending()); // empty → parks
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
  tx.try_send(5).unwrap(); // wakes the parked recv
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Some(5)));
}

#[test]
fn recv_ready_when_item_available() {
  let (tx, mut rx) = bounded::<u32>(1);
  tx.try_send(9).unwrap();
  let (w, _cw) = counting_waker();
  let mut fut = rx.recv();
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Some(9)));
}

#[test]
fn recv_returns_none_when_disconnected() {
  let (tx, mut rx) = bounded::<u32>(1);
  drop(tx);
  let (w, _cw) = counting_waker();
  let mut fut = rx.recv();
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(None));
}

#[test]
fn recv_future_reports_terminated_after_ready() {
  let (tx, mut rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap();
  let (w, _cw) = counting_waker();
  let mut fut = rx.recv();
  assert!(!fut.is_terminated());
  let _ = poll_once(&mut fut, &w);
  assert!(fut.is_terminated());
}

#[test]
fn send_ready_when_room() {
  let (tx, mut rx) = bounded::<u32>(2);
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(7);
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn send_parks_when_full_then_wakes_on_recv() {
  let (tx, mut rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap(); // fill
  let (w, cw) = counting_waker();
  let mut fut = tx.send(2);
  assert!(poll_once(&mut fut, &w).is_pending()); // full → parks
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
  assert_eq!(rx.try_recv(), Ok(1)); // frees a slot → wakes the parked send
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn send_returns_err_when_receiver_gone() {
  let (tx, rx) = bounded::<u32>(1);
  drop(rx);
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(3);
  match poll_once(&mut fut, &w) {
    Poll::Ready(Err(e)) => assert_eq!(e.into_inner(), 3),
    _ => panic!("expected a SendError"),
  }
}

#[test]
fn unbounded_send_is_immediate() {
  let (tx, mut rx) = unbounded::<u32>();
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(8);
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv(), Ok(8));
}

#[test]
fn send_future_reports_terminated_after_ready() {
  let (tx, mut rx) = bounded::<u32>(2);
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(1);
  assert!(!fut.is_terminated());
  let _ = poll_once(&mut fut, &w);
  assert!(fut.is_terminated());
  let _ = rx.try_recv();
}
