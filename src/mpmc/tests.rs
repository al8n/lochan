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

#[test]
fn bounded_try_send_recv_is_fifo() {
  let (tx, rx) = bounded::<u32>(4);
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
  let (tx, rx) = bounded::<u32>(4);
  tx.try_send(1).unwrap();
  drop(tx);
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn bounded_reports_len_capacity_and_fullness() {
  let (tx, rx) = bounded::<u32>(2);
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
  let (tx, rx) = bounded::<u32>(1);
  let tx2 = tx.clone();
  drop(tx); // one producer remains → still open
  tx2.try_send(7).unwrap();
  drop(tx2); // last producer gone
  assert_eq!(rx.try_recv(), Ok(7));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn is_closed_after_last_receiver_drop() {
  let (tx, rx) = bounded::<u32>(1);
  assert!(!tx.is_closed());
  drop(rx);
  assert!(tx.is_closed());
}

#[test]
fn cloning_a_receiver_keeps_the_channel_open_until_the_last_drops() {
  let (tx, rx) = bounded::<u32>(2);
  let rx2 = rx.clone();
  drop(rx); // one consumer remains → still open for sends
  assert!(!tx.is_closed());
  tx.try_send(7).unwrap();
  assert_eq!(rx2.try_recv(), Ok(7));
  drop(rx2); // last consumer gone → closed
  assert!(tx.is_closed());
  assert!(matches!(tx.try_send(8), Err(TrySendError::Closed(8))));
}

#[test]
fn sender_sees_closed_only_after_the_last_receiver_drops() {
  let (tx, rx) = unbounded::<u32>();
  let rx2 = rx.clone();
  drop(rx);
  // One receiver still alive: a send must succeed, not report Closed.
  tx.try_send(1).unwrap();
  assert!(!tx.is_closed());
  drop(rx2); // now the last receiver is gone
  assert!(tx.is_closed());
  assert!(matches!(tx.try_send(2), Err(TrySendError::Closed(2))));
}

#[test]
fn two_receivers_split_the_queued_items() {
  let (tx, rx) = unbounded::<u32>();
  let rx2 = rx.clone();
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  // Either consumer may pop; each item is delivered exactly once, in FIFO order.
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx2.try_recv(), Ok(2));
  assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
  assert_eq!(rx2.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn unbounded_try_send_recv_is_fifo_across_blocks() {
  let (tx, rx) = unbounded::<u32>();
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
  let (tx, rx) = unbounded::<u32>();
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
  let (tx, rx) = bounded::<u32>(1);
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
  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(9).unwrap();
  let (w, _cw) = counting_waker();
  let mut fut = rx.recv();
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Some(9)));
}

#[test]
fn recv_returns_none_when_disconnected() {
  let (tx, rx) = bounded::<u32>(1);
  drop(tx);
  let (w, _cw) = counting_waker();
  let mut fut = rx.recv();
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(None));
}

#[test]
fn recv_future_reports_terminated_after_ready() {
  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap();
  let (w, _cw) = counting_waker();
  let mut fut = rx.recv();
  assert!(!fut.is_terminated());
  let _ = poll_once(&mut fut, &w);
  assert!(fut.is_terminated());
}

#[test]
fn two_parked_receivers_one_send_wakes_both_one_gets_the_item() {
  // A single send wakes EVERY parked receiver; exactly one re-poll pops the item and
  // the other re-parks (finding nothing).
  let (tx, rx) = bounded::<u32>(1);
  let rx2 = rx.clone();
  let (w1, cw1) = counting_waker();
  let (w2, cw2) = counting_waker();
  let mut f1 = rx.recv();
  let mut f2 = rx2.recv();
  assert!(poll_once(&mut f1, &w1).is_pending()); // both empty → both park
  assert!(poll_once(&mut f2, &w2).is_pending());
  assert_eq!(cw1.0.load(Ordering::SeqCst), 0);
  assert_eq!(cw2.0.load(Ordering::SeqCst), 0);

  tx.try_send(42).unwrap(); // one item → wakes BOTH parked receivers
  assert_eq!(cw1.0.load(Ordering::SeqCst), 1);
  assert_eq!(cw2.0.load(Ordering::SeqCst), 1);

  // The first to re-poll gets the single item; the second re-parks empty-handed.
  assert_eq!(poll_once(&mut f1, &w1), Poll::Ready(Some(42)));
  assert!(poll_once(&mut f2, &w2).is_pending());

  // A second item then resolves the still-parked receiver.
  tx.try_send(43).unwrap();
  assert_eq!(cw2.0.load(Ordering::SeqCst), 2);
  assert_eq!(poll_once(&mut f2, &w2), Poll::Ready(Some(43)));
}

#[test]
fn dropping_a_parked_recv_unregisters_its_waker() {
  let (tx, rx) = bounded::<u32>(1);
  let (w, cw) = counting_waker();
  {
    let mut fut = rx.recv();
    assert!(poll_once(&mut fut, &w).is_pending()); // park → registers waker
                                                   // fut drops here → its waker is unregistered
  }
  tx.try_send(1).unwrap(); // wake_receivers finds no waker to wake
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
}

#[test]
fn re_polling_a_parked_recv_replaces_its_waker() {
  // A parked recv, woken and re-polled while still empty, must replace its registration
  // rather than leave a second entry behind.
  let (tx, rx) = unbounded::<u32>();
  let (w, cw) = counting_waker();
  let mut fut = rx.recv();
  assert!(poll_once(&mut fut, &w).is_pending()); // park #1
  assert!(poll_once(&mut fut, &w).is_pending()); // re-poll: still empty → re-park (replace)
                                                 // Exactly one registration remains, so a single send wakes exactly once.
  tx.try_send(7).unwrap();
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Some(7)));
}

#[test]
fn send_ready_when_room() {
  let (tx, rx) = bounded::<u32>(2);
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(7);
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn send_parks_when_full_then_wakes_on_recv() {
  let (tx, rx) = bounded::<u32>(1);
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
fn parked_send_wakes_with_closed_only_after_last_receiver_drops() {
  let (tx, rx) = bounded::<u32>(1);
  let rx2 = rx.clone();
  tx.try_send(0).unwrap(); // fill
  let (w, cw) = counting_waker();
  let mut fut = tx.send(1);
  assert!(poll_once(&mut fut, &w).is_pending()); // full → parks
  drop(rx); // one receiver remains → channel still open, parked send untouched
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
  assert!(poll_once(&mut fut, &w).is_pending()); // still full, still open
  drop(rx2); // last receiver gone → wakes the parked send to observe Closed
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  match poll_once(&mut fut, &w) {
    Poll::Ready(Err(e)) => assert_eq!(e.into_inner(), 1),
    other => panic!("expected a SendError, got {other:?}"),
  }
}

#[test]
fn unbounded_send_is_immediate() {
  let (tx, rx) = unbounded::<u32>();
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(8);
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv(), Ok(8));
}

#[test]
fn send_future_reports_terminated_after_ready() {
  let (tx, rx) = bounded::<u32>(2);
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(1);
  assert!(!fut.is_terminated());
  let _ = poll_once(&mut fut, &w);
  assert!(fut.is_terminated());
  let _ = rx.try_recv();
}

#[test]
fn dropping_a_parked_send_unregisters_its_waker() {
  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(0).unwrap(); // fill
  let (w, cw) = counting_waker();
  {
    let mut fut = tx.send(1);
    assert!(poll_once(&mut fut, &w).is_pending()); // park → registers waker
                                                   // fut drops here → its waker is unregistered
  }
  rx.try_recv().unwrap(); // frees a slot → wake_senders finds no waker to wake
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
}

#[test]
fn multiple_parked_sends_all_wake_on_recv() {
  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(0).unwrap(); // fill
  let tx2 = tx.clone();
  let (w1, cw1) = counting_waker();
  let (w2, cw2) = counting_waker();
  let mut f1 = tx.send(1);
  let mut f2 = tx2.send(2);
  assert!(poll_once(&mut f1, &w1).is_pending());
  assert!(poll_once(&mut f2, &w2).is_pending());
  rx.try_recv().unwrap(); // free a slot → wake_senders wakes BOTH parked sends
  assert_eq!(cw1.0.load(Ordering::SeqCst), 1);
  assert_eq!(cw2.0.load(Ordering::SeqCst), 1);
  let _ = poll_once(&mut f1, &w1);
  let _ = poll_once(&mut f2, &w2);
}

#[test]
fn last_receiver_drop_wakes_sender_before_panicking_payload_drop() {
  use std::panic::{catch_unwind, AssertUnwindSafe};

  let drops = Rc::new(Cell::new(0));
  #[derive(Debug)]
  struct MaybePanic(u32, Rc<Cell<usize>>);
  impl Drop for MaybePanic {
    fn drop(&mut self) {
      self.1.set(self.1.get() + 1);
      if self.0 == 99 {
        panic!("payload drop panics");
      }
    }
  }

  let (tx, rx) = bounded::<MaybePanic>(1);
  tx.try_send(MaybePanic(99, drops.clone())).unwrap(); // queue a panicking payload
  let (w, cw) = counting_waker();
  let mut fut = tx.send(MaybePanic(2, drops.clone())); // park (full)
  assert!(poll_once(&mut fut, &w).is_pending());
  // Dropping the last receiver wakes the parked sender BEFORE draining (the queued
  // payload's Drop panics); the sender must still have been woken.
  let _ = catch_unwind(AssertUnwindSafe(|| drop(rx)));
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  let _ = poll_once(&mut fut, &w); // resolves to Err; MaybePanic(2) drops cleanly
}

#[test]
fn completed_send_releases_its_waker() {
  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(0).unwrap(); // fill
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let w = waker(cw.clone());
  let mut fut = tx.send(1);
  assert!(poll_once(&mut fut, &w).is_pending()); // park → stores a waker clone
  rx.try_recv().unwrap(); // free a slot → wakes the parked send
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(())))); // completes
                                                                   // The completed future released its stored waker clone: only `cw` and `w` hold one.
  assert_eq!(Arc::strong_count(&cw), 2);
}

#[test]
fn completed_recv_releases_its_waker() {
  let (tx, rx) = bounded::<u32>(1);
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let w = waker(cw.clone());
  let mut fut = rx.recv();
  assert!(poll_once(&mut fut, &w).is_pending()); // park → stores a waker clone
  assert_eq!(Arc::strong_count(&cw), 3); // cw + w + the registered clone
  tx.try_send(1).unwrap(); // wakes the parked recv
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Some(1))); // completes
                                                             // The completed future released its stored waker clone: only `cw` and `w` hold one.
  assert_eq!(Arc::strong_count(&cw), 2);
}

#[test]
fn send_waker_ops_run_vtable_outside_borrow() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A raw waker whose clone AND drop callbacks re-enter the channel's send-waker
  // registration — which double-borrows send_wakers if they run while it is held.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.add_send_waker(&futures::task::noop_waker());
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.add_send_waker(&futures::task::noop_waker());
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let chan = Chan::<u32>::bounded(1);
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  let id = chan.add_send_waker(&reentrant); // clones reentrant → clone re-enters add
  chan.remove_send_waker(id); // drops the stored clone → drop re-enters add
}

#[test]
fn recv_waker_ops_run_vtable_outside_borrow() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A raw waker whose clone AND drop callbacks re-enter the channel's recv-waker
  // registration — which double-borrows recv_wakers if they run while it is held.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.add_recv_waker(&futures::task::noop_waker());
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.add_recv_waker(&futures::task::noop_waker());
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let chan = Chan::<u32>::bounded(1);
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  let id = chan.add_recv_waker(&reentrant); // clones reentrant → clone re-enters add
  chan.remove_recv_waker(id); // drops the stored clone → drop re-enters add
}

#[test]
fn dropping_a_pending_recv_clears_its_waker() {
  let (tx, rx) = bounded::<u32>(1);
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let w = waker(cw.clone());
  {
    let mut fut = rx.recv();
    assert!(poll_once(&mut fut, &w).is_pending()); // empty → parks, registers a clone
    assert_eq!(Arc::strong_count(&cw), 3); // cw + w + the registered clone
                                           // fut drops here → clears the registered waker
  }
  assert_eq!(Arc::strong_count(&cw), 2); // back to cw + w
  let _ = tx;
}

#[test]
fn send_rechecks_readiness_after_registration() {
  use super::chan::Chan;
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};

  // A raw waker whose CLONE frees a slot — a re-entrant callback that changes
  // readiness during the park-path registration. The recheck must then complete the
  // send instead of parking with a lost wake.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.pop(); // free a slot during registration
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  chan.try_push(0).unwrap(); // fill
  let tx = Sender::new(chan.clone());
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  let mut fut = tx.send(1);
  let res = Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant));
  assert!(matches!(res, Poll::Ready(Ok(())))); // the recheck saw the freed slot
}

#[test]
fn recv_rechecks_readiness_after_registration() {
  use super::chan::Chan;
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};

  // A raw waker whose CLONE pushes an item — a re-entrant callback that makes the
  // channel ready during the park-path registration. The recheck must then complete
  // the recv instead of parking with a lost wake.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    let _ = chan.try_push(99); // Ignoring Err: empty cap-2 channel, the push succeeds
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let chan = Chan::<u32>::bounded(2);
  let _tx = Sender::new(chan.clone());
  let rx = Receiver::new(chan.clone());
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  let mut fut = rx.recv();
  let res = Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant));
  assert!(matches!(res, Poll::Ready(Some(99)))); // the recheck saw the pushed item
}

#[test]
fn drain_runs_payload_drop_outside_flavor_borrow() {
  struct Reentrant(Option<Sender<Reentrant>>);
  impl Drop for Reentrant {
    fn drop(&mut self) {
      if let Some(tx) = &self.0 {
        let _ = tx.is_full(); // Ignoring the result — the point is to re-borrow the channel
      }
    }
  }
  let (tx, rx) = bounded::<Reentrant>(4);
  tx.try_send(Reentrant(Some(tx.clone()))).unwrap();
  tx.try_send(Reentrant(None)).unwrap();
  // Dropping the last receiver drains the queue; each payload's Drop re-borrows the
  // channel via the live sender, which overlaps the flavor borrow if drain dropped
  // items while holding it (RefCell panic in debug, UB under Miri release).
  drop(rx);
}

#[test]
fn send_rechecks_closure_after_registration() {
  use super::chan::Chan;
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};

  // A raw waker whose CLONE closes the receiver (and drains, freeing the slot) — a
  // re-entrant callback that closes the channel during park-path registration. The
  // recheck must return Err, not enqueue into the now-closed channel.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.decr_receivers();
    chan.drain();
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  chan.try_push(0).unwrap(); // fill
  let tx = Sender::new(chan.clone());
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  let mut fut = tx.send(1);
  match Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant)) {
    Poll::Ready(Err(e)) => assert_eq!(e.into_inner(), 1),
    other => panic!("expected Err for a closed channel, got {other:?}"),
  }
}

#[test]
fn recv_recheck_observes_disconnect_during_registration() {
  use core::{
    ptr,
    task::{Context, RawWaker, RawWakerVTable, Waker},
  };

  std::thread_local! {
    static LAST_TX: core::cell::RefCell<Option<Sender<u32>>> =
      const { core::cell::RefCell::new(None) };
  }
  // A waker whose CLONE drops the last sender, so registration leaves the channel
  // disconnected and the recv's recheck takes the `senders() == 0` branch.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    LAST_TX.with(|t| drop(t.borrow_mut().take()));
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let (tx, rx) = unbounded::<u32>();
  LAST_TX.with(|t| *t.borrow_mut() = Some(tx));
  let waker = unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VT)) };

  // First check: empty, sender alive → register → the clone drops the sender → recheck
  // sees `senders() == 0` → disconnected.
  let mut fut = rx.recv();
  assert_eq!(
    Pin::new(&mut fut).poll(&mut Context::from_waker(&waker)),
    Poll::Ready(None)
  );
}

#[test]
fn last_receiver_drop_drains_even_if_a_sender_waker_panics() {
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let drops = Rc::new(Cell::new(0));
  // Field 0 (the Sender) is held to form the Rc cycle through Chan, then dropped to
  // break it — it is never read, hence the allow.
  #[allow(dead_code)]
  struct Cyclic(Option<Sender<Cyclic>>, Rc<Cell<usize>>);
  impl Drop for Cyclic {
    fn drop(&mut self) {
      self.1.set(self.1.get() + 1);
    }
  }

  let (tx, rx) = bounded::<Cyclic>(1);
  // A queued payload owning a Sender clone — an Rc cycle through Chan that only the
  // drain can break.
  tx.try_send(Cyclic(Some(tx.clone()), drops.clone()))
    .unwrap();
  // A parked sender whose waker panics on wake.
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  let mut fut = tx.send(Cyclic(None, drops.clone()));
  assert!(Pin::new(&mut fut)
    .poll(&mut Context::from_waker(&panicking))
    .is_pending());
  // Dropping the last receiver wakes the parked sender (which panics); the drain must
  // still run, breaking the cycle and freeing the queued payload.
  let _ = catch_unwind(AssertUnwindSafe(|| drop(rx)));
  assert_eq!(drops.get(), 1); // the queued payload was drained despite the panic
  drop(fut); // releases the parked send's Cyclic(None)
}

#[test]
fn drain_continues_past_a_panicking_payload() {
  use std::panic::{catch_unwind, AssertUnwindSafe};

  let drops = Rc::new(Cell::new(0));
  // `sender` (field, never read) holds a Sender clone forming an Rc cycle that only
  // draining can break.
  #[allow(dead_code)]
  struct Item {
    panic_on_drop: bool,
    sender: Option<Sender<Item>>,
    drops: Rc<Cell<usize>>,
  }
  impl Drop for Item {
    fn drop(&mut self) {
      self.drops.set(self.drops.get() + 1);
      if self.panic_on_drop {
        panic!("payload drop panics");
      }
    }
  }

  let (tx, rx) = bounded::<Item>(4);
  tx.try_send(Item {
    panic_on_drop: true,
    sender: None,
    drops: drops.clone(),
  })
  .unwrap();
  tx.try_send(Item {
    panic_on_drop: false,
    sender: Some(tx.clone()),
    drops: drops.clone(),
  })
  .unwrap();
  // Dropping the last receiver drains: the first payload's Drop panics, but the drain
  // must continue and free the later (Sender-owning) payload, breaking the Rc cycle.
  let _ = catch_unwind(AssertUnwindSafe(|| drop(rx)));
  assert_eq!(drops.get(), 2); // both freed despite the first's panic
}

#[test]
fn send_replays_committed_ok_after_a_wake_panic() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A recv waker that panics on wake.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("recv waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let (tx, rx) = bounded::<u32>(2);
  // Register a panicking recv waker directly, so the send's wake_receivers panics.
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  tx.chan().add_recv_waker(&panicking);

  let (w, _cw) = counting_waker();
  let mut fut = tx.send(7);
  // The send pushes the item (commits Ok), then wake_receivers panics.
  assert!(catch_unwind(AssertUnwindSafe(|| poll_once(&mut fut, &w))).is_err());
  // Re-poll: the committed Ok is replayed (not a hang or an expect-panic).
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  // The item was delivered despite the wake panic.
  assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn recv_does_not_lose_a_queued_item_when_waking_a_sender_panics() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A parked sender whose waker panics on wake.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap(); // queue item 1, filling the channel
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  let mut sfut = tx.send(2);
  assert!(Pin::new(&mut sfut)
    .poll(&mut Context::from_waker(&panicking))
    .is_pending()); // parks (full)
                    // `try_recv` pops item 1, parks it in the redelivery slot, then wakes the parked sender
                    // (which panics). The unwinding wake leaves item 1 in the slot rather than dropping it.
  assert!(catch_unwind(AssertUnwindSafe(|| rx.try_recv())).is_err());
  // Item 1 survived the wake panic — the next recv drains it from the redelivery slot.
  assert_eq!(rx.try_recv(), Ok(1));
  drop(sfut);
}

#[test]
fn a_redelivered_item_precedes_the_queue_in_fifo_order() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let (tx, rx) = bounded::<u32>(2);
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap(); // queue is [1, 2], full
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  let mut sfut = tx.send(3);
  assert!(Pin::new(&mut sfut)
    .poll(&mut Context::from_waker(&panicking))
    .is_pending()); // parks (full)
                    // The wake while item 1 is parked panics; item 1 stays in the redelivery slot and item
                    // 2 stays queued.
  assert!(catch_unwind(AssertUnwindSafe(|| rx.try_recv())).is_err());
  // The redelivered head (1) is delivered before the still-queued item (2).
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx.try_recv(), Ok(2));
  drop(sfut);
}

#[test]
fn a_redelivered_item_keeps_the_stream_live_and_non_empty() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use futures_core::stream::FusedStream;
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap();
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  let mut sfut = tx.send(2);
  assert!(Pin::new(&mut sfut)
    .poll(&mut Context::from_waker(&panicking))
    .is_pending());
  // The wake while item 1 is parked panics; item 1 stays in the redelivery slot.
  assert!(catch_unwind(AssertUnwindSafe(|| rx.try_recv())).is_err());
  // Drop the pending send and the last sender: the backing queue is empty and senders==0.
  drop(sfut);
  drop(tx);
  // The redelivered item is still pending, so the channel is neither empty nor terminated
  // and its length reflects the in-transit item — a Stream consumer must keep polling.
  assert!(!rx.is_empty());
  assert_eq!(rx.len(), 1);
  assert!(!rx.is_terminated());
  // It is still deliverable; only after draining it is the channel empty and terminated.
  assert_eq!(rx.try_recv(), Ok(1));
  assert!(rx.is_empty());
  assert!(rx.is_terminated());
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn a_redelivered_item_can_transiently_exceed_capacity() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(1).unwrap();
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  let mut sfut = tx.send(2);
  assert!(Pin::new(&mut sfut)
    .poll(&mut Context::from_waker(&panicking))
    .is_pending());
  // recv pops 1, parks it, wakes (panics) → 1 in redelivery, the backing queue is empty.
  assert!(catch_unwind(AssertUnwindSafe(|| rx.try_recv())).is_err());
  drop(sfut);
  // The queue freed a slot, so a fresh send is admitted while the in-transit item is still
  // pending: the channel momentarily holds capacity + 1. Documented bound, not an unbounded
  // leak — the slot holds at most one.
  tx.try_send(3).unwrap();
  assert_eq!(tx.capacity(), Some(1));
  assert_eq!(rx.len(), 2);
  // FIFO drain: the redelivered head (1) before the queued item (3).
  assert_eq!(rx.try_recv(), Ok(1));
  assert_eq!(rx.try_recv(), Ok(3));
  assert!(rx.is_empty());
}

#[test]
fn wake_all_isolates_multiple_panicking_wakers() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A waker that panics on wake.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  // Register two panicking sender wakers with one counting waker between them.
  let p1 = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  let p2 = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  chan.add_send_waker(&p1);
  let (cw, cwc) = counting_waker();
  chan.add_send_waker(&cw);
  chan.add_send_waker(&p2);
  // Under `std`, `wake_senders` must wake the counting waker despite BOTH panicking wakers
  // around it, then resume a single panic — never a double-panic abort.
  assert!(catch_unwind(AssertUnwindSafe(|| chan.wake_senders())).is_err());
  assert_eq!(cwc.0.load(Ordering::SeqCst), 1);
}

#[test]
fn wake_senders_releases_the_borrow_before_a_reentrant_wake() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A waker whose WAKE re-enters the channel's send-waker registration — re-borrowing
  // `send_wakers` while `wake_senders` is running. If the take did not release the borrow
  // first, this double-borrows (debug panic / release UB).
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.add_send_waker(&futures::task::noop_waker());
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  chan.add_send_waker(&reentrant); // clones reentrant → stored in send_wakers
  chan.wake_senders(); // wakes it → its wake() re-enters add_send_waker
}

#[test]
fn wake_receivers_releases_the_borrow_before_a_reentrant_wake() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // The receiver-list mirror: a waker whose WAKE re-enters the recv-waker registration.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.add_recv_waker(&futures::task::noop_waker());
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  chan.add_recv_waker(&reentrant);
  chan.wake_receivers();
}

#[test]
fn last_receiver_drop_removes_stream_waker_even_if_a_sender_wake_panics() {
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};
  use futures_core::stream::Stream;
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &PANIC_VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static PANIC_VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let (tx, mut rx) = bounded::<u32>(1);
  // Park rx as a stream so it registers a stream waker (a stale registration).
  let (sw, swc) = counting_waker();
  assert!(Pin::new(&mut rx)
    .poll_next(&mut Context::from_waker(&sw))
    .is_pending());
  assert_eq!(Arc::strong_count(&swc), 3); // swc + sw + the registered clone
                                          // Fill the channel and park a sender whose waker panics on wake.
  tx.try_send(1).unwrap();
  let panic_w = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &PANIC_VT)) };
  let mut sfut = tx.send(2);
  assert!(Pin::new(&mut sfut)
    .poll(&mut Context::from_waker(&panic_w))
    .is_pending());
  // Dropping the last receiver wakes the panicking sender; the cleanup guard must still
  // remove rx's stream registration despite the unwind — no leaked recv waker.
  assert!(catch_unwind(AssertUnwindSafe(|| drop(rx))).is_err());
  assert_eq!(Arc::strong_count(&swc), 2); // the registered clone was removed
  drop(sfut);
}

#[test]
fn last_receiver_drop_removes_stream_waker_even_if_a_payload_drop_panics() {
  use core::task::Context;
  use futures_core::stream::Stream;
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A payload whose Drop panics.
  struct PanicOnDrop;
  impl Drop for PanicOnDrop {
    fn drop(&mut self) {
      panic!("payload drop panics");
    }
  }

  let (tx, mut rx) = bounded::<PanicOnDrop>(1);
  // Park rx as a stream so it registers a stream waker.
  let (sw, swc) = counting_waker();
  assert!(Pin::new(&mut rx)
    .poll_next(&mut Context::from_waker(&sw))
    .is_pending());
  assert_eq!(Arc::strong_count(&swc), 3); // swc + sw + the registered clone
                                          // Queue a payload whose Drop panics.
  tx.try_send(PanicOnDrop).unwrap();
  // Dropping the last receiver drains the panicking payload; the cleanup guard must still
  // remove rx's stream registration despite the unwind — no leaked recv waker.
  assert!(catch_unwind(AssertUnwindSafe(|| drop(rx))).is_err());
  assert_eq!(Arc::strong_count(&swc), 2); // the stream registration was removed
  drop(tx);
}

#[test]
fn committed_send_removes_its_send_waker_even_if_a_receiver_wake_panics() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A send waker whose CLONE frees a slot (pops the queued item) so the recheck commits Ok.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    let _ = chan.pop();
    RawWaker::new(p, &SEND_VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static SEND_VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  // A receiver waker that panics on wake, so commit's wake_receivers unwinds.
  unsafe fn vt_rclone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &RECV_VT)
  }
  unsafe fn vt_rwake(_: *const ()) {
    panic!("receiver waker panics on wake");
  }
  static RECV_VT: RawWakerVTable = RawWakerVTable::new(vt_rclone, vt_rwake, vt_rwake, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  let tx = Sender::new(chan.clone());
  chan.try_push(0).unwrap(); // fill
  let panic_rw = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &RECV_VT)) };
  chan.add_recv_waker(&panic_rw);
  // Send 7: full → registers the reentrant send waker (its clone frees the slot) → the
  // recheck commits Ok → wake_receivers panics. The completed send's waker registration
  // must still be removed despite the unwind.
  let reentrant =
    unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &SEND_VT)) };
  let mut fut = tx.send(7);
  assert!(catch_unwind(AssertUnwindSafe(|| {
    let _ = Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant));
  }))
  .is_err());
  assert_eq!(chan.send_wakers_len(), 0); // the registration was removed, not leaked
}

#[test]
fn recheck_disconnect_removes_the_just_registered_recv_waker() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A recv waker whose CLONE drops the last sender (held in the data-ptr Cell), so the
  // registration in poll_recv observes senders()==0 on the recheck.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let slot = unsafe { &*(p as *const Cell<Option<Sender<u32>>>) };
    let _ = slot.take(); // drops the last sender → senders()==0
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let chan = Chan::<u32>::unbounded();
  let tx = Sender::new(chan.clone());
  let rx = Receiver::new(chan.clone());
  let tx_slot = Cell::new(Some(tx));
  let waker = unsafe { Waker::from_raw(RawWaker::new(&tx_slot as *const _ as *const (), &VT)) };
  // poll_recv: empty + sender alive → registers `waker`, whose clone drops the last sender
  // → the recheck sees senders()==0 → Ready(None). The just-registered waker must be
  // removed, not retained, while rx and chan stay alive.
  let waker_id = Cell::new(None);
  assert!(matches!(
    rx.poll_recv(&mut Context::from_waker(&waker), &waker_id),
    Poll::Ready(None)
  ));
  assert_eq!(chan.recv_wakers_len(), 0); // the just-registered waker was removed
}

#[test]
fn final_item_recheck_keeps_stream_non_terminal_until_waker_cleared() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use futures_core::stream::{FusedStream, Stream};

  // A recv waker whose CLONE enqueues the final item AND drops the last sender, via the
  // Sender held in the data-ptr Cell.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let slot = unsafe { &*(p as *const Cell<Option<Sender<u32>>>) };
    if let Some(tx) = slot.take() {
      let _ = tx.try_send(99); // enqueue the final item; tx then drops → last sender gone
    }
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let chan = Chan::<u32>::unbounded();
  let tx = Sender::new(chan.clone());
  let mut rx = Receiver::new(chan.clone());
  let tx_slot = Cell::new(Some(tx));
  let waker = unsafe { Waker::from_raw(RawWaker::new(&tx_slot as *const _ as *const (), &VT)) };
  // poll_next: empty → registers `waker`; its clone enqueues 99 and drops the last sender;
  // the recheck delivers 99 while leaving the waker registered.
  assert_eq!(
    Pin::new(&mut rx).poll_next(&mut Context::from_waker(&waker)),
    Poll::Ready(Some(99))
  );
  // Drained and sender-less, but a stream waker is still registered, so termination must
  // NOT be reported — else a FusedStream consumer stops polling and strands the waker.
  assert!(!rx.is_terminated());
  // One more poll clears the stale registration at the poll-top stale-remove and returns
  // None; only then is the stream terminal.
  assert_eq!(
    Pin::new(&mut rx).poll_next(&mut Context::from_waker(&waker)),
    Poll::Ready(None)
  );
  assert!(rx.is_terminated());
  assert_eq!(chan.recv_wakers_len(), 0);
}

#[test]
fn try_take_rechecks_queue_when_a_reentrant_consumer_drains_redelivery() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A send waker whose WAKE re-enters as a receiver and drains the redelivery slot.
  unsafe fn vt_wake(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    let _ = chan.try_take(); // drains the parked redelivery item re-entrantly
  }
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let chan = Chan::<u32>::bounded(2);
  chan.try_push(1).unwrap();
  chan.try_push(2).unwrap(); // full: [1, 2]
                             // Register a parked sender (so has_parked_senders is true) whose wake drains redelivery.
  let waker = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  chan.add_send_waker(&waker);
  // try_take pops 1, parks it, wakes the sender; the wake drains 1 re-entrantly, so the
  // reclaim yields None. The fix must re-check the queue and return tail item 2, not None.
  assert_eq!(chan.try_take(), Some(2));
}

#[test]
fn final_item_recheck_keeps_recv_non_terminal_until_waker_cleared() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use futures_core::future::FusedFuture;

  // A recv waker whose CLONE enqueues the final item AND drops the last sender.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let slot = unsafe { &*(p as *const Cell<Option<Sender<u32>>>) };
    if let Some(tx) = slot.take() {
      let _ = tx.try_send(99); // enqueue the final item; tx then drops → last sender gone
    }
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let chan = Chan::<u32>::unbounded();
  let tx = Sender::new(chan.clone());
  let rx = Receiver::new(chan.clone());
  let tx_slot = Cell::new(Some(tx));
  let waker = unsafe { Waker::from_raw(RawWaker::new(&tx_slot as *const _ as *const (), &VT)) };
  let mut fut = rx.recv();
  // First poll registers `waker`; its clone enqueues 99 and drops the last sender; the
  // recheck delivers 99 while leaving the waker registered.
  assert_eq!(
    Pin::new(&mut fut).poll(&mut Context::from_waker(&waker)),
    Poll::Ready(Some(99))
  );
  // Completed, but the recheck left the waker registered, so the fused future is NOT yet
  // terminal — a consumer must poll once more to clear it.
  assert!(!fut.is_terminated());
  // The extra poll clears the stale registration WITHOUT consuming another item, and must
  // WAKE its waker so a fused consumer re-evaluates rather than parking with no wake source.
  let (cw, count) = counting_waker();
  assert_eq!(
    Pin::new(&mut fut).poll(&mut Context::from_waker(&cw)),
    Poll::Pending
  );
  assert!(fut.is_terminated());
  assert!(count.0.load(Ordering::SeqCst) >= 1);
  assert_eq!(chan.recv_wakers_len(), 0);
}

#[test]
fn committed_send_is_not_terminated_until_the_outcome_is_replayed() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use futures_core::future::FusedFuture;
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A reentrant send waker whose clone frees a slot (so the recheck commits Ok).
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    let _ = chan.pop();
    RawWaker::new(p, &SEND_VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  static SEND_VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  // A receiver waker that panics on wake, so the post-commit wake unwinds.
  unsafe fn vt_rclone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &RECV_VT)
  }
  unsafe fn vt_rwake(_: *const ()) {
    panic!("receiver waker panics on wake");
  }
  static RECV_VT: RawWakerVTable = RawWakerVTable::new(vt_rclone, vt_rwake, vt_rwake, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  let tx = Sender::new(chan.clone());
  chan.try_push(0).unwrap(); // fill
  let panic_rw = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &RECV_VT)) };
  chan.add_recv_waker(&panic_rw);
  let reentrant =
    unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &SEND_VT)) };
  let mut fut = tx.send(7);
  // First poll: full → registers the reentrant send waker (its clone frees the slot) → the
  // recheck commits Ok → wake_receivers panics, so the outcome is stored but not returned.
  assert!(catch_unwind(AssertUnwindSafe(|| {
    let _ = Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant));
  }))
  .is_err());
  // The send committed Ok but the panic skipped delivery; the future is NOT terminal until
  // the outcome is replayed — else a fused consumer drops it and loses the Ok result.
  assert!(!fut.is_terminated());
  // The replay poll yields the committed Ok, and only then is it terminal.
  assert!(matches!(
    Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant)),
    Poll::Ready(Ok(()))
  ));
  assert!(fut.is_terminated());
}

#[test]
fn last_receiver_drop_decrements_before_a_panicking_stream_waker_drop() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use futures_core::stream::Stream;
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A stream waker whose DROP panics.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(_: *const ()) {
    panic!("stream waker drop panics");
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let (tx, mut rx) = unbounded::<u32>();
  // Park the receiver as a stream, registering the panic-on-drop waker on its stream slot.
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  assert!(Pin::new(&mut rx)
    .poll_next(&mut Context::from_waker(&panicking))
    .is_pending());
  // Dropping the last receiver removes the stream registration (whose drop panics). The
  // receiver count must already be decremented, so senders observe disconnect — a panic
  // in the waker drop cannot leave them believing a receiver is still alive.
  assert!(catch_unwind(AssertUnwindSafe(|| drop(rx))).is_err());
  assert!(tx.is_closed());
  assert!(matches!(tx.try_send(9), Err(TrySendError::Closed(9))));
  // The original waker's own drop also panics; forget it so it does not fire uncaught at
  // end of scope. The stored clone already exercised the panicking drop above, caught.
  core::mem::forget(panicking);
}

#[test]
fn committed_send_wakes_a_parked_receiver_before_a_panicking_send_waker_drop() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A send waker whose CLONE frees a slot (pops the queued item) so the recheck path
  // commits Ok, and whose DROP panics — that drop runs inside commit's deregister.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    let _ = chan.pop(); // free the slot so the recheck `try_push` succeeds
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(_: *const ()) {
    panic!("send waker drop panics");
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let chan = Chan::<u32>::bounded(1);
  let tx = Sender::new(chan.clone());
  chan.try_push(0).unwrap(); // fill
                             // A parked "loser" receiver, registered directly; commit's wake must reach it.
  let (rw, rcw) = counting_waker();
  chan.add_recv_waker(&rw);
  // Send 7: full → registers the reentrant waker (whose clone frees the slot) → the
  // recheck pushes 7 → commit(Ok) wakes the receiver, THEN deregisters the waker, whose
  // drop panics. The wake must have already reached the parked receiver.
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  let mut fut = tx.send(7);
  assert!(catch_unwind(AssertUnwindSafe(|| {
    let _ = Pin::new(&mut fut).poll(&mut Context::from_waker(&reentrant));
  }))
  .is_err());
  // The receiver was woken BEFORE the send-waker drop panicked.
  assert_eq!(rcw.0.load(Ordering::SeqCst), 1);
  // The original waker's own drop also panics; forget it so it does not fire uncaught at
  // end of scope. The stored clone already exercised the panicking drop above, caught.
  core::mem::forget(reentrant);
}

#[test]
fn send_keeps_message_across_a_panicking_waker_clone() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A waker whose CLONE panics.
  unsafe fn vt_clone(_: *const ()) -> RawWaker {
    panic!("waker clone panics");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let (tx, rx) = bounded::<u32>(1);
  tx.try_send(0).unwrap(); // fill
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };

  let mut fut = tx.send(7);
  // Full → the send parks, registering its waker — whose clone panics. The message
  // must stay in the future, not be dropped.
  assert!(catch_unwind(AssertUnwindSafe(|| {
    let _ = Pin::new(&mut fut).poll(&mut Context::from_waker(&panicking));
  }))
  .is_err());
  // Re-poll with a normal waker: the message survived; the send parks normally.
  let (w, _cw) = counting_waker();
  assert!(poll_once(&mut fut, &w).is_pending());
  // Free a slot; the send completes and delivers the original message.
  assert_eq!(rx.try_recv(), Ok(0));
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn recv_keeps_polling_across_a_panicking_waker_clone() {
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  // A waker whose CLONE panics, exercised on the recv park path. The recv future must
  // survive and still deliver a later item.
  unsafe fn vt_clone(_: *const ()) -> RawWaker {
    panic!("waker clone panics");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_noop);

  let (tx, rx) = bounded::<u32>(1);
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };

  let mut fut = rx.recv();
  // Empty → the recv parks, registering its waker — whose clone panics.
  assert!(catch_unwind(AssertUnwindSafe(|| {
    let _ = Pin::new(&mut fut).poll(&mut Context::from_waker(&panicking));
  }))
  .is_err());
  // Re-poll with a normal waker: the future survived; it parks normally and a later
  // send delivers the item.
  let (w, _cw) = counting_waker();
  assert!(poll_once(&mut fut, &w).is_pending());
  tx.try_send(5).unwrap();
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Some(5)));
}

#[test]
fn wake_senders_wakes_the_rest_when_one_waker_panics() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("sender waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let chan = Chan::<u32>::bounded(1);
  chan.try_push(0).unwrap(); // full
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let normal = waker(cw.clone());
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  chan.add_send_waker(&normal);
  chan.add_send_waker(&panicking); // added last → woken first
                                   // The panicking waker (woken first) panics, but the normal one must still be woken.
  let _ = catch_unwind(AssertUnwindSafe(|| chan.wake_senders()));
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
}

#[test]
fn wake_receivers_wakes_the_rest_when_one_waker_panics() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};
  use std::panic::{catch_unwind, AssertUnwindSafe};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(_: *const ()) {
    panic!("recv waker panics on wake");
  }
  unsafe fn vt_noop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_noop);

  let chan = Chan::<u32>::unbounded();
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let normal = waker(cw.clone());
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  chan.add_recv_waker(&normal);
  chan.add_recv_waker(&panicking); // added last → woken first
                                   // The panicking waker (woken first) panics, but the normal one must still be woken.
  let _ = catch_unwind(AssertUnwindSafe(|| chan.wake_receivers()));
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
}

#[test]
fn recv_delivers_rechecked_item_despite_a_panicking_recv_waker_drop() {
  use super::chan::Chan;
  use core::task::{Context, RawWaker, RawWakerVTable, Waker};

  // A waker whose CLONE pushes an item (so the recheck finds one) and whose DROP
  // panics — but whose WAKE is a no-op, so teardown can consume it without dropping.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    let _ = chan.try_push(99); // Ignoring Err: empty cap-2 channel, the push succeeds
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(_: *const ()) {
    panic!("recv waker drop panics");
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let chan = Chan::<u32>::bounded(2);
  let _tx = Sender::new(chan.clone());
  let rx = Receiver::new(chan.clone());
  let waker = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };

  // pop None → register (the clone pushes 99) → recheck pop 99. The recheck does not
  // remove the recv waker inline, so the panicking-drop waker is not dropped here and
  // the item is delivered.
  let mut fut = rx.recv();
  assert!(matches!(
    Pin::new(&mut fut).poll(&mut Context::from_waker(&waker)),
    Poll::Ready(Some(99))
  ));
  // The future kept the rechecked waker's id; consume the leftover clone by waking (a
  // no-op) rather than dropping it, then forget the future so its `Drop` does not run
  // the panicking remove.
  chan.wake_receivers();
  core::mem::forget(fut);
  waker.wake();
}

#[test]
fn recv_drop_clears_a_stale_recheck_recv_waker() {
  use super::chan::Chan;
  use core::{
    ptr::addr_of,
    task::{Context, RawWaker, RawWakerVTable, Waker},
  };

  struct WakerCtx {
    chan: *const Chan<u32>,
    drops: Cell<usize>,
  }
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let ctx = unsafe { &*(p as *const WakerCtx) };
    let _ = unsafe { (*ctx.chan).try_push(99) }; // Ignoring Err: empty cap-2 channel
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(p: *const ()) {
    let ctx = unsafe { &*(p as *const WakerCtx) };
    ctx.drops.set(ctx.drops.get() + 1);
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let chan = Chan::<u32>::bounded(2);
  let tx = Sender::new(chan.clone()); // keep a sender alive → Chan outlives the receiver
  let ctx = WakerCtx {
    chan: Rc::as_ptr(&chan),
    drops: Cell::new(0),
  };
  let waker = unsafe { Waker::from_raw(RawWaker::new(addr_of!(ctx) as *const (), &VT)) };

  {
    let rx = Receiver::new(chan.clone());
    let mut fut = rx.recv();
    // recheck-Ready: the clone pushes 99 → Ready(99), leaving its clone registered.
    assert!(matches!(
      Pin::new(&mut fut).poll(&mut Context::from_waker(&waker)),
      Poll::Ready(Some(99))
    ));
    drop(fut); // Recv::drop removes the stale recheck waker
    drop(rx);
  }
  assert_eq!(ctx.drops.get(), 1); // the registered clone was released
  drop(tx);
  waker.wake(); // consume our handle without a (counted) drop
}

#[test]
fn try_send_error_methods() {
  let full = TrySendError::Full(7u32);
  assert!(full.is_full());
  assert!(!full.is_closed());
  assert_eq!(format!("{full}"), "sending on a full channel");
  assert_eq!(format!("{full:?}"), "Full(..)");
  assert_eq!(full.into_inner(), 7);

  let closed = TrySendError::Closed(9u32);
  assert!(closed.is_closed());
  assert!(!closed.is_full());
  assert_eq!(format!("{closed}"), "sending on a closed channel");
  assert_eq!(format!("{closed:?}"), "Closed(..)");
  assert_eq!(closed.into_inner(), 9);
}

#[test]
fn try_recv_error_methods() {
  let empty = TryRecvError::Empty;
  assert!(empty.is_empty());
  assert!(!empty.is_disconnected());
  assert_eq!(empty.as_str(), "receiving on an empty channel");
  assert_eq!(format!("{empty}"), empty.as_str());

  let disconnected = TryRecvError::Disconnected;
  assert!(disconnected.is_disconnected());
  assert!(!disconnected.is_empty());
  assert_eq!(
    disconnected.as_str(),
    "receiving on an empty and disconnected channel"
  );
  assert_eq!(format!("{disconnected}"), disconnected.as_str());
}

#[test]
fn send_error_methods() {
  let (tx, rx) = bounded::<u32>(1);
  drop(rx);
  let (w, _cw) = counting_waker();
  let mut fut = tx.send(5);
  match poll_once(&mut fut, &w) {
    Poll::Ready(Err(e)) => {
      assert_eq!(format!("{e}"), "sending on a closed channel");
      assert_eq!(format!("{e:?}"), "SendError(..)");
      assert_eq!(e.into_inner(), 5);
    }
    other => panic!("expected a SendError, got {other:?}"),
  }
}

#[test]
fn accessors_cover_both_flavors() {
  // Unbounded: Sender + Receiver len / is_empty (the unbounded `Flavor` arms).
  let (tx, rx) = unbounded::<u32>();
  assert!(tx.is_empty() && rx.is_empty());
  assert_eq!(tx.len(), 0);
  assert_eq!(rx.len(), 0);
  tx.try_send(1).unwrap();
  assert_eq!(tx.len(), 1);
  assert_eq!(rx.len(), 1);
  assert!(!tx.is_empty() && !rx.is_empty());
  assert_eq!(rx.try_recv(), Ok(1));

  // Bounded: the Receiver's len / is_empty.
  let (tx, rx) = bounded::<u32>(2);
  assert!(rx.is_empty());
  assert_eq!(rx.len(), 0);
  tx.try_send(1).unwrap();
  assert_eq!(rx.len(), 1);
  assert!(!rx.is_empty());
  assert_eq!(rx.try_recv(), Ok(1));
}

#[test]
fn last_receiver_drop_drains_queued_items() {
  let drops = Rc::new(Cell::new(0));
  #[derive(Debug)]
  struct D(Rc<Cell<usize>>);
  impl Drop for D {
    fn drop(&mut self) {
      self.0.set(self.0.get() + 1);
    }
  }
  let (tx, rx) = unbounded::<D>();
  let rx2 = rx.clone();
  for _ in 0..3 {
    tx.try_send(D(drops.clone())).unwrap();
  }
  assert_eq!(drops.get(), 0);
  drop(rx); // not the last receiver → the queue is NOT drained
  assert_eq!(drops.get(), 0);
  drop(rx2); // last receiver → drains the queued items
  assert_eq!(drops.get(), 3);
}

#[test]
#[should_panic(expected = "non-zero capacity")]
fn bounded_zero_capacity_panics() {
  let _ = bounded::<u32>(0);
}

#[test]
fn stream_collects_queued_items_until_disconnect() {
  use futures::{executor::block_on, StreamExt};
  let (tx, rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  tx.try_send(3).unwrap();
  drop(tx); // disconnect → the stream ends once drained
  assert_eq!(block_on(rx.collect::<Vec<_>>()), vec![1, 2, 3]);
}

#[test]
fn stream_poll_next_parks_then_wakes_on_send() {
  use futures_core::Stream;
  let (tx, mut rx) = unbounded::<u32>();
  let (w, cw) = counting_waker();
  assert!(Pin::new(&mut rx)
    .poll_next(&mut Context::from_waker(&w))
    .is_pending());
  assert_eq!(cw.0.load(Ordering::SeqCst), 0);
  tx.try_send(9).unwrap();
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  assert_eq!(
    Pin::new(&mut rx).poll_next(&mut Context::from_waker(&w)),
    Poll::Ready(Some(9))
  );
}

#[test]
fn stream_re_poll_replaces_its_registration() {
  use futures_core::Stream;
  // A Receiver polled as a stream repeatedly while empty must replace its own
  // registration each time, so a single send wakes it exactly once.
  let (tx, mut rx) = unbounded::<u32>();
  let (w, cw) = counting_waker();
  assert!(Pin::new(&mut rx)
    .poll_next(&mut Context::from_waker(&w))
    .is_pending());
  assert!(Pin::new(&mut rx)
    .poll_next(&mut Context::from_waker(&w))
    .is_pending());
  tx.try_send(1).unwrap();
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  assert_eq!(
    Pin::new(&mut rx).poll_next(&mut Context::from_waker(&w)),
    Poll::Ready(Some(1))
  );
}

#[test]
fn dropping_a_streamed_receiver_clears_its_registration() {
  use futures_core::Stream;
  let (tx, rx) = unbounded::<u32>();
  let cw = Arc::new(CountingWaker(AtomicUsize::new(0)));
  let w = waker(cw.clone());
  {
    let mut rx = rx;
    assert!(Pin::new(&mut rx)
      .poll_next(&mut Context::from_waker(&w))
      .is_pending()); // parks as a stream → registers a clone
    assert_eq!(Arc::strong_count(&cw), 3); // cw + w + the registered clone
                                           // rx drops here → clears its stream registration
  }
  assert_eq!(Arc::strong_count(&cw), 2); // back to cw + w
  let _ = tx;
}

#[test]
fn fused_stream_terminates_once_drained_and_disconnected() {
  use futures_core::stream::FusedStream;
  let (tx, rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  drop(tx);
  assert!(!rx.is_terminated()); // an item is still queued
  assert_eq!(rx.try_recv(), Ok(1));
  assert!(rx.is_terminated()); // drained and every sender gone
}

#[test]
fn try_iter_drains_ready_items_without_blocking() {
  let (tx, rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  let drained: Vec<u32> = rx.try_iter().collect();
  assert_eq!(drained, vec![1, 2]);
  // try_iter stops at Empty rather than waiting; the receiver stays usable.
  assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
  tx.try_send(3).unwrap();
  assert_eq!(rx.try_recv(), Ok(3));
}
