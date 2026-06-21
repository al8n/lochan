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

#[test]
fn dropping_a_parked_send_unregisters_its_waker() {
  let (tx, mut rx) = bounded::<u32>(1);
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
  let (tx, mut rx) = bounded::<u32>(1);
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
fn receiver_drop_wakes_sender_before_panicking_payload_drop() {
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
  // Dropping the receiver wakes the parked sender BEFORE draining (the queued
  // payload's Drop panics); the sender must still have been woken.
  let _ = catch_unwind(AssertUnwindSafe(|| drop(rx)));
  assert_eq!(cw.0.load(Ordering::SeqCst), 1);
  let _ = poll_once(&mut fut, &w); // resolves to Err; MaybePanic(2) drops cleanly
}

#[test]
fn completed_send_releases_its_waker() {
  let (tx, mut rx) = bounded::<u32>(1);
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
fn wake_receiver_releases_borrow_before_waking() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  // A raw waker that, on wake, re-enters the channel by registering a recv waker —
  // which double-borrows recv_waker if wake() runs while that borrow is still held.
  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_wake(p: *const ()) {
    vt_wake_ref(p)
  }
  unsafe fn vt_wake_ref(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.register_recv_waker(&futures::task::noop_waker());
  }
  unsafe fn vt_drop(_: *const ()) {}
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake_ref, vt_drop);

  let chan = Chan::<u32>::bounded(1);
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  chan.register_recv_waker(&reentrant);
  // wake_receiver must drop the recv_waker borrow before calling wake(), or the
  // re-entry above double-borrow-panics.
  chan.wake_receiver();
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
fn recv_waker_registration_runs_vtable_outside_borrow() {
  use super::chan::Chan;
  use core::task::{RawWaker, RawWakerVTable, Waker};

  unsafe fn vt_clone(p: *const ()) -> RawWaker {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.register_recv_waker(&futures::task::noop_waker());
    RawWaker::new(p, &VT)
  }
  unsafe fn vt_noop(_: *const ()) {}
  unsafe fn vt_drop(p: *const ()) {
    let chan = unsafe { &*(p as *const Chan<u32>) };
    chan.register_recv_waker(&futures::task::noop_waker());
  }
  static VT: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_noop, vt_noop, vt_drop);

  let chan = Chan::<u32>::bounded(1);
  let reentrant = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };
  chan.register_recv_waker(&reentrant); // clone re-enters register
  chan.register_recv_waker(&futures::task::noop_waker()); // replace drops the clone → re-enters
}

#[test]
fn dropping_a_pending_recv_clears_its_waker() {
  let (tx, mut rx) = bounded::<u32>(1);
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
  // Dropping the receiver drains the queue; each payload's Drop re-borrows the channel
  // via the live sender, which overlaps the flavor borrow if drain dropped items while
  // holding it (RefCell panic in debug, UB under Miri release).
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
    chan.clear_receiver();
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
fn receiver_drop_drains_even_if_a_sender_waker_panics() {
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
  // Dropping the receiver wakes the parked sender (which panics); the drain must still
  // run, breaking the cycle and freeing the queued payload.
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
  // Dropping the receiver drains: the first payload's Drop panics, but the drain must
  // continue and free the later (Sender-owning) payload, breaking the Rc cycle.
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

  let (tx, mut rx) = bounded::<u32>(2);
  // Register a panicking recv waker directly, so the send's wake_receiver panics.
  let panicking = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VT)) };
  tx.chan().register_recv_waker(&panicking);

  let (w, _cw) = counting_waker();
  let mut fut = tx.send(7);
  // The send pushes the item (commits Ok), then wake_receiver panics.
  assert!(catch_unwind(AssertUnwindSafe(|| poll_once(&mut fut, &w))).is_err());
  // Re-poll: the committed Ok is replayed (not a hang or an expect-panic).
  assert!(matches!(poll_once(&mut fut, &w), Poll::Ready(Ok(()))));
  // The item was delivered despite the wake panic.
  assert_eq!(rx.try_recv(), Ok(7));
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

  let (tx, mut rx) = bounded::<u32>(1);
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
  let mut rx = Receiver::new(chan.clone());
  let waker = unsafe { Waker::from_raw(RawWaker::new(Rc::as_ptr(&chan) as *const (), &VT)) };

  // pop None → register (the clone pushes 99) → recheck pop 99. The recheck no longer
  // clears the recv waker inline, so the panicking-drop waker is not dropped here and
  // the item is delivered.
  let mut fut = rx.recv();
  assert!(matches!(
    Pin::new(&mut fut).poll(&mut Context::from_waker(&waker)),
    Poll::Ready(Some(99))
  ));
  // Consume the leftover waker clones by waking (a no-op) rather than dropping them.
  chan.wake_receiver();
  waker.wake();
}

#[test]
fn receiver_drop_clears_a_stale_recheck_recv_waker() {
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
    let mut rx = Receiver::new(chan.clone());
    let mut fut = rx.recv();
    // recheck-Ready: the clone pushes 99 → Ready(99), leaving its clone registered.
    assert!(matches!(
      Pin::new(&mut fut).poll(&mut Context::from_waker(&waker)),
      Poll::Ready(Some(99))
    ));
    drop(fut);
    drop(rx); // Receiver::drop must clear the stale recv waker
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
  let (tx, mut rx) = unbounded::<u32>();
  assert!(tx.is_empty() && rx.is_empty());
  assert_eq!(tx.len(), 0);
  assert_eq!(rx.len(), 0);
  tx.try_send(1).unwrap();
  assert_eq!(tx.len(), 1);
  assert_eq!(rx.len(), 1);
  assert!(!tx.is_empty() && !rx.is_empty());
  assert_eq!(rx.try_recv(), Ok(1));

  // Bounded: the Receiver's len / is_empty.
  let (tx, mut rx) = bounded::<u32>(2);
  assert!(rx.is_empty());
  assert_eq!(rx.len(), 0);
  tx.try_send(1).unwrap();
  assert_eq!(rx.len(), 1);
  assert!(!rx.is_empty());
  assert_eq!(rx.try_recv(), Ok(1));
}

#[test]
fn receiver_drop_drains_queued_items() {
  let drops = Rc::new(Cell::new(0));
  #[derive(Debug)]
  struct D(Rc<Cell<usize>>);
  impl Drop for D {
    fn drop(&mut self) {
      self.0.set(self.0.get() + 1);
    }
  }
  let (tx, rx) = unbounded::<D>();
  for _ in 0..3 {
    tx.try_send(D(drops.clone())).unwrap();
  }
  assert_eq!(drops.get(), 0);
  drop(rx); // Receiver::drop drains the queued items
  assert_eq!(drops.get(), 3);
}

#[test]
#[should_panic(expected = "non-zero capacity")]
fn bounded_zero_capacity_panics() {
  let _ = bounded::<u32>(0);
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

  let (tx, mut rx) = unbounded::<u32>();
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
fn fused_stream_terminates_once_drained_and_disconnected() {
  use futures_core::stream::FusedStream;
  let (tx, mut rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  drop(tx);
  assert!(!rx.is_terminated()); // an item is still queued
  assert_eq!(rx.try_recv(), Ok(1));
  assert!(rx.is_terminated()); // drained and every sender gone
}

#[test]
fn try_iter_drains_ready_items_without_blocking() {
  let (tx, mut rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  let drained: Vec<u32> = rx.try_iter().collect();
  assert_eq!(drained, vec![1, 2]);
  // try_iter stops at Empty rather than waiting; the receiver stays usable.
  assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
  tx.try_send(3).unwrap();
  assert_eq!(rx.try_recv(), Ok(3));
}
