use super::*;

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
