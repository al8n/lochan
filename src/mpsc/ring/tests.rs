use super::*;

use alloc::rc::Rc;
use core::cell::Cell;

#[test]
fn push_pop_is_fifo() {
  let mut ring = Ring::with_capacity(3);
  ring.push(1).unwrap();
  ring.push(2).unwrap();
  assert_eq!(ring.pop(), Some(1));
  assert_eq!(ring.pop(), Some(2));
  assert_eq!(ring.pop(), None);
}

#[test]
fn push_reports_full_at_capacity() {
  let mut ring = Ring::with_capacity(2);
  ring.push(1).unwrap();
  ring.push(2).unwrap();
  assert_eq!(ring.push(3), Err(3));
}

#[test]
fn wraps_around_the_buffer() {
  let mut ring = Ring::with_capacity(2);
  ring.push(1).unwrap();
  ring.push(2).unwrap();
  assert_eq!(ring.pop(), Some(1)); // head advances to slot 1
  ring.push(3).unwrap(); // tail wraps to slot 0
  assert_eq!(ring.pop(), Some(2));
  assert_eq!(ring.pop(), Some(3));
  assert_eq!(ring.pop(), None);
}

#[derive(Debug)]
struct DropCounter(Rc<Cell<usize>>);

impl Drop for DropCounter {
  fn drop(&mut self) {
    self.0.set(self.0.get() + 1);
  }
}

#[test]
fn drop_releases_queued_items() {
  let count = Rc::new(Cell::new(0));
  {
    let mut ring = Ring::with_capacity(4);
    ring.push(DropCounter(count.clone())).unwrap();
    ring.push(DropCounter(count.clone())).unwrap();
    assert_eq!(count.get(), 0);
  }
  // The ring's Drop must drop both still-queued items exactly once.
  assert_eq!(count.get(), 2);
}

#[test]
fn pop_then_drop_does_not_double_drop() {
  let count = Rc::new(Cell::new(0));
  let mut ring = Ring::with_capacity(4);
  ring.push(DropCounter(count.clone())).unwrap();
  ring.push(DropCounter(count.clone())).unwrap();
  drop(ring.pop()); // drops one
  assert_eq!(count.get(), 1);
  drop(ring); // drops the remaining one — not the already-popped slot
  assert_eq!(count.get(), 2);
}
