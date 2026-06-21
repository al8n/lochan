use super::*;

use core::cell::Cell;
use std::rc::Rc;

#[test]
fn push_pop_within_one_block() {
  let mut list = BlockList::<u32, 4>::new();
  list.push(1);
  list.push(2);
  assert_eq!(list.pop(), Some(1));
  assert_eq!(list.pop(), Some(2));
  assert_eq!(list.pop(), None);
}

#[test]
fn push_pop_across_block_boundaries() {
  let mut list = BlockList::<u32, 2>::new();
  for i in 0..5 {
    list.push(i); // N = 2 → spills into a third block
  }
  for i in 0..5 {
    assert_eq!(list.pop(), Some(i));
  }
  assert_eq!(list.pop(), None);
}

#[test]
fn interleaved_push_pop_reclaims_blocks() {
  let mut list = BlockList::<u32, 2>::new();
  list.push(1);
  list.push(2);
  list.push(3); // crosses into a second block
  assert_eq!(list.pop(), Some(1));
  list.push(4);
  assert_eq!(list.pop(), Some(2)); // drains + frees the first block
  assert_eq!(list.pop(), Some(3));
  assert_eq!(list.pop(), Some(4));
  assert_eq!(list.pop(), None);
}

#[test]
fn len_tracks_size() {
  let mut list = BlockList::<u32, 2>::new();
  assert!(list.is_empty());
  list.push(1);
  list.push(2);
  list.push(3);
  assert_eq!(list.len(), 3);
  list.pop();
  assert_eq!(list.len(), 2);
}

#[derive(Debug)]
struct DropCounter(Rc<Cell<usize>>);

impl Drop for DropCounter {
  fn drop(&mut self) {
    self.0.set(self.0.get() + 1);
  }
}

#[test]
fn drop_releases_queued_items_across_blocks() {
  let count = Rc::new(Cell::new(0));
  {
    let mut list = BlockList::<DropCounter, 2>::new();
    for _ in 0..5 {
      list.push(DropCounter(count.clone())); // 3 blocks, 5 items
    }
    list.pop(); // reads one out and drops it
    assert_eq!(count.get(), 1);
    // `list` drops here, releasing the remaining 4 across the chain.
  }
  assert_eq!(count.get(), 5);
}
