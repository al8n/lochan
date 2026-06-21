//! Segmented (block) queue backing the unbounded flavor.
//!
//! A singly-linked list of fixed-size blocks. The producer writes into the tail
//! block, linking a fresh block when it fills; the consumer reads from the head
//! block, freeing it once drained. Items never move after they are written, and
//! drained blocks are returned to the allocator — so there is no reallocation and
//! memory tracks live usage. Single-threaded, so the links are plain `Cell` /
//! `NonNull`, never atomics.

use core::{
  cell::{Cell, UnsafeCell},
  marker::PhantomData,
  mem::MaybeUninit,
  ptr::{addr_of_mut, NonNull},
};
use std::boxed::Box;

/// One block: a fixed array of `N` slots plus the link to the next block. Slots
/// `[begin, end)` are initialized; `[0, begin)` were read out, `[end, N)` are
/// untouched. Every field is interior-mutable so the block can be driven through a
/// shared `&Block` reached from a raw pointer.
struct Block<T, const N: usize> {
  next: Cell<Option<NonNull<Block<T, N>>>>,
  values: [UnsafeCell<MaybeUninit<T>>; N],
  begin: Cell<usize>,
  end: Cell<usize>,
}

impl<T, const N: usize> Block<T, N> {
  fn alloc() -> NonNull<Self> {
    // Allocate the block uninitialized and write only the header in place — the
    // value slots are `MaybeUninit` and need no construction. This avoids building
    // the whole `[…; N]` array as a stack temporary (which `Box::new(Self { … })`
    // would, then copy to the heap).
    let mut block = Box::<Self>::new_uninit();
    let ptr = block.as_mut_ptr();
    // SAFETY: `ptr` is freshly allocated and aligned. We initialize every header
    // field; the `values` slots stay uninitialized but valid (they are
    // `MaybeUninit`), so the block is fully valid for `assume_init`.
    unsafe {
      addr_of_mut!((*ptr).next).write(Cell::new(None));
      addr_of_mut!((*ptr).begin).write(Cell::new(0));
      addr_of_mut!((*ptr).end).write(Cell::new(0));
      NonNull::new_unchecked(Box::into_raw(block.assume_init()))
    }
  }
}

impl<T, const N: usize> Drop for Block<T, N> {
  fn drop(&mut self) {
    let begin = self.begin.get();
    let end = self.end.get();
    for i in begin..end {
      // SAFETY: slot `i` in `[begin, end)` holds an initialized, unread item.
      unsafe { (*self.values[i].get()).assume_init_drop() };
    }
  }
}

/// An unbounded FIFO over a chain of [`Block`]s. Owns its blocks via raw pointers;
/// the `PhantomData<T>` records that ownership for drop-check.
pub(super) struct BlockList<T, const N: usize = 32> {
  head: NonNull<Block<T, N>>,
  tail: NonNull<Block<T, N>>,
  len: usize,
  _marker: PhantomData<T>,
}

impl<T, const N: usize> BlockList<T, N> {
  pub(super) fn new() -> Self {
    let first = Block::<T, N>::alloc();
    Self {
      head: first,
      tail: first,
      len: 0,
      _marker: PhantomData,
    }
  }

  pub(super) fn len(&self) -> usize {
    self.len
  }

  pub(super) fn is_empty(&self) -> bool {
    self.len == 0
  }

  pub(super) fn push(&mut self, item: T) {
    // SAFETY: `tail` always points to a live block this list owns.
    let tail = unsafe { self.tail.as_ref() };
    let end = tail.end.get();
    if end < N {
      // SAFETY: slot `end` (< N) is currently uninitialized; initialize it once.
      unsafe { (*tail.values[end].get()).write(item) };
      tail.end.set(end + 1);
    } else {
      // Tail block full: link a fresh block and start it with this item.
      let new = Block::<T, N>::alloc();
      // SAFETY: `new` is freshly allocated and distinct from `tail`; slot 0 is
      // uninitialized.
      let new_block = unsafe { new.as_ref() };
      unsafe { (*new_block.values[0].get()).write(item) };
      new_block.end.set(1);
      tail.next.set(Some(new));
      self.tail = new;
    }
    self.len += 1;
  }

  pub(super) fn pop(&mut self) -> Option<T> {
    loop {
      // SAFETY: `head` always points to a live block this list owns.
      let head = unsafe { self.head.as_ref() };
      let begin = head.begin.get();
      let end = head.end.get();
      if begin < end {
        // SAFETY: slot `begin` is within the initialized region `[begin, end)`;
        // read it out exactly once and advance past it.
        let item = unsafe { (*head.values[begin].get()).assume_init_read() };
        head.begin.set(begin + 1);
        self.len -= 1;
        return Some(item);
      }
      // The head block is fully consumed.
      if self.head == self.tail {
        return None;
      }
      let next = head.next.get().expect("a non-tail block always has a next");
      // The shared `head` borrow is dead past here; reclaim the drained block.
      let old = self.head;
      self.head = next;
      // SAFETY: `old` came from `Block::alloc`, is fully drained (no live items),
      // and is no longer referenced by the list.
      unsafe { drop(Box::from_raw(old.as_ptr())) };
    }
  }
}

impl<T, const N: usize> Drop for BlockList<T, N> {
  fn drop(&mut self) {
    let mut cur = Some(self.head);
    while let Some(ptr) = cur {
      // SAFETY: every linked pointer came from `Block::alloc`; take ownership back.
      // `Block::drop` releases the block's unread items; we just walk + free.
      let block = unsafe { Box::from_raw(ptr.as_ptr()) };
      cur = block.next.get();
    }
  }
}

#[cfg(all(test, feature = "std"))]
mod tests;
