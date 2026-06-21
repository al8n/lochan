//! Fixed-capacity ring buffer backing the bounded flavor.

use core::mem::MaybeUninit;
use std::{boxed::Box, vec::Vec};

/// A fixed-capacity ring of `MaybeUninit` slots: exactly `cap` slots in one
/// allocation, with `head`/`len` marking the initialized region
/// `[head, head + len) mod cap`. Slots outside that region are uninitialized and
/// are never read.
pub(super) struct Ring<T> {
  slots: Box<[MaybeUninit<T>]>,
  head: usize,
  len: usize,
}

impl<T> Ring<T> {
  pub(super) fn with_capacity(cap: usize) -> Self {
    debug_assert!(cap > 0, "a bounded ring needs a non-zero capacity");
    let mut slots = Vec::with_capacity(cap);
    slots.resize_with(cap, MaybeUninit::uninit);
    Self {
      slots: slots.into_boxed_slice(),
      head: 0,
      len: 0,
    }
  }

  pub(super) fn cap(&self) -> usize {
    self.slots.len()
  }

  pub(super) fn len(&self) -> usize {
    self.len
  }

  pub(super) fn is_empty(&self) -> bool {
    self.len == 0
  }

  pub(super) fn is_full(&self) -> bool {
    self.len == self.cap()
  }

  /// Appends an item, or hands it back via `Err` when the ring is full.
  #[inline(always)]
  pub(super) fn push(&mut self, item: T) -> Result<(), T> {
    if self.is_full() {
      return Err(item);
    }
    let tail = (self.head + self.len) % self.cap();
    // `MaybeUninit::write` initializes the slot without reading the old (uninit)
    // contents, so this needs no `unsafe`. The slot is outside `[head, head+len)`
    // and so was uninitialized.
    self.slots[tail].write(item);
    self.len += 1;
    Ok(())
  }

  /// Removes and returns the oldest item, or `None` when empty.
  #[inline(always)]
  pub(super) fn pop(&mut self) -> Option<T> {
    if self.len == 0 {
      return None;
    }
    // SAFETY: `len > 0`, so `head` is inside the initialized region and holds a `T`
    // written by a prior `push`. We read it out exactly once and immediately shrink
    // the region past it, so the slot is never read again while initialized.
    let item = unsafe { self.slots[self.head].assume_init_read() };
    self.head = (self.head + 1) % self.cap();
    self.len -= 1;
    Some(item)
  }

  /// Drops every queued item, leaving the ring empty.
  pub(super) fn clear(&mut self) {
    while self.pop().is_some() {}
  }
}

impl<T> Drop for Ring<T> {
  fn drop(&mut self) {
    // Drop the still-initialized items; uninitialized slots hold no `T` to drop.
    self.clear();
  }
}

#[cfg(all(test, feature = "std"))]
mod tests;
