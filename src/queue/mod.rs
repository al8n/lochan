//! The shared FIFO storage backing every channel flavor.
//!
//! A [`Flavor`] is either a fixed [`Ring`](ring::Ring) (bounded) or a segmented
//! [`BlockList`](list::BlockList) (unbounded). The queue holds only the items — no
//! wakers, no handle counts — so the `mpsc` and `mpmc` channel cores share it
//! verbatim rather than each carrying their own copy of the unsafe ring/block code.

mod list;
mod ring;

use list::BlockList;
use ring::Ring;

/// The storage backing a channel: a fixed ring (bounded) or a segmented block-list
/// (unbounded). Dispatch is a single match per operation.
pub(crate) enum Flavor<T> {
  Bounded(Ring<T>),
  Unbounded(BlockList<T>),
}

impl<T> Flavor<T> {
  /// A bounded flavor holding at most `cap` items.
  pub(crate) fn bounded(cap: usize) -> Self {
    Self::Bounded(Ring::with_capacity(cap))
  }

  /// An unbounded flavor that grows a block at a time.
  pub(crate) fn unbounded() -> Self {
    Self::Unbounded(BlockList::new())
  }

  pub(crate) fn len(&self) -> usize {
    match self {
      Self::Bounded(r) => r.len(),
      Self::Unbounded(l) => l.len(),
    }
  }

  pub(crate) fn is_empty(&self) -> bool {
    match self {
      Self::Bounded(r) => r.is_empty(),
      Self::Unbounded(l) => l.is_empty(),
    }
  }

  /// The capacity, or `None` when unbounded.
  pub(crate) fn cap(&self) -> Option<usize> {
    match self {
      Self::Bounded(r) => Some(r.cap()),
      Self::Unbounded(_) => None,
    }
  }

  pub(crate) fn is_full(&self) -> bool {
    match self {
      Self::Bounded(r) => r.is_full(),
      Self::Unbounded(_) => false,
    }
  }

  /// Pushes, or hands the item back via `Err` when a bounded channel is full.
  /// Unbounded never fails.
  #[inline(always)]
  pub(crate) fn try_push(&mut self, item: T) -> Result<(), T> {
    match self {
      Self::Bounded(r) => r.push(item),
      Self::Unbounded(l) => {
        l.push(item);
        Ok(())
      }
    }
  }

  #[inline(always)]
  pub(crate) fn pop(&mut self) -> Option<T> {
    match self {
      Self::Bounded(r) => r.pop(),
      Self::Unbounded(l) => l.pop(),
    }
  }
}
