//! A single-threaded interior-mutability cell.
//!
//! Checked with [`RefCell`](core::cell::RefCell) under `debug_assertions`, and an
//! unchecked [`UnsafeCell`](core::cell::UnsafeCell) in release. `lochan` is `!Send`
//! and upholds, by construction, that no borrow of a given cell ever overlaps another
//! — no `Waker` vtable callback or user `Drop` runs while a cell is borrowed. The
//! debug build verifies that invariant dynamically (and Miri checks both builds), so
//! release can drop the borrow flag.

#[cfg(debug_assertions)]
pub(crate) use checked::LocalCell;
#[cfg(not(debug_assertions))]
pub(crate) use unchecked::LocalCell;

#[cfg(debug_assertions)]
mod checked {
  use core::cell::{Ref, RefCell, RefMut};

  pub(crate) struct LocalCell<T>(RefCell<T>);

  impl<T> LocalCell<T> {
    pub(crate) const fn new(value: T) -> Self {
      Self(RefCell::new(value))
    }

    pub(crate) fn borrow(&self) -> Ref<'_, T> {
      self.0.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> RefMut<'_, T> {
      self.0.borrow_mut()
    }
  }
}

#[cfg(not(debug_assertions))]
mod unchecked {
  use core::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
  };

  pub(crate) struct LocalCell<T>(UnsafeCell<T>);

  impl<T> LocalCell<T> {
    pub(crate) const fn new(value: T) -> Self {
      Self(UnsafeCell::new(value))
    }

    #[inline(always)]
    pub(crate) fn borrow(&self) -> LocalRef<'_, T> {
      // SAFETY: `!Send` single-threaded use with no overlapping borrow of this cell —
      // no vtable callback or user `Drop` runs while a borrow is live.
      LocalRef(unsafe { &*self.0.get() })
    }

    #[allow(clippy::mut_from_ref)]
    // deliberate interior mutability; soundness is the
    // no-overlap invariant, dynamically checked in debug and by Miri in both builds.
    #[inline(always)]
    pub(crate) fn borrow_mut(&self) -> LocalRefMut<'_, T> {
      // SAFETY: as above — this borrow does not overlap any other borrow of the cell.
      LocalRefMut(unsafe { &mut *self.0.get() })
    }
  }

  pub(crate) struct LocalRef<'a, T>(&'a T);
  pub(crate) struct LocalRefMut<'a, T>(&'a mut T);

  impl<T> Deref for LocalRef<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
      self.0
    }
  }

  impl<T> Deref for LocalRefMut<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
      self.0
    }
  }

  impl<T> DerefMut for LocalRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
      self.0
    }
  }
}
