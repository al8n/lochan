#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

#[cfg(not(feature = "std"))]
extern crate alloc as std;

#[cfg(all(feature = "std", not(feature = "alloc")))]
extern crate std;

mod cell;
mod queue;

pub mod mpmc;
pub mod mpsc;
pub mod oneshot;

/// Disposes of a caught panic payload that will not be resumed. Dropping it inside
/// `catch_unwind` frees it (no leak) while swallowing a `Drop` panic from a toxic
/// `panic_any` payload, so a payload whose own `Drop` panics cannot double-panic into a
/// process abort while a wake path is already unwinding from another panic.
#[cfg(feature = "std")]
pub(crate) fn drop_panic_payload(payload: std::boxed::Box<dyn core::any::Any + Send>) {
  let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(payload)));
}
