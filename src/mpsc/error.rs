//! `mpsc` error types.

use core::fmt;

/// Error returned by [`Sender::try_send`](super::Sender::try_send) (and
/// [`UnboundedSender::send`](super::UnboundedSender::send)) when an item cannot be
/// delivered. The unsent item is carried back.
pub enum TrySendError<T> {
  /// The bounded channel is at capacity.
  Full(T),
  /// Every receiver is gone.
  Closed(T),
}

impl<T> TrySendError<T> {
  /// Consumes the error, returning the item that could not be sent.
  pub fn into_inner(self) -> T {
    match self {
      Self::Full(item) | Self::Closed(item) => item,
    }
  }

  /// Returns `true` if the channel was full.
  pub const fn is_full(&self) -> bool {
    matches!(self, Self::Full(_))
  }

  /// Returns `true` if the channel was closed.
  pub const fn is_closed(&self) -> bool {
    matches!(self, Self::Closed(_))
  }
}

// Hand-written so the payload `T` is never required to be `Debug`, and is never shown.
impl<T> fmt::Debug for TrySendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      Self::Full(_) => "Full(..)",
      Self::Closed(_) => "Closed(..)",
    })
  }
}

impl<T> fmt::Display for TrySendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      Self::Full(_) => "sending on a full channel",
      Self::Closed(_) => "sending on a closed channel",
    })
  }
}

impl<T> core::error::Error for TrySendError<T> {}

/// Error returned by [`Receiver::try_recv`](super::Receiver::try_recv) when no item
/// is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
  /// The channel is empty, but senders remain.
  Empty,
  /// The channel is empty and every sender is gone.
  Disconnected,
}

impl TryRecvError {
  /// Returns the static string form of this error.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Empty => "receiving on an empty channel",
      Self::Disconnected => "receiving on an empty and disconnected channel",
    }
  }

  /// Returns `true` if the channel was merely empty (senders remain).
  pub const fn is_empty(&self) -> bool {
    matches!(self, Self::Empty)
  }

  /// Returns `true` if the channel was empty and every sender had dropped.
  pub const fn is_disconnected(&self) -> bool {
    matches!(self, Self::Disconnected)
  }
}

impl fmt::Display for TryRecvError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl core::error::Error for TryRecvError {}
