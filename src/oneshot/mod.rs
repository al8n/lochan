//! One-shot channel: a single value sent once from the producer to the consumer.
//!
//! `!Send`, no-atomics. The [`Sender`] delivers one value synchronously (consuming
//! itself); the [`Receiver`] is itself a `Future` (await it) and also offers a
//! non-blocking [`Receiver::try_recv`].
