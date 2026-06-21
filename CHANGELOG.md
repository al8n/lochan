# UNRELEASED

# 0.1.1 (June 21st, 2026)

FEATURES

- The `mpsc::Receiver` now implements `Stream` and `FusedStream`, and both the `mpsc`
  and `oneshot` receivers expose a non-blocking `try_iter()`. An `mpsc` receiver can be
  driven with `StreamExt` (`while let Some(x) = rx.next().await`, `.map`, `.collect`, …)
  or drained synchronously; the `oneshot` `try_iter()` yields at most one item
  (`oneshot::Receiver` stays a `Future`, not a `Stream`).

# 0.1.0 (June 21st, 2026)

Initial release.

FEATURES

- Single-threaded (`!Send`), `no_std` + `alloc`, no-atomics async channels for
  thread-per-core runtimes (compio, monoio, glommio, embassy, a tokio `LocalSet`,
  …): handles hold `Rc` rather than `Arc`, so they are strictly lighter than their
  `Send` counterparts on a single-threaded executor.
- `mpsc` — multi-producer, single-consumer, in `bounded` (a fixed `MaybeUninit`
  ring) and `unbounded` (a segmented block-list that never reallocates) flavors,
  with non-blocking `try_send` / `try_recv` and awaitable `send` / `recv`. The
  awaitable methods return named `Unpin` + `FusedFuture` types that drop into
  `select_biased!` without `.fuse()`.
- `oneshot` — a single value sent once; the `Receiver` is itself a `Future`.
