<div align="center">
<h1>lochan</h1>
</div>
<div align="center">

Single-threaded (`!Send`), `no_std`, **no-atomics** async channels for thread-per-core runtimes.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/lochan-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2F327b2a8aef9003246e45c6e47fe63937%2Fraw%2Flochan" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/lochan/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/lochan?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-lochan-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/lochan?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/lochan?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-Apache%202.0%2FMIT-blue.svg?style=for-the-badge" height="22">

</div>

## Overview

`lochan` is a family of **single-threaded** async channels for thread-per-core
runtimes — compio, monoio, glommio, embassy, a tokio `LocalSet`, and the like.
The handles hold `Rc` rather than `Arc`, so they are `!Send` (producer and
consumer always live on one thread) and the implementation uses **no atomics** —
making them strictly lighter than their `Send` counterparts (`tokio`, `flume`,
`futures`) on a single-threaded executor. `no_std` + `alloc`.

- **`mpsc`** — multi-producer, single-consumer. `bounded` (a fixed
  `MaybeUninit` ring) and `unbounded` (a segmented block-list that never
  reallocates). Non-blocking `try_send` / `try_recv`, plus awaitable
  `send` / `recv`.
- **`mpmc`** — multi-producer, multi-consumer. The same `bounded` / `unbounded`
  flavors, but both `Sender` and `Receiver` are `Clone`: every clone is another
  producer or consumer, and a delivered item goes to exactly one awaiting
  consumer. The `Receiver` is also a `Stream`.
- **`oneshot`** — a single value sent once; the `Receiver` is itself a `Future`.

Every awaitable method returns a named `Unpin` + [`FusedFuture`] type, so it
drops into `select_biased!` without `.fuse()` and can be stored in a hand-rolled
driver state machine.

## Installation

```toml
[dependencies]
lochan = "0.1"
```

For `no_std`, disable default features: `lochan = { version = "0.1", default-features = false }`.

## Example

```rust,ignore
// mpsc — sync surface
let (tx, mut rx) = lochan::mpsc::bounded::<u32>(16);
tx.try_send(1).unwrap();
assert_eq!(rx.try_recv(), Ok(1));

// mpsc — async surface
tx.send(2).await.unwrap();
assert_eq!(rx.recv().await, Some(2));

// mpmc — Sender AND Receiver are Clone
let (tx, rx) = lochan::mpmc::unbounded::<u32>();
let rx2 = rx.clone(); // a second consumer
tx.try_send(3).unwrap();
assert_eq!(rx2.try_recv(), Ok(3));

// oneshot — the Receiver is the future
let (tx, rx) = lochan::oneshot::channel::<u32>();
tx.send(42).unwrap();
assert_eq!(rx.await, Ok(42));
```

## Benchmarks

A throughput comparison against the other single-threaded (`!Send`) channel
crates, [`local-sync`] and [`local-channel`], measured with `cargo bench`
(criterion). Each buffer + drain row queues 1024 `u32` values then drains them on
a single task; `oneshot` times one create + send + receive. `local-channel`
provides only an unbounded `mpsc`. There is no other `!Send` multi-consumer
channel, so `mpmc` is measured against the same single-consumer peers (one
consumer).

| benchmark (1024 elements) | `lochan` | `local-sync` | `local-channel` |
| --- | --- | --- | --- |
| `mpsc` unbounded — buffer + drain | 6.4 µs · 161 Melem/s | 5.9 µs · 174 Melem/s | 6.4 µs · 160 Melem/s |
| `mpmc` unbounded — buffer + drain | 7.6 µs · 134 Melem/s | 5.9 µs · 174 Melem/s | 6.4 µs · 160 Melem/s |
| `mpsc` bounded — buffer + drain | 5.8 µs · 175 Melem/s | 12.6 µs · 81 Melem/s | — |
| `mpmc` bounded — buffer + drain | 7.5 µs · 136 Melem/s | 12.6 µs · 81 Melem/s | — |
| `oneshot` — create + send + recv | 18.8 ns | 20.6 ns | — |

Indicative only — laptop run-to-run variance is ±10–15%, so compare *within* one
`cargo bench` run rather than against the absolute figures. On that basis,
`lochan`'s `mpsc` is on par with `local-sync` on the unbounded channel and on
`oneshot`, and ~2× faster on the bounded channel (its fixed `MaybeUninit` ring
beats `local-sync`'s semaphore-gated bounded queue). `mpmc` adds ~20–30% over
`mpsc` for its multi-consumer machinery — the receiver-waker list and the
panic-safe redelivery slot — yet still runs ~1.7× faster than `local-sync` on
the bounded path.

## License

`lochan` is under the terms of both the MIT license and the Apache License
(Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2026 Al Liu.

[Github-url]: https://github.com/al8n/lochan/
[CI-url]: https://github.com/al8n/lochan/actions/workflows/ci.yml
[doc-url]: https://docs.rs/lochan
[crates-url]: https://crates.io/crates/lochan
[`FusedFuture`]: https://docs.rs/futures-core/latest/futures_core/future/trait.FusedFuture.html
[codecov-url]: https://app.codecov.io/gh/al8n/lochan/
[doc-url]: https://docs.rs/lochan
[crates-url]: https://crates.io/crates/lochan
[`local-sync`]: https://crates.io/crates/local-sync
[`local-channel`]: https://crates.io/crates/local-channel
