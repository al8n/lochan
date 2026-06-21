<div align="center">
<h1>lochan</h1>
</div>
<div align="center">

Single-threaded (`!Send`), `no_std`, **no-atomics** async channels for thread-per-core runtimes.

[<img alt="github" src="https://img.shields.io/badge/GitHub-al8n/lochan-8da0cb?style=for-the-badge&logo=github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/lochan/ci.yml?style=for-the-badge&logo=github-actions&label=ci" height="22">][CI-url]
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-lochan-66c2a5?style=for-the-badge&logo=docs.rs" height="22">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/lochan?style=for-the-badge&logo=rust" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-Apache%202.0%2FMIT-blue.svg?style=for-the-badge" height="22">

English | [简体中文][zh-cn-url]

</div>

`lochan` is a family of **single-threaded** async channels for thread-per-core
runtimes — compio, monoio, glommio, embassy, a tokio `LocalSet`, and the like.
The handles hold `Rc` rather than `Arc`, so they are `!Send` (producer and
consumer always live on one thread) and the implementation uses **no atomics** —
making them strictly lighter than their `Send` counterparts (`tokio`, `flume`,
`futures`) on a single-threaded executor. `no_std` + `alloc`.

## Channels

- **`mpsc`** — multi-producer, single-consumer. `bounded` (a fixed
  `MaybeUninit` ring) and `unbounded` (a segmented block-list that never
  reallocates). Non-blocking `try_send` / `try_recv`, plus awaitable
  `send` / `recv`.
- **`oneshot`** — a single value sent once; the `Receiver` is itself a `Future`.

Every awaitable method returns a named `Unpin` + [`FusedFuture`] type, so it
drops into `select_biased!` without `.fuse()` and can be stored in a hand-rolled
driver state machine.

## Usage

```rust
// mpsc — sync surface
let (tx, mut rx) = lochan::mpsc::bounded::<u32>(16);
tx.try_send(1).unwrap();
assert_eq!(rx.try_recv(), Ok(1));

// mpsc — async surface
tx.send(2).await.unwrap();
assert_eq!(rx.recv().await, Some(2));

// oneshot — the Receiver is the future
let (tx, rx) = lochan::oneshot::channel::<u32>();
tx.send(42).unwrap();
assert_eq!(rx.await, Ok(42));
```

## Installation

```toml
[dependencies]
lochan = "0.1"
```

For `no_std`, disable default features: `lochan = { version = "0.1", default-features = false }`.

#### License

`lochan` is under the terms of both the MIT license and the Apache License
(Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2026 Al Liu.

[Github-url]: https://github.com/al8n/lochan/
[CI-url]: https://github.com/al8n/lochan/actions/workflows/ci.yml
[doc-url]: https://docs.rs/lochan
[crates-url]: https://crates.io/crates/lochan
[zh-cn-url]: https://github.com/al8n/lochan/tree/main/README-zh_CN.md
[`FusedFuture`]: https://docs.rs/futures-core/latest/futures_core/future/trait.FusedFuture.html
