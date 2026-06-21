//! Throughput comparison of `lochan` against other single-threaded (`!Send`) async
//! channels: [`local-sync`] and [`local-channel`].
//!
//! Each buffer+drain benchmark queues `N` items, then drains them on one task — the hot
//! ready path, with no waker parking. `lochan` appears twice per group, as both its
//! `mpsc` and `mpmc` flavors, so the multi-consumer machinery's overhead is visible
//! against the single-consumer `mpsc`. There is no other `!Send` multi-consumer channel
//! to compare `mpmc` against directly, so its peers here are the single-consumer ones.
//! `local-channel` only offers an unbounded mpsc, so it appears in that group alone; only
//! `local-sync` and `lochan` provide a oneshot.
//!
//! Run with `cargo bench`.
//!
//! [`local-sync`]: https://crates.io/crates/local-sync
//! [`local-channel`]: https://crates.io/crates/local-channel

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures::executor::block_on;

const N: u64 = 1024;

fn unbounded(c: &mut Criterion) {
  let mut group = c.benchmark_group("unbounded/buffer+drain");
  group.throughput(Throughput::Elements(N));

  group.bench_function("lochan-mpsc", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, mut rx) = lochan::mpsc::unbounded::<u32>();
        for i in 0..N as u32 {
          tx.try_send(i).unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.bench_function("lochan-mpmc", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, rx) = lochan::mpmc::unbounded::<u32>();
        for i in 0..N as u32 {
          tx.try_send(i).unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.bench_function("local-sync", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, mut rx) = local_sync::mpsc::unbounded::channel::<u32>();
        for i in 0..N as u32 {
          tx.send(i).unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.bench_function("local-channel", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, mut rx) = local_channel::mpsc::channel::<u32>();
        for i in 0..N as u32 {
          tx.send(i).unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.finish();
}

fn bounded(c: &mut Criterion) {
  let mut group = c.benchmark_group("bounded/buffer+drain");
  group.throughput(Throughput::Elements(N));

  group.bench_function("lochan-mpsc", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, mut rx) = lochan::mpsc::bounded::<u32>(N as usize);
        for i in 0..N as u32 {
          tx.send(i).await.unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.bench_function("lochan-mpmc", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, rx) = lochan::mpmc::bounded::<u32>(N as usize);
        for i in 0..N as u32 {
          tx.send(i).await.unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.bench_function("local-sync", |b| {
    b.iter(|| {
      block_on(async {
        let (tx, mut rx) = local_sync::mpsc::bounded::channel::<u32>(N as usize);
        for i in 0..N as u32 {
          tx.send(i).await.unwrap();
        }
        let mut acc = 0u64;
        for _ in 0..N {
          acc += rx.recv().await.unwrap() as u64;
        }
        black_box(acc)
      })
    })
  });

  group.finish();
}

fn oneshot(c: &mut Criterion) {
  let mut group = c.benchmark_group("oneshot/create+send+recv");

  group.bench_function("lochan", |b| {
    b.iter(|| {
      let (tx, mut rx) = lochan::oneshot::channel::<u32>();
      tx.send(black_box(42)).unwrap();
      black_box(rx.try_recv().unwrap())
    })
  });

  group.bench_function("local-sync", |b| {
    b.iter(|| {
      let (tx, mut rx) = local_sync::oneshot::channel::<u32>();
      tx.send(black_box(42)).unwrap();
      black_box(rx.try_recv().unwrap())
    })
  });

  group.finish();
}

criterion_group!(benches, unbounded, bounded, oneshot);
criterion_main!(benches);
