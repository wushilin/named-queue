# msgbus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Rust library providing named, typed, bounded MPMC channels (a "message bus") built on flume, with a graceful shutdown lifecycle: after `shutdown(name)` no new senders/receivers can be acquired and sends fail, but receivers drain the queue; once the last message is consumed the queue is removed and the name is reusable.

**Architecture:** An instance-based `MessageBus` holds a `RwLock<HashMap<String, Entry>>` registry mapping names to type-erased channel cores. Each `ChannelCore<T>` owns the single long-lived `flume::Sender<T>` and a keeper `flume::Receiver<T>`, both inside `ArcSwapOption` so `shutdown` can atomically drop them without locks on the hot path. `Sender<T>`/`Receiver<T>` wrappers share the core via `Arc`; send/recv never touch the registry lock — the lock is taken only for create/acquire/shutdown/removal.

**Tech Stack:** Rust 2021, `flume 0.11` (channel), `arc-swap 1` (lock-free shutdown of the keeper handles). Dev: `tokio 1` (async tests only — the library itself is executor-agnostic).

## Global Constraints

- Message types require only `T: Send + 'static`. No `Clone` or `Debug` bounds on `T` in the library API (error types' `std::error::Error` impls may require `T: Debug`).
- No `unsafe` code anywhere.
- Hot path (send/recv/try variants) must never take the registry `RwLock`; only lifecycle operations may.
- Every fallible operation returns a matchable enum error (API-friendly state transitions):

| Operation | Returns |
|---|---|
| `create::<T>(name, cap)` | `Ok(())` \| `Err(QueueAlreadyExists)` |
| `acquire_sender::<T>(name)` | `Ok(Sender<T>)` \| `Err(NoSuchQueue \| TypeMismatch \| ShutDown)` |
| `acquire_receiver::<T>(name)` | `Ok(Receiver<T>)` \| `Err(NoSuchQueue \| TypeMismatch \| ShutDown)` |
| `shutdown(name)` | `Ok(())` \| `Err(NoSuchQueue \| ShutDown)` (second call: `ShutDown` while draining, `NoSuchQueue` after removal) |
| `Sender::send` / `send_async` | `Ok(())` \| `Err(SendError::Closed(msg))` (message handed back) |
| `Sender::try_send` | `Ok(())` \| `Err(TrySendError::WouldBlock(msg) \| TrySendError::Closed(msg))` |
| `Receiver::recv` / `recv_async` | `Ok(T)` \| `Err(RecvError::Closed)` |
| `Receiver::try_recv` | `Ok(T)` \| `Err(TryRecvError::WouldBlock \| TryRecvError::Closed)` |

- Shutdown semantics (the spec): after `shutdown(name)` — no new acquires, existing senders get `Closed`, existing receivers drain remaining messages then get `Closed`; the registry entry is removed when the last message is consumed. If at shutdown time the queue is already empty, or there are no receivers to drain it (also: when the last receiver drops mid-drain), the entry is removed immediately and queued messages are dropped. The name becomes reusable only after removal.
- In-flight blocking sends that entered `send()` before `shutdown` may still deliver their message (it gets drained like any other); sends started after shutdown fail. Blocked senders with no receivers left are woken with `Closed`, never left hanging.
- Concurrency invariants (all must hold in every task):
  1. All registry-entry removals happen under the map **write** lock and verify identity via `Arc::ptr_eq` first, so a stale removal after name reuse is a no-op.
  2. `acquire_receiver` increments `receiver_count` while holding the **read** lock; `shutdown` decides under the **write** lock — so shutdown can never miss a just-acquired receiver and drop messages it was entitled to drain.
  3. `is_shutdown` is monotonic: once `tx` is swapped to `None` it never comes back.

## File Structure

```
msgbus/
├── Cargo.toml
├── plan.md                (this file)
├── README.md              (Task 7)
├── src/
│   ├── lib.rs             re-exports + crate docs
│   ├── error.rs           BusError, SendError, TrySendError, RecvError, TryRecvError
│   ├── channel.rs         ChannelCore<T> (shared state) + ChannelControl (type-erased view)
│   ├── bus.rs             MessageBus registry: create / acquire_* / shutdown / removal
│   ├── sender.rs          Sender<T>: send, try_send, send_async
│   └── receiver.rs        Receiver<T>: recv, try_recv, recv_async, Drop bookkeeping
└── tests/
    ├── errors.rs          Display formatting
    ├── create.rs          registration rules
    ├── send_recv.rs       acquire + sync data plane
    ├── shutdown.rs        lifecycle state machine
    ├── drain.rs           removal edge cases + threaded MPMC end-to-end
    └── async_api.rs       tokio-based async coverage
```

---

### Task 1: Project scaffold and error types

**Files:**
- Create: `Cargo.toml`, `.gitignore`, `src/lib.rs`, `src/error.rs`
- Test: `tests/errors.rs`

**Interfaces:**
- Produces: `msgbus::BusError` (`QueueAlreadyExists(String)`, `NoSuchQueue(String)`, `TypeMismatch { name, expected, actual }`, `ShutDown(String)`), `SendError<T>::Closed(T)`, `TrySendError<T>::{WouldBlock(T), Closed(T)}`, `RecvError::Closed`, `TryRecvError::{WouldBlock, Closed}`. All derive `Debug + PartialEq + Eq` (plus `Clone` where `T` doesn't block it) and implement `Display` + `std::error::Error`.

- [ ] **Step 1: Scaffold the project**

```bash
cd /home/code/home_workspace/msgbus
git init
cargo init --lib --name msgbus
```

Replace `Cargo.toml` with:

```toml
[package]
name = "msgbus"
version = "0.1.0"
edition = "2021"
description = "Named, typed, bounded MPMC message bus over flume with graceful shutdown"

[dependencies]
flume = "0.11"
arc-swap = "1"

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
```

Ensure `.gitignore` contains `/target`.

- [ ] **Step 2: Write the failing test**

Create `tests/errors.rs`:

```rust
use msgbus::{BusError, RecvError, SendError, TryRecvError, TrySendError};

#[test]
fn bus_error_display() {
    assert_eq!(
        BusError::QueueAlreadyExists("a".into()).to_string(),
        "queue `a` already exists"
    );
    assert_eq!(
        BusError::NoSuchQueue("b".into()).to_string(),
        "no such queue `b`"
    );
    assert_eq!(
        BusError::ShutDown("c".into()).to_string(),
        "queue `c` is shut down"
    );
    let e = BusError::TypeMismatch {
        name: "d".into(),
        expected: "i32",
        actual: "u8",
    };
    assert_eq!(e.to_string(), "queue `d` carries `u8`, not `i32`");
}

#[test]
fn channel_error_display_and_message_recovery() {
    assert_eq!(
        SendError::Closed(7).to_string(),
        "sending on a closed queue"
    );
    assert_eq!(SendError::Closed(7).into_inner(), 7);
    assert_eq!(
        TrySendError::WouldBlock(7).to_string(),
        "queue is full; send would block"
    );
    assert_eq!(
        TrySendError::Closed(7).to_string(),
        "sending on a closed queue"
    );
    assert_eq!(TrySendError::WouldBlock(7).into_inner(), 7);
    assert_eq!(TrySendError::Closed(7).into_inner(), 7);
    assert_eq!(
        RecvError::Closed.to_string(),
        "queue is shut down and drained"
    );
    assert_eq!(
        TryRecvError::WouldBlock.to_string(),
        "queue is empty; recv would block"
    );
    assert_eq!(
        TryRecvError::Closed.to_string(),
        "queue is shut down and drained"
    );
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --test errors`
Expected: compile FAIL — `unresolved import msgbus::BusError` (etc.)

- [ ] **Step 4: Implement the error types**

Create `src/error.rs`:

```rust
use std::error::Error;
use std::fmt;

/// Errors from bus lifecycle operations: `create`, `acquire_*`, `shutdown`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    /// `create` was called with a name that is already registered
    /// (including a queue that is shut down but not yet drained).
    QueueAlreadyExists(String),
    /// The named queue does not exist (never created, or already removed).
    NoSuchQueue(String),
    /// The queue exists but carries a different message type.
    TypeMismatch {
        name: String,
        expected: &'static str,
        actual: &'static str,
    },
    /// The queue is shut down: no new senders/receivers, no second shutdown.
    ShutDown(String),
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BusError::QueueAlreadyExists(name) => write!(f, "queue `{name}` already exists"),
            BusError::NoSuchQueue(name) => write!(f, "no such queue `{name}`"),
            BusError::TypeMismatch {
                name,
                expected,
                actual,
            } => write!(f, "queue `{name}` carries `{actual}`, not `{expected}`"),
            BusError::ShutDown(name) => write!(f, "queue `{name}` is shut down"),
        }
    }
}

impl Error for BusError {}

/// The queue is shut down; the unsent message is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError<T> {
    Closed(T),
}

impl<T> SendError<T> {
    /// Recover the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            SendError::Closed(msg) => msg,
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed queue")
    }
}

impl<T: fmt::Debug> Error for SendError<T> {}

/// Outcome of a non-blocking send probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The buffer is full; a blocking `send` would wait. Message handed back.
    WouldBlock(T),
    /// The queue is shut down. Message handed back.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Recover the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::WouldBlock(msg) | TrySendError::Closed(msg) => msg,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::WouldBlock(_) => f.write_str("queue is full; send would block"),
            TrySendError::Closed(_) => f.write_str("sending on a closed queue"),
        }
    }
}

impl<T: fmt::Debug> Error for TrySendError<T> {}

/// The queue is shut down and every remaining message has been consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    Closed,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("queue is shut down and drained")
    }
}

impl Error for RecvError {}

/// Outcome of a non-blocking receive probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The queue is currently empty; a blocking `recv` would wait.
    WouldBlock,
    /// The queue is shut down and drained.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::WouldBlock => f.write_str("queue is empty; recv would block"),
            TryRecvError::Closed => f.write_str("queue is shut down and drained"),
        }
    }
}

impl Error for TryRecvError {}
```

Replace `src/lib.rs` with:

```rust
mod error;

pub use error::{BusError, RecvError, SendError, TryRecvError, TrySendError};
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --test errors`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: scaffold msgbus crate with API-friendly error types

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Channel core and `MessageBus::create`

**Files:**
- Create: `src/channel.rs`, `src/bus.rs`
- Modify: `src/lib.rs`
- Test: `tests/create.rs`

**Interfaces:**
- Consumes: `BusError` from Task 1.
- Produces:
  - `pub(crate) struct ChannelCore<T: Send + 'static>` with fields `name: String`, `tx: ArcSwapOption<flume::Sender<T>>`, `rx: ArcSwapOption<flume::Receiver<T>>`, `receiver_count: AtomicUsize`; methods `new(name: &str, capacity: usize) -> Self`, `is_shutdown(&self) -> bool`.
  - `pub(crate) trait ChannelControl: Send + Sync` with `fn shutdown(&self) -> bool` (returns "remove entry now") and `fn is_shutdown(&self) -> bool`.
  - `pub struct MessageBus` (`Clone + Default`) with `new() -> Self` and `create<T: Send + 'static>(&self, name: &str, capacity: usize) -> Result<(), BusError>`.

- [ ] **Step 1: Write the failing test**

Create `tests/create.rs`:

```rust
use msgbus::{BusError, MessageBus};

#[test]
fn create_registers_a_queue() {
    let bus = MessageBus::new();
    assert_eq!(bus.create::<String>("events", 16), Ok(()));
}

#[test]
fn create_rejects_duplicate_names_even_across_types() {
    let bus = MessageBus::new();
    bus.create::<String>("events", 16).unwrap();
    assert_eq!(
        bus.create::<String>("events", 16),
        Err(BusError::QueueAlreadyExists("events".into()))
    );
    assert_eq!(
        bus.create::<u64>("events", 16),
        Err(BusError::QueueAlreadyExists("events".into()))
    );
}

#[test]
fn distinct_names_coexist() {
    let bus = MessageBus::new();
    assert_eq!(bus.create::<String>("a", 4), Ok(()));
    assert_eq!(bus.create::<u64>("b", 4), Ok(()));
}

#[test]
fn clones_share_state_but_new_buses_are_independent() {
    let bus = MessageBus::new();
    bus.create::<u8>("a", 4).unwrap();
    let clone = bus.clone();
    assert_eq!(
        clone.create::<u8>("a", 4),
        Err(BusError::QueueAlreadyExists("a".into()))
    );
    let other = MessageBus::new();
    assert_eq!(other.create::<u8>("a", 4), Ok(()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test create`
Expected: compile FAIL — `unresolved import msgbus::MessageBus`

- [ ] **Step 3: Implement channel core and registry**

Create `src/channel.rs`:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwapOption;

/// Shared state for one named queue. Senders and receivers hold this via
/// `Arc`; it outlives the registry entry, so late drains still work after
/// the entry has been removed.
pub(crate) struct ChannelCore<T: Send + 'static> {
    pub(crate) name: String,
    /// The only long-lived flume sender. `shutdown` swaps it to `None`;
    /// once in-flight sends finish, flume disconnects and blocked receivers
    /// wake up after draining what is left.
    pub(crate) tx: ArcSwapOption<flume::Sender<T>>,
    /// Keeper receiver: the template `acquire_receiver` clones from, and
    /// what keeps queued messages alive while no receiver is around.
    /// `shutdown` swaps it to `None` so blocked senders are woken once the
    /// last real receiver is gone (or immediately, if there are none).
    pub(crate) rx: ArcSwapOption<flume::Receiver<T>>,
    /// Live `Receiver<T>` wrappers. Incremented under the registry read
    /// lock on acquire (see bus.rs invariant) and on clone; decremented on
    /// drop.
    pub(crate) receiver_count: AtomicUsize,
}

impl<T: Send + 'static> ChannelCore<T> {
    pub(crate) fn new(name: &str, capacity: usize) -> Self {
        let (tx, rx) = flume::bounded(capacity);
        ChannelCore {
            name: name.to_string(),
            tx: ArcSwapOption::from_pointee(tx),
            rx: ArcSwapOption::from_pointee(rx),
            receiver_count: AtomicUsize::new(0),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.tx.load().is_none()
    }
}

/// Type-erased view of a `ChannelCore<T>` for operations that do not know
/// `T`, i.e. `MessageBus::shutdown`.
pub(crate) trait ChannelControl: Send + Sync {
    /// Swap out the sender and keeper receiver. Returns `true` when the
    /// registry entry can be removed immediately: the queue is already
    /// drained, or no receiver exists to drain it (queued messages are
    /// dropped in that case).
    fn shutdown(&self) -> bool;
    fn is_shutdown(&self) -> bool;
}

impl<T: Send + 'static> ChannelControl for ChannelCore<T> {
    fn shutdown(&self) -> bool {
        self.tx.store(None);
        let drained = match self.rx.load().as_ref() {
            Some(rx) => rx.is_empty(),
            None => true,
        };
        self.rx.store(None);
        drained || self.receiver_count.load(Ordering::SeqCst) == 0
    }

    fn is_shutdown(&self) -> bool {
        ChannelCore::is_shutdown(self)
    }
}
```

Create `src/bus.rs`:

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::channel::{ChannelControl, ChannelCore};
use crate::error::BusError;

pub(crate) struct Entry {
    /// The `Arc<ChannelCore<T>>`, kept as `dyn Any` for typed downcasts.
    pub(crate) any: Arc<dyn Any + Send + Sync>,
    /// The same core, viewed type-erased for `shutdown`.
    pub(crate) control: Arc<dyn ChannelControl>,
    /// For readable `TypeMismatch` errors.
    pub(crate) type_name: &'static str,
}

/// A registry of named, typed, bounded MPMC queues. Cheap to clone; clones
/// share the same registry. Send/recv never touch the registry lock.
#[derive(Clone, Default)]
pub struct MessageBus {
    map: Arc<RwLock<HashMap<String, Entry>>>,
}

impl MessageBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new bounded queue carrying `T`. Fails with
    /// `QueueAlreadyExists` if the name is taken — including by a queue that
    /// is shut down but still draining. A capacity of 0 creates a rendezvous
    /// queue (every send waits for a matching recv).
    pub fn create<T: Send + 'static>(&self, name: &str, capacity: usize) -> Result<(), BusError> {
        let mut map = self.map.write().unwrap();
        if map.contains_key(name) {
            return Err(BusError::QueueAlreadyExists(name.to_string()));
        }
        let core = Arc::new(ChannelCore::<T>::new(name, capacity));
        map.insert(
            name.to_string(),
            Entry {
                any: core.clone(),
                control: core,
                type_name: std::any::type_name::<T>(),
            },
        );
        Ok(())
    }
}
```

Update `src/lib.rs`:

```rust
mod bus;
mod channel;
mod error;

pub use bus::MessageBus;
pub use error::{BusError, RecvError, SendError, TryRecvError, TrySendError};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test create`
Expected: PASS (4 tests). `cargo test` overall: PASS (warnings about unused code are acceptable until Task 3).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: MessageBus registry with typed channel cores and create()

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `acquire_sender` / `acquire_receiver` and the sync data plane

**Files:**
- Create: `src/sender.rs`, `src/receiver.rs`
- Modify: `src/bus.rs`, `src/lib.rs`
- Test: `tests/send_recv.rs`

**Interfaces:**
- Consumes: `ChannelCore<T>`, `Entry`, `MessageBus`, error types.
- Produces:
  - `MessageBus::acquire_sender<T: Send + 'static>(&self, name: &str) -> Result<Sender<T>, BusError>`
  - `MessageBus::acquire_receiver<T: Send + 'static>(&self, name: &str) -> Result<Receiver<T>, BusError>`
  - `MessageBus` internal: `remove_core<T>(&self, core: &Arc<ChannelCore<T>>)` — ptr-eq-guarded entry removal (used by Receiver in Tasks 3–5).
  - `Sender<T>`: `send(&self, T) -> Result<(), SendError<T>>`, `try_send(&self, T) -> Result<(), TrySendError<T>>`, `name(&self) -> &str`, `Clone`.
  - `Receiver<T>`: `recv(&self) -> Result<T, RecvError>`, `try_recv(&self) -> Result<T, TryRecvError>`, `name(&self) -> &str`, `Clone`, `Drop` (count bookkeeping; abandoned-queue removal becomes reachable in Task 4).

- [ ] **Step 1: Write the failing test**

Create `tests/send_recv.rs`:

```rust
use std::collections::HashSet;

use msgbus::{BusError, MessageBus, TryRecvError, TrySendError};

#[test]
fn roundtrip() {
    let bus = MessageBus::new();
    bus.create::<String>("chat", 8).unwrap();
    let tx = bus.acquire_sender::<String>("chat").unwrap();
    let rx = bus.acquire_receiver::<String>("chat").unwrap();
    tx.send("hi".to_string()).unwrap();
    assert_eq!(rx.recv().unwrap(), "hi");
    assert_eq!(tx.name(), "chat");
    assert_eq!(rx.name(), "chat");
}

#[test]
fn acquire_on_missing_queue_is_no_such_queue() {
    let bus = MessageBus::new();
    assert_eq!(
        bus.acquire_sender::<u8>("nope").err(),
        Some(BusError::NoSuchQueue("nope".into()))
    );
    assert_eq!(
        bus.acquire_receiver::<u8>("nope").err(),
        Some(BusError::NoSuchQueue("nope".into()))
    );
}

#[test]
fn acquire_with_wrong_type_is_type_mismatch() {
    let bus = MessageBus::new();
    bus.create::<String>("events", 4).unwrap();
    match bus.acquire_sender::<u64>("events") {
        Err(BusError::TypeMismatch { name, .. }) => assert_eq!(name, "events"),
        other => panic!("expected TypeMismatch, got {:?}", other.err()),
    }
    match bus.acquire_receiver::<u64>("events") {
        Err(BusError::TypeMismatch { name, .. }) => assert_eq!(name, "events"),
        other => panic!("expected TypeMismatch, got {:?}", other.err()),
    }
}

#[test]
fn try_send_would_block_when_full_and_try_recv_would_block_when_empty() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 2).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();

    assert_eq!(rx.try_recv(), Err(TryRecvError::WouldBlock));
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.try_send(3), Err(TrySendError::WouldBlock(3)));
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(tx.try_send(3), Ok(()));
}

#[test]
fn clones_share_one_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("shared", 8).unwrap();
    let tx1 = bus.acquire_sender::<u32>("shared").unwrap();
    let tx2 = tx1.clone();
    let rx1 = bus.acquire_receiver::<u32>("shared").unwrap();
    let rx2 = rx1.clone();

    tx1.send(1).unwrap();
    tx2.send(2).unwrap();
    let got: HashSet<u32> = [rx1.recv().unwrap(), rx2.recv().unwrap()].into();
    assert_eq!(got, HashSet::from([1, 2]));
}

#[test]
fn messages_do_not_require_clone_or_debug() {
    struct Opaque(Vec<u8>);
    let bus = MessageBus::new();
    bus.create::<Opaque>("blobs", 4).unwrap();
    let tx = bus.acquire_sender::<Opaque>("blobs").unwrap();
    let rx = bus.acquire_receiver::<Opaque>("blobs").unwrap();
    // Note: no `.unwrap()` on send — that would demand `Opaque: Debug`
    // (via `SendError<Opaque>: Debug`), which this test exists to avoid.
    assert!(tx.send(Opaque(vec![1, 2, 3])).is_ok());
    assert_eq!(rx.recv().ok().map(|o| o.0), Some(vec![1, 2, 3]));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test send_recv`
Expected: compile FAIL — no method `acquire_sender` on `MessageBus`

- [ ] **Step 3: Implement Sender, Receiver, and the acquire methods**

Create `src/sender.rs`:

```rust
use std::sync::Arc;

use crate::channel::ChannelCore;
use crate::error::{SendError, TrySendError};

/// A producer handle for one named queue. Clone freely; all clones feed the
/// same queue. Every send fails with `Closed` once the queue is shut down.
pub struct Sender<T: Send + 'static> {
    core: Arc<ChannelCore<T>>,
}

impl<T: Send + 'static> Sender<T> {
    pub(crate) fn new(core: Arc<ChannelCore<T>>) -> Self {
        Sender { core }
    }

    /// Send a message, blocking while the buffer is full.
    ///
    /// `load_full` (a lock-free Arc clone) rather than a borrowed guard:
    /// the guard must not be pinned for the unbounded time a full queue can
    /// block, and the clone keeps flume alive so a send that started before
    /// `shutdown` completes instead of deadlocking.
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self.core.tx.load_full() {
            Some(tx) => tx
                .send(msg)
                .map_err(|flume::SendError(m)| SendError::Closed(m)),
            None => Err(SendError::Closed(msg)),
        }
    }

    /// Probe-send: never blocks. `WouldBlock` hands the message back when
    /// the buffer is full; `Closed` when the queue is shut down.
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        match self.core.tx.load_full() {
            Some(tx) => tx.try_send(msg).map_err(|e| match e {
                flume::TrySendError::Full(m) => TrySendError::WouldBlock(m),
                flume::TrySendError::Disconnected(m) => TrySendError::Closed(m),
            }),
            None => Err(TrySendError::Closed(msg)),
        }
    }

    /// The queue name this sender feeds.
    pub fn name(&self) -> &str {
        &self.core.name
    }
}

impl<T: Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            core: self.core.clone(),
        }
    }
}
```

Create `src/receiver.rs`:

```rust
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::bus::MessageBus;
use crate::channel::ChannelCore;
use crate::error::{RecvError, TryRecvError};

/// A consumer handle for one named queue. Clones compete for messages
/// (work-sharing, not broadcast). After `shutdown`, receivers drain what is
/// left and then get `Closed`; the receiver that consumes the last message
/// retires the registry entry, freeing the name.
pub struct Receiver<T: Send + 'static> {
    rx: flume::Receiver<T>,
    core: Arc<ChannelCore<T>>,
    bus: MessageBus,
}

impl<T: Send + 'static> Receiver<T> {
    /// `receiver_count` must already have been incremented by the caller
    /// (acquire does it under the registry read lock; `clone` does it
    /// itself).
    pub(crate) fn new(rx: flume::Receiver<T>, core: Arc<ChannelCore<T>>, bus: MessageBus) -> Self {
        Receiver { rx, core, bus }
    }

    /// Receive a message, blocking while the queue is empty. Returns
    /// `Closed` once the queue is shut down and fully drained.
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.rx.recv() {
            Ok(msg) => Ok(msg),
            Err(flume::RecvError::Disconnected) => {
                // Disconnected == shut down + drained: retire the entry.
                self.bus.remove_core(&self.core);
                Err(RecvError::Closed)
            }
        }
    }

    /// Probe-receive: never blocks. `WouldBlock` when the queue is empty,
    /// `Closed` when it is shut down and drained.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.rx.try_recv() {
            Ok(msg) => Ok(msg),
            Err(flume::TryRecvError::Empty) => Err(TryRecvError::WouldBlock),
            Err(flume::TryRecvError::Disconnected) => {
                self.bus.remove_core(&self.core);
                Err(TryRecvError::Closed)
            }
        }
    }

    /// The queue name this receiver drains.
    pub fn name(&self) -> &str {
        &self.core.name
    }
}

impl<T: Send + 'static> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        // A clone can only be made from a live receiver, so the count is
        // already >= 1 and shutdown cannot have retired the queue as
        // receiverless in between.
        self.core.receiver_count.fetch_add(1, Ordering::SeqCst);
        Receiver {
            rx: self.rx.clone(),
            core: self.core.clone(),
            bus: self.bus.clone(),
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        let prev = self.core.receiver_count.fetch_sub(1, Ordering::SeqCst);
        // Last receiver on a shut-down queue: nobody can drain what's left,
        // so retire the entry now (dropping any queued messages). Safe
        // outside a lock: after shutdown the count can never rise again.
        if prev == 1 && self.core.is_shutdown() {
            self.bus.remove_core(&self.core);
        }
    }
}
```

In `src/bus.rs`, extend the imports and `impl MessageBus`:

```rust
use std::sync::atomic::Ordering;

use crate::receiver::Receiver;
use crate::sender::Sender;
```

```rust
    /// Get a producer handle. Errors: `NoSuchQueue`, `TypeMismatch`,
    /// `ShutDown`.
    pub fn acquire_sender<T: Send + 'static>(&self, name: &str) -> Result<Sender<T>, BusError> {
        let map = self.map.read().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        if core.is_shutdown() {
            return Err(BusError::ShutDown(name.to_string()));
        }
        Ok(Sender::new(core))
    }

    /// Get a consumer handle. Errors: `NoSuchQueue`, `TypeMismatch`,
    /// `ShutDown`.
    pub fn acquire_receiver<T: Send + 'static>(&self, name: &str) -> Result<Receiver<T>, BusError> {
        let map = self.map.read().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        let rx = core
            .rx
            .load_full()
            .ok_or_else(|| BusError::ShutDown(name.to_string()))?;
        // Incremented while the read lock is held: `shutdown` takes the
        // write lock, so it can never observe a stale zero and discard
        // messages this receiver is entitled to drain.
        core.receiver_count.fetch_add(1, Ordering::SeqCst);
        Ok(Receiver::new((*rx).clone(), core, self.clone()))
    }

    fn core_of<T: Send + 'static>(
        map: &HashMap<String, Entry>,
        name: &str,
    ) -> Result<Arc<ChannelCore<T>>, BusError> {
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        entry
            .any
            .clone()
            .downcast::<ChannelCore<T>>()
            .map_err(|_| BusError::TypeMismatch {
                name: name.to_string(),
                expected: std::any::type_name::<T>(),
                actual: entry.type_name,
            })
    }

    /// Remove `core`'s entry if it is still the one registered under its
    /// name. Called by receivers observing the drained/abandoned states.
    /// The `Arc::ptr_eq` check makes a stale call harmless after the name
    /// has been reused by a new queue.
    pub(crate) fn remove_core<T: Send + 'static>(&self, core: &Arc<ChannelCore<T>>) {
        let mut map = self.map.write().unwrap();
        let Some(entry) = map.get(&core.name) else {
            return;
        };
        let Ok(existing) = entry.any.clone().downcast::<ChannelCore<T>>() else {
            return;
        };
        if Arc::ptr_eq(&existing, core) {
            map.remove(&core.name);
        }
    }
```

Update `src/lib.rs`:

```rust
mod bus;
mod channel;
mod error;
mod receiver;
mod sender;

pub use bus::MessageBus;
pub use error::{BusError, RecvError, SendError, TryRecvError, TrySendError};
pub use receiver::Receiver;
pub use sender::Sender;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (errors, create, send_recv — all green)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: typed Sender/Receiver acquisition and sync send/recv

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Shutdown lifecycle

**Files:**
- Modify: `src/bus.rs`
- Test: `tests/shutdown.rs`

**Interfaces:**
- Consumes: `ChannelControl::shutdown` (Task 2), `Sender`/`Receiver` (Task 3).
- Produces: `MessageBus::shutdown(&self, name: &str) -> Result<(), BusError>` — note it is **not generic**: it works through the type-erased `control` handle.

- [ ] **Step 1: Write the failing test**

Create `tests/shutdown.rs`:

```rust
use std::thread;
use std::time::Duration;

use msgbus::{BusError, MessageBus, RecvError, SendError, TrySendError};

#[test]
fn shutdown_unknown_name_is_no_such_queue() {
    let bus = MessageBus::new();
    assert_eq!(
        bus.shutdown("ghost"),
        Err(BusError::NoSuchQueue("ghost".into()))
    );
}

#[test]
fn shutdown_blocks_new_acquires_and_all_sends_but_lets_receivers_drain() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(42).unwrap();

    assert_eq!(bus.shutdown("q"), Ok(()));

    // Entry retained while a receiver still has a message to drain.
    assert_eq!(
        bus.acquire_sender::<u32>("q").err(),
        Some(BusError::ShutDown("q".into()))
    );
    assert_eq!(
        bus.acquire_receiver::<u32>("q").err(),
        Some(BusError::ShutDown("q".into()))
    );
    assert_eq!(bus.shutdown("q"), Err(BusError::ShutDown("q".into())));

    // Existing senders are cut off, message handed back.
    assert_eq!(tx.send(1), Err(SendError::Closed(1)));
    assert_eq!(tx.try_send(2), Err(TrySendError::Closed(2)));

    // Existing receivers drain, then see Closed.
    assert_eq!(rx.recv(), Ok(42));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
}

#[test]
fn empty_queue_is_removed_immediately_on_shutdown() {
    let bus = MessageBus::new();
    bus.create::<u8>("q", 4).unwrap();
    assert_eq!(bus.shutdown("q"), Ok(()));
    // Name is free again right away…
    assert_eq!(bus.create::<u8>("q", 4), Ok(()));
    // …and a second shutdown of the *old* queue would have been NoSuchQueue:
    let bus2 = MessageBus::new();
    bus2.create::<u8>("gone", 4).unwrap();
    bus2.shutdown("gone").unwrap();
    assert_eq!(
        bus2.shutdown("gone"),
        Err(BusError::NoSuchQueue("gone".into()))
    );
}

#[test]
fn shutdown_with_no_receivers_discards_queued_messages() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(bus.shutdown("q"), Ok(()));
    // Nobody could ever consume those messages, so the entry is gone.
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
}

#[test]
fn blocked_receiver_wakes_with_closed_on_shutdown() {
    let bus = MessageBus::new();
    bus.create::<u8>("q", 4).unwrap();
    let rx = bus.acquire_receiver::<u8>("q").unwrap();

    let handle = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(100)); // let the thread block in recv
    bus.shutdown("q").unwrap();

    assert_eq!(handle.join().unwrap(), Err(RecvError::Closed));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test shutdown`
Expected: compile FAIL — no method `shutdown` on `MessageBus`

- [ ] **Step 3: Implement shutdown**

Add to `impl MessageBus` in `src/bus.rs`:

```rust
    /// Shut the named queue down: no new senders/receivers, all sends fail.
    /// Existing receivers drain the remaining messages; the entry is removed
    /// once the last message is consumed. If the queue is already empty, or
    /// no receiver exists to drain it, the entry is removed immediately
    /// (discarding any queued messages in the receiverless case).
    ///
    /// Errors: `NoSuchQueue` (never existed, or already fully retired),
    /// `ShutDown` (shutdown already in progress, still draining).
    pub fn shutdown(&self, name: &str) -> Result<(), BusError> {
        let mut map = self.map.write().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        if entry.control.is_shutdown() {
            return Err(BusError::ShutDown(name.to_string()));
        }
        if entry.control.shutdown() {
            map.remove(name);
        }
        Ok(())
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (all suites)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: graceful shutdown lifecycle with immediate retire of drained/abandoned queues

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Drain-completion removal, abandonment, and threaded MPMC end-to-end

**Files:**
- Test: `tests/drain.rs` (no production code expected — these tests pin down behavior already wired up in Tasks 3–4; if any fails, fix the production code, not the test)

**Interfaces:**
- Consumes: the full public API from Tasks 2–4.

- [ ] **Step 1: Write the tests**

Create `tests/drain.rs`:

```rust
use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use msgbus::{BusError, MessageBus, RecvError, SendError};

#[test]
fn name_frees_only_after_last_message_is_consumed() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    bus.shutdown("q").unwrap();
    // Still draining: the name is not reusable yet.
    assert_eq!(
        bus.create::<u32>("q", 4),
        Err(BusError::QueueAlreadyExists("q".into()))
    );

    assert_eq!(rx.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Err(RecvError::Closed)); // last consume retires entry

    assert_eq!(bus.create::<u32>("q", 4), Ok(()));
}

#[test]
fn dropping_last_receiver_after_shutdown_retires_the_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(1).unwrap();

    bus.shutdown("q").unwrap();
    assert_eq!(
        bus.create::<u32>("q", 4),
        Err(BusError::QueueAlreadyExists("q".into()))
    );

    drop(rx); // abandons the drain
    assert_eq!(bus.create::<u32>("q", 4), Ok(()));
}

#[test]
fn blocked_sender_wakes_with_closed_when_queue_is_abandoned() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 1).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap(); // buffer now full

    let handle = thread::spawn(move || tx.send(2));
    thread::sleep(Duration::from_millis(100)); // let the thread block in send

    // No receivers exist: shutdown drops the keeper receiver, flume
    // disconnects, and the blocked sender gets its message back.
    bus.shutdown("q").unwrap();
    assert_eq!(handle.join().unwrap(), Err(SendError::Closed(2)));
}

#[test]
fn mpmc_end_to_end_under_backpressure() {
    const SENDERS: usize = 4;
    const RECEIVERS: usize = 4;
    const PER_SENDER: usize = 250;

    let bus = MessageBus::new();
    bus.create::<usize>("work", 16).unwrap();

    let receivers: Vec<_> = (0..RECEIVERS)
        .map(|_| {
            let rx = bus.acquire_receiver::<usize>("work").unwrap();
            thread::spawn(move || {
                let mut got = Vec::new();
                while let Ok(v) = rx.recv() {
                    got.push(v);
                }
                got // recv returned Closed: shut down + drained
            })
        })
        .collect();

    let senders: Vec<_> = (0..SENDERS)
        .map(|i| {
            let tx = bus.acquire_sender::<usize>("work").unwrap();
            thread::spawn(move || {
                for j in 0..PER_SENDER {
                    tx.send(i * PER_SENDER + j).unwrap();
                }
            })
        })
        .collect();

    for s in senders {
        s.join().unwrap();
    }
    bus.shutdown("work").unwrap();

    let mut all = HashSet::new();
    for r in receivers {
        for v in r.join().unwrap() {
            assert!(all.insert(v), "message {v} delivered twice");
        }
    }
    assert_eq!(all.len(), SENDERS * PER_SENDER);

    // Fully drained: the name is free again.
    assert_eq!(bus.create::<usize>("work", 16), Ok(()));
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --test drain`
Expected: PASS (4 tests). If a test fails, debug the production code (Tasks 2–4 logic) — the tests encode the specified semantics.

- [ ] **Step 3: Run the full suite**

Run: `cargo test`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test: drain-completion removal, abandonment wakeups, threaded MPMC end-to-end

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Async API

**Files:**
- Modify: `src/sender.rs`, `src/receiver.rs`
- Test: `tests/async_api.rs`

**Interfaces:**
- Consumes: `Sender<T>`/`Receiver<T>` internals from Task 3.
- Produces: `Sender::send_async(&self, T) -> impl Future<Output = Result<(), SendError<T>>>`, `Receiver::recv_async(&self) -> impl Future<Output = Result<T, RecvError>>` (plain `async fn`s; executor-agnostic — flume futures work on any runtime).

- [ ] **Step 1: Write the failing test**

Create `tests/async_api.rs`:

```rust
use std::time::Duration;

use msgbus::{MessageBus, RecvError};

#[tokio::test(flavor = "multi_thread")]
async fn async_roundtrip() {
    let bus = MessageBus::new();
    bus.create::<String>("chat", 8).unwrap();
    let tx = bus.acquire_sender::<String>("chat").unwrap();
    let rx = bus.acquire_receiver::<String>("chat").unwrap();

    tx.send_async("hello".to_string()).await.unwrap();
    assert_eq!(rx.recv_async().await.unwrap(), "hello");
}

#[tokio::test(flavor = "multi_thread")]
async fn send_async_applies_backpressure() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 1).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();

    tx.send_async(1).await.unwrap(); // buffer full
    let pending = tokio::spawn(async move { tx.send_async(2).await });
    tokio::time::sleep(Duration::from_millis(50)).await; // let it park

    assert_eq!(rx.recv_async().await, Ok(1)); // frees a slot
    pending.await.unwrap().unwrap(); // parked send completes
    assert_eq!(rx.recv_async().await, Ok(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn recv_async_drains_then_sees_closed() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();

    tx.send_async(9).await.unwrap();
    bus.shutdown("q").unwrap();

    assert_eq!(rx.recv_async().await, Ok(9));
    assert_eq!(rx.recv_async().await, Err(RecvError::Closed));
    // Drained: name reusable.
    assert_eq!(bus.create::<u32>("q", 4), Ok(()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test async_api`
Expected: compile FAIL — no method `send_async` on `Sender`

- [ ] **Step 3: Implement the async methods**

Add to `impl<T: Send + 'static> Sender<T>` in `src/sender.rs`:

```rust
    /// Async send: awaits while the buffer is full. Executor-agnostic.
    pub async fn send_async(&self, msg: T) -> Result<(), SendError<T>> {
        match self.core.tx.load_full() {
            Some(tx) => tx
                .send_async(msg)
                .await
                .map_err(|flume::SendError(m)| SendError::Closed(m)),
            None => Err(SendError::Closed(msg)),
        }
    }
```

Add to `impl<T: Send + 'static> Receiver<T>` in `src/receiver.rs`:

```rust
    /// Async receive: awaits while the queue is empty. Returns `Closed`
    /// once the queue is shut down and fully drained. Executor-agnostic.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        match self.rx.recv_async().await {
            Ok(msg) => Ok(msg),
            Err(flume::RecvError::Disconnected) => {
                self.bus.remove_core(&self.core);
                Err(RecvError::Closed)
            }
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (all suites)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: executor-agnostic async send/recv

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Documentation and polish

**Files:**
- Modify: `src/lib.rs`
- Create: `README.md`

**Interfaces:**
- Consumes: the complete public API. No API changes in this task.

- [ ] **Step 1: Write crate-level docs with a runnable doctest**

Prepend to `src/lib.rs`:

```rust
//! Named, typed, bounded MPMC queues over [flume], with a graceful
//! shutdown lifecycle.
//!
//! - `create::<T>(name, capacity)` registers a bounded queue.
//! - Any number of [`Sender`]s and [`Receiver`]s share that queue
//!   (work-sharing: each message is delivered to exactly one receiver).
//! - `shutdown(name)` stops new acquires and new sends; receivers drain
//!   what is left, and the queue is retired once the last message is
//!   consumed — after which the name is reusable.
//! - Send/recv never touch the registry lock; the hot path is pure flume
//!   plus one lock-free pointer load.
//!
//! ```
//! use msgbus::MessageBus;
//!
//! let bus = MessageBus::new();
//! bus.create::<String>("greetings", 16).unwrap();
//!
//! let tx = bus.acquire_sender::<String>("greetings").unwrap();
//! let rx = bus.acquire_receiver::<String>("greetings").unwrap();
//!
//! tx.send("hello".to_string()).unwrap();
//! assert_eq!(rx.recv().unwrap(), "hello");
//!
//! bus.shutdown("greetings").unwrap();
//! assert!(tx.send("too late".to_string()).is_err());
//! ```
```

- [ ] **Step 2: Run the doctest**

Run: `cargo test --doc`
Expected: PASS (1 doctest)

- [ ] **Step 3: Write README.md**

Create `README.md` covering: what the crate does (the bullet list from the crate docs), the quick-start example (same as the doctest), the state-transition table from **Global Constraints** above (copy it verbatim — it documents every operation's possible results), and the shutdown-semantics paragraph (drain rules, immediate-retire cases, in-flight send caveat).

- [ ] **Step 4: Lint, format, full verification**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

Expected: fmt makes no semantic changes; clippy clean; all tests pass. Fix any clippy findings before committing.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: crate docs with doctest and README

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
