# msgbus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Rust library providing named, typed, bounded MPMC channels (a "message bus") built on flume, with a graceful shutdown lifecycle: after `shutdown(name)` no new senders exist and sends fail, but receivers — existing ones, or new ones acquired while messages remain — drain the queue; once the last message is consumed the queue is retired and the name is reusable. `state(name)` probes any queue; `destroy(name)` force-retires one, discarding pending messages.

**Architecture:** An instance-based `MessageBus` holds a `RwLock<HashMap<String, Entry>>` registry mapping names to type-erased channel cores. Each `ChannelCore<T>` owns the single long-lived `flume::Sender<T>` (inside an `ArcSwapOption`, so `shutdown` can atomically revoke it lock-free) and a keeper `flume::Receiver<T>` that holds queued messages and serves as the template for late receiver acquisition. `Sender<T>`/`Receiver<T>` wrappers share the core via `Arc`; send/recv never touch the registry lock — the lock is taken only for create/acquire/shutdown/state/destroy/removal.

**Tech Stack:** Rust 2021, `flume 0.11` (channel), `arc-swap 1` (lock-free revocation of the sender). Dev: `tokio 1` (async tests only — the library itself is executor-agnostic).

## Global Constraints

- Message types require only `T: Send + 'static`. No `Clone` or `Debug` bounds on `T` in the library API (error types' `std::error::Error` impls may require `T: Debug`).
- No `unsafe` code anywhere.
- Hot path (send/recv/try variants) must never take the registry `RwLock`; only lifecycle operations may.
- Every fallible operation returns a matchable enum error (API-friendly state transitions):

| Operation | Returns |
|---|---|
| `create::<T>(name, cap)` | `Ok(())` \| `Err(QueueAlreadyExists)` |
| `acquire_sender::<T>(name)` | `Ok(Sender<T>)` \| `Err(NoSuchQueue \| TypeMismatch \| ShutDown)` |
| `acquire_receiver::<T>(name)` | `Ok(Receiver<T>)` \| `Err(NoSuchQueue \| TypeMismatch \| ShutDown)` — `ShutDown` only when the queue is closed **and** already drained; a closed queue with pending messages still admits receivers |
| `shutdown(name)` | `Ok(())` \| `Err(NoSuchQueue \| ShutDown)` (second call: `ShutDown` while draining, `NoSuchQueue` after retirement) |
| `state(name)` | `Ok(QueueState::Open { pending } \| QueueState::Closed { pending })` \| `Err(NoSuchQueue)` |
| `destroy(name)` | `Ok(())` \| `Err(NoSuchQueue)` |
| `destroy_take::<T>(name)` | `Ok(Vec<T>)` (the unconsumed messages) \| `Err(NoSuchQueue \| TypeMismatch)` |
| `Sender::send` / `send_async` | `Ok(())` \| `Err(SendError::Closed(msg))` (message handed back) |
| `Sender::try_send` | `Ok(())` \| `Err(TrySendError::WouldBlock(msg) \| TrySendError::Closed(msg))` |
| `Receiver::recv` / `recv_async` | `Ok(T)` \| `Err(RecvError::Closed)` |
| `Receiver::try_recv` | `Ok(T)` \| `Err(TryRecvError::WouldBlock \| TryRecvError::Closed)` |

- `QueueState` semantics: `Open { pending }` — accepting sends and acquires, `pending` messages buffered. `Closed { pending }` — shut down, `pending` messages still consumable. "Closed and nothing can ever come" is `Err(NoSuchQueue)`: a closed queue is retired the moment it is drained, so `Closed { pending: 0 }` is only observable in a fleeting race window between drain and retirement.
- Shutdown semantics (the spec): after `shutdown(name)` — no new senders, all sends fail with `Closed`. Receivers drain remaining messages then get `Closed`; new receivers may still be acquired **while messages remain**. The registry entry is retired when the last message is consumed (observed by a receiver hitting the drained state, or by the last drop of a receiver on a drained closed queue) — or immediately at shutdown if the queue is already empty. The name becomes reusable only after retirement.
- A closed queue with pending messages that nobody drains lives forever — by design (a late receiver may still come). `destroy(name)` is the escape hatch: it force-retires a queue in **any** state, discarding pending messages and waking blocked receivers; blocked senders wake with either `Ok` (their message entered the queue during teardown and is then discarded with it) or `Closed(msg)`. `destroy_take::<T>(name)` is the typed variant that returns the unconsumed messages to the caller instead of discarding them, so nothing is silently swallowed; on `TypeMismatch` it leaves the queue untouched.
- In-flight blocking sends that entered `send()` before `shutdown` may still deliver their message (it gets drained like any other); sends started after shutdown fail. A sender blocked on a full closed queue stays parked until a receiver frees space or the queue is destroyed.
- Concurrency invariants (all must hold in every task):
  1. All registry-entry removals happen under the map **write** lock and verify identity via `Arc::ptr_eq` first, so a stale removal after name reuse is a no-op.
  2. While an entry exists in the map, its keeper receiver is present (`rx` is only swapped to `None` by `destroy`, under the same write lock that removes the entry).
  3. `is_shutdown` is monotonic: once `tx` is swapped to `None` it never comes back.

## File Structure

```
msgbus/
├── Cargo.toml
├── plan.md                (this file)
├── README.md              (Task 8)
├── src/
│   ├── lib.rs             re-exports + crate docs
│   ├── error.rs           BusError, SendError, TrySendError, RecvError, TryRecvError
│   ├── channel.rs         ChannelCore<T> (shared state) + ChannelControl (type-erased view)
│   ├── bus.rs             MessageBus registry + QueueState: create / acquire_* / shutdown / state / destroy
│   ├── sender.rs          Sender<T>: send, try_send, send_async
│   └── receiver.rs        Receiver<T>: recv, try_recv, recv_async, Drop retirement check
└── tests/
    ├── errors.rs          Display formatting
    ├── create.rs          registration rules
    ├── send_recv.rs       acquire + sync data plane
    ├── shutdown.rs        lifecycle state machine incl. late receivers
    ├── state.rs           state() probe + destroy()
    ├── drain.rs           drain-driven retirement + threaded MPMC end-to-end
    └── async_api.rs       tokio-based async coverage
```

---

### Task 1: Project scaffold and error types

**Files:**
- Create: `Cargo.toml`, `.gitignore`, `src/lib.rs`, `src/error.rs`
- Test: `tests/errors.rs`

**Interfaces:**
- Produces: `msgbus::BusError` (`QueueAlreadyExists(String)`, `NoSuchQueue(String)`, `TypeMismatch { name, expected, actual }`, `ShutDown(String)`), `SendError<T>::Closed(T)`, `TrySendError<T>::{WouldBlock(T), Closed(T)}`, `RecvError::Closed`, `TryRecvError::{WouldBlock, Closed}`. All derive `Debug + PartialEq + Eq` (plus `Clone` where `T` doesn't block it) and implement `Display` + `std::error::Error`.

- [x] **Step 1: Scaffold the project**

```bash
cd /home/code/home_workspace/msgbus
cargo init --lib --name msgbus
```

(The directory is already a git repository holding `plan.md`.)

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

- [x] **Step 2: Write the failing test**

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

- [x] **Step 3: Run test to verify it fails**

Run: `cargo test --test errors`
Expected: compile FAIL — `unresolved import msgbus::BusError` (etc.)

- [x] **Step 4: Implement the error types**

Create `src/error.rs`:

```rust
use std::error::Error;
use std::fmt;

/// Errors from bus lifecycle operations: `create`, `acquire_*`, `shutdown`,
/// `state`, `destroy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    /// `create` was called with a name that is already registered
    /// (including a queue that is shut down but not yet drained).
    QueueAlreadyExists(String),
    /// The named queue does not exist (never created, or already retired).
    NoSuchQueue(String),
    /// The queue exists but carries a different message type.
    TypeMismatch {
        name: String,
        expected: &'static str,
        actual: &'static str,
    },
    /// The queue is shut down. For `acquire_receiver` this means it is also
    /// already drained — a closed queue with pending messages still admits
    /// receivers.
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

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test --test errors`
Expected: PASS (2 tests)

- [x] **Step 6: Commit**

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
  - `pub(crate) struct ChannelCore<T: Send + 'static>` with fields `name: String`, `tx: ArcSwapOption<flume::Sender<T>>`, `rx: ArcSwapOption<flume::Receiver<T>>`; methods `new(name: &str, capacity: usize) -> Self`, `is_shutdown(&self) -> bool`.
  - `pub(crate) trait ChannelControl: Send + Sync` with `fn shutdown(&self) -> bool` (returns "queue already drained — retire the entry now") and `fn is_shutdown(&self) -> bool`. (Task 5 extends this trait with `pending` and `destroy`.)
  - `pub struct MessageBus` (`Clone + Default`) with `new() -> Self` and `create<T: Send + 'static>(&self, name: &str, capacity: usize) -> Result<(), BusError>`.

- [x] **Step 1: Write the failing test**

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

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test --test create`
Expected: compile FAIL — `unresolved import msgbus::MessageBus`

- [x] **Step 3: Implement channel core and registry**

Create `src/channel.rs`:

```rust
use arc_swap::ArcSwapOption;

/// Shared state for one named queue. Senders and receivers hold this via
/// `Arc`; it outlives the registry entry, so late drains still work after
/// the entry has been retired.
pub(crate) struct ChannelCore<T: Send + 'static> {
    pub(crate) name: String,
    /// The only long-lived flume sender. `shutdown` swaps it to `None`;
    /// once in-flight sends finish, flume disconnects and blocked receivers
    /// wake up after draining what is left.
    pub(crate) tx: ArcSwapOption<flume::Sender<T>>,
    /// Keeper receiver: holds queued messages while no receiver is around
    /// and serves as the template `acquire_receiver` clones from — also
    /// after shutdown, so late receivers can drain a closed queue. Only
    /// `destroy` swaps it to `None` (Task 5), under the registry write lock
    /// that removes the entry.
    pub(crate) rx: ArcSwapOption<flume::Receiver<T>>,
}

impl<T: Send + 'static> ChannelCore<T> {
    pub(crate) fn new(name: &str, capacity: usize) -> Self {
        let (tx, rx) = flume::bounded(capacity);
        ChannelCore {
            name: name.to_string(),
            tx: ArcSwapOption::from_pointee(tx),
            rx: ArcSwapOption::from_pointee(rx),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.tx.load().is_none()
    }
}

/// Type-erased view of a `ChannelCore<T>` for operations that do not know
/// `T`: `MessageBus::shutdown` (and `state`/`destroy` in Task 5).
pub(crate) trait ChannelControl: Send + Sync {
    /// Revoke the sender. Returns `true` when the queue is already drained,
    /// meaning the registry entry can be retired immediately.
    fn shutdown(&self) -> bool;
    fn is_shutdown(&self) -> bool;
}

impl<T: Send + 'static> ChannelControl for ChannelCore<T> {
    fn shutdown(&self) -> bool {
        self.tx.store(None);
        match self.rx.load().as_ref() {
            Some(rx) => rx.is_empty(),
            None => true,
        }
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
    /// The same core, viewed type-erased for shutdown/state/destroy.
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

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test --test create`
Expected: PASS (4 tests). `cargo test` overall: PASS (warnings about unused code are acceptable until later tasks).

- [x] **Step 5: Commit**

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
  - `MessageBus::acquire_receiver<T: Send + 'static>(&self, name: &str) -> Result<Receiver<T>, BusError>` (admits receivers on a closed queue while messages remain)
  - `MessageBus` internal: `remove_core<T>(&self, core: &Arc<ChannelCore<T>>)` — ptr-eq-guarded entry retirement (used by Receiver in Tasks 3–6).
  - `Sender<T>`: `send(&self, T) -> Result<(), SendError<T>>`, `try_send(&self, T) -> Result<(), TrySendError<T>>`, `name(&self) -> &str`, `Clone`.
  - `Receiver<T>`: `recv(&self) -> Result<T, RecvError>`, `try_recv(&self) -> Result<T, TryRecvError>`, `name(&self) -> &str`, `Clone`, `Drop` (retires a closed, drained queue that would otherwise never see another recv).

- [x] **Step 1: Write the failing test**

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

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test --test send_recv`
Expected: compile FAIL — no method `acquire_sender` on `MessageBus`

- [x] **Step 3: Implement Sender, Receiver, and the acquire methods**

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
use std::sync::Arc;

use crate::bus::MessageBus;
use crate::channel::ChannelCore;
use crate::error::{RecvError, TryRecvError};

/// A consumer handle for one named queue. Clones compete for messages
/// (work-sharing, not broadcast). After `shutdown`, receivers drain what is
/// left and then get `Closed`; the receiver that observes the drained state
/// retires the registry entry, freeing the name.
pub struct Receiver<T: Send + 'static> {
    rx: flume::Receiver<T>,
    core: Arc<ChannelCore<T>>,
    bus: MessageBus,
}

impl<T: Send + 'static> Receiver<T> {
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
        Receiver {
            rx: self.rx.clone(),
            core: self.core.clone(),
            bus: self.bus.clone(),
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        // A receiver that drained the last message and then drops without
        // another recv would leave a closed, empty queue with no one left
        // to observe Disconnected — retire it on the way out. Ptr-eq
        // guarding in remove_core makes a racy stale call harmless.
        if self.core.is_shutdown() && self.rx.is_empty() {
            self.bus.remove_core(&self.core);
        }
    }
}
```

In `src/bus.rs`, extend the imports and `impl MessageBus`:

```rust
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

    /// Get a consumer handle. A closed queue admits new receivers while
    /// messages remain to be drained. Errors: `NoSuchQueue`, `TypeMismatch`,
    /// `ShutDown` (closed **and** drained).
    pub fn acquire_receiver<T: Send + 'static>(&self, name: &str) -> Result<Receiver<T>, BusError> {
        let map = self.map.read().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        let rx = core
            .rx
            .load_full()
            .ok_or_else(|| BusError::ShutDown(name.to_string()))?;
        if core.is_shutdown() && rx.is_empty() {
            return Err(BusError::ShutDown(name.to_string()));
        }
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

    /// Retire `core`'s entry if it is still the one registered under its
    /// name. Called by receivers observing the drained state. The
    /// `Arc::ptr_eq` check makes a stale call harmless after the name has
    /// been reused by a new queue.
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

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (errors, create, send_recv — all green)

- [x] **Step 5: Commit**

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

- [x] **Step 1: Write the failing test**

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
fn shutdown_cuts_senders_but_lets_receivers_drain() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(42).unwrap();

    assert_eq!(bus.shutdown("q"), Ok(()));

    // No new senders; second shutdown reports the draining state.
    assert_eq!(
        bus.acquire_sender::<u32>("q").err(),
        Some(BusError::ShutDown("q".into()))
    );
    assert_eq!(bus.shutdown("q"), Err(BusError::ShutDown("q".into())));

    // Existing senders are cut off, message handed back.
    assert_eq!(tx.send(1), Err(SendError::Closed(1)));
    assert_eq!(tx.try_send(2), Err(TrySendError::Closed(2)));

    // Existing receivers drain, then see Closed; the last consume retires
    // the entry, so the name reads as gone afterwards.
    assert_eq!(rx.recv(), Ok(42));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(
        bus.acquire_receiver::<u32>("q").err(),
        Some(BusError::NoSuchQueue("q".into()))
    );
}

#[test]
fn closed_queue_admits_new_receivers_while_messages_remain() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(7).unwrap();
    bus.shutdown("q").unwrap();

    // Pending message: late receiver is admitted.
    let late = bus.acquire_receiver::<u32>("q").unwrap();
    assert_eq!(rx.recv(), Ok(7));

    // Drained (but entry still present until observed): a further acquire
    // is refused with ShutDown.
    assert_eq!(
        bus.acquire_receiver::<u32>("q").err(),
        Some(BusError::ShutDown("q".into()))
    );

    // Any receiver observing the drained state retires the entry.
    assert_eq!(late.recv(), Err(RecvError::Closed));
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
}

#[test]
fn empty_queue_is_retired_immediately_on_shutdown() {
    let bus = MessageBus::new();
    bus.create::<u8>("q", 4).unwrap();
    assert_eq!(bus.shutdown("q"), Ok(()));
    // Name is free again right away…
    assert_eq!(bus.create::<u8>("q", 4), Ok(()));
    // …and shutting down an already-retired queue is NoSuchQueue:
    let bus2 = MessageBus::new();
    bus2.create::<u8>("gone", 4).unwrap();
    bus2.shutdown("gone").unwrap();
    assert_eq!(
        bus2.shutdown("gone"),
        Err(BusError::NoSuchQueue("gone".into()))
    );
}

#[test]
fn shutdown_with_no_receivers_retains_messages_for_a_late_drain() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(bus.shutdown("q"), Ok(()));

    // Messages wait for a late receiver; the name is NOT free yet.
    assert_eq!(
        bus.create::<u32>("q", 8),
        Err(BusError::QueueAlreadyExists("q".into()))
    );
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    assert_eq!(rx.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
}

#[test]
fn drained_receiver_drop_retires_the_queue_without_a_final_recv() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(1).unwrap();
    bus.shutdown("q").unwrap();

    assert_eq!(rx.recv(), Ok(1)); // drained, but Disconnected never observed
    drop(rx); // Drop sees closed + empty and retires the entry
    assert_eq!(bus.create::<u32>("q", 4), Ok(()));
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

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test --test shutdown`
Expected: compile FAIL — no method `shutdown` on `MessageBus`

- [x] **Step 3: Implement shutdown**

Add to `impl MessageBus` in `src/bus.rs`:

```rust
    /// Shut the named queue down: no new senders, all sends fail. Existing
    /// receivers — and new ones acquired while messages remain — drain the
    /// queue; the entry is retired once the last message is consumed, or
    /// immediately if the queue is already empty. A closed queue that nobody
    /// drains lives until `destroy` is called on it.
    ///
    /// Errors: `NoSuchQueue` (never existed, or already retired), `ShutDown`
    /// (shutdown already in progress, still draining).
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

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (all suites)

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: graceful shutdown with drain-driven retirement and late receivers

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: `state()` probe, `destroy()`, and `destroy_take()`

**Files:**
- Modify: `src/channel.rs`, `src/bus.rs`, `src/lib.rs`
- Test: `tests/state.rs`

**Interfaces:**
- Consumes: `ChannelControl`, `Entry`, `MessageBus`.
- Produces:
  - `pub enum QueueState { Open { pending: usize }, Closed { pending: usize } }` (`Debug + Clone + Copy + PartialEq + Eq`), re-exported from the crate root.
  - `MessageBus::state(&self, name: &str) -> Result<QueueState, BusError>`
  - `MessageBus::destroy(&self, name: &str) -> Result<(), BusError>` (type-erased; discards)
  - `MessageBus::destroy_take<T: Send + 'static>(&self, name: &str) -> Result<Vec<T>, BusError>` (typed; returns the unconsumed messages)
  - `ChannelControl` gains `fn pending(&self) -> usize` and `fn destroy(&self)`.

- [x] **Step 1: Write the failing test**

Create `tests/state.rs`:

```rust
use std::thread;
use std::time::Duration;

use msgbus::{BusError, MessageBus, QueueState, RecvError};

#[test]
fn state_reports_open_with_pending_count() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    assert_eq!(bus.state("q"), Ok(QueueState::Open { pending: 0 }));

    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(bus.state("q"), Ok(QueueState::Open { pending: 2 }));
}

#[test]
fn state_reports_closed_with_remaining_then_no_such_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(9).unwrap();
    bus.shutdown("q").unwrap();

    assert_eq!(bus.state("q"), Ok(QueueState::Closed { pending: 1 }));
    assert_eq!(rx.recv(), Ok(9));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    // Fully drained and retired: "nothing can possibly come" == gone.
    assert_eq!(bus.state("q"), Err(BusError::NoSuchQueue("q".into())));
}

#[test]
fn state_of_unknown_queue_is_no_such_queue() {
    let bus = MessageBus::new();
    assert_eq!(
        bus.state("ghost"),
        Err(BusError::NoSuchQueue("ghost".into()))
    );
}

#[test]
fn destroy_discards_pending_and_frees_the_name() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    bus.shutdown("q").unwrap(); // retained: messages await a drainer

    assert_eq!(bus.destroy("q"), Ok(()));
    assert_eq!(bus.state("q"), Err(BusError::NoSuchQueue("q".into())));
    assert_eq!(bus.destroy("q"), Err(BusError::NoSuchQueue("q".into())));
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
}

#[test]
fn destroy_take_returns_unconsumed_messages() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    bus.shutdown("q").unwrap(); // retained: messages await a drainer

    assert_eq!(bus.destroy_take::<u32>("q"), Ok(vec![1, 2]));
    assert_eq!(bus.state("q"), Err(BusError::NoSuchQueue("q".into())));
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
    // Works on open queues too, and reports missing ones.
    assert_eq!(bus.destroy_take::<u32>("q"), Ok(vec![]));
    assert_eq!(
        bus.destroy_take::<u32>("q"),
        Err(BusError::NoSuchQueue("q".into()))
    );
}

#[test]
fn destroy_take_with_wrong_type_is_type_mismatch_and_keeps_the_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    match bus.destroy_take::<String>("q") {
        Err(BusError::TypeMismatch { name, .. }) => assert_eq!(name, "q"),
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
    // Queue untouched by the failed teardown.
    assert_eq!(bus.state("q"), Ok(QueueState::Open { pending: 0 }));
}

#[test]
fn destroy_works_on_open_queues_and_wakes_blocked_receivers() {
    let bus = MessageBus::new();
    bus.create::<u8>("q", 4).unwrap();
    let rx = bus.acquire_receiver::<u8>("q").unwrap();

    let handle = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(100)); // let the thread block in recv
    assert_eq!(bus.destroy("q"), Ok(()));

    assert_eq!(handle.join().unwrap(), Err(RecvError::Closed));
    assert_eq!(bus.create::<u8>("q", 4), Ok(()));
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test --test state`
Expected: compile FAIL — `unresolved import msgbus::QueueState`

- [x] **Step 3: Implement state and destroy**

In `src/channel.rs`, extend `ChannelControl` and its impl:

```rust
pub(crate) trait ChannelControl: Send + Sync {
    /// Revoke the sender. Returns `true` when the queue is already drained,
    /// meaning the registry entry can be retired immediately.
    fn shutdown(&self) -> bool;
    fn is_shutdown(&self) -> bool;
    /// Messages currently buffered (0 if the keeper is gone).
    fn pending(&self) -> usize;
    /// Force-teardown: revoke the sender, discard buffered messages, drop
    /// the keeper receiver. Caller must also remove the registry entry
    /// (under the same write lock).
    fn destroy(&self);
}

impl<T: Send + 'static> ChannelControl for ChannelCore<T> {
    fn shutdown(&self) -> bool {
        self.tx.store(None);
        match self.rx.load().as_ref() {
            Some(rx) => rx.is_empty(),
            None => true,
        }
    }

    fn is_shutdown(&self) -> bool {
        ChannelCore::is_shutdown(self)
    }

    fn pending(&self) -> usize {
        self.rx.load().as_ref().map(|rx| rx.len()).unwrap_or(0)
    }

    fn destroy(&self) {
        self.tx.store(None);
        if let Some(rx) = self.rx.load_full() {
            // Discard buffered messages. Bounded work: at most capacity
            // plus however many parked senders complete into freed slots.
            while rx.try_recv().is_ok() {}
        }
        self.rx.store(None);
    }
}
```

In `src/bus.rs`, add the state enum and the two methods:

```rust
/// Snapshot of one queue's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueState {
    /// Accepting sends and acquires; `pending` messages currently buffered.
    Open { pending: usize },
    /// Shut down, still draining; `pending` messages remain consumable.
    /// A drained closed queue is retired, so `Closed { pending: 0 }` is
    /// only observable in the brief window before retirement.
    Closed { pending: usize },
}
```

```rust
    /// Probe a queue's lifecycle state. `Err(NoSuchQueue)` means the queue
    /// never existed or has been retired — closed, drained, gone.
    pub fn state(&self, name: &str) -> Result<QueueState, BusError> {
        let map = self.map.read().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        let pending = entry.control.pending();
        Ok(if entry.control.is_shutdown() {
            QueueState::Closed { pending }
        } else {
            QueueState::Open { pending }
        })
    }

    /// Force-retire a queue in any state, discarding pending messages.
    /// Blocked receivers wake with `Closed`. Blocked senders wake with
    /// either `Ok` (their message slipped into the queue during teardown
    /// and was discarded with it) or `Closed(msg)`.
    pub fn destroy(&self, name: &str) -> Result<(), BusError> {
        let mut map = self.map.write().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        entry.control.destroy();
        map.remove(name);
        Ok(())
    }

    /// Typed force-retire: like [`destroy`](Self::destroy), but hands the
    /// unconsumed messages back to the caller instead of discarding them.
    /// On `TypeMismatch` the queue is left untouched.
    pub fn destroy_take<T: Send + 'static>(&self, name: &str) -> Result<Vec<T>, BusError> {
        let mut map = self.map.write().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        core.tx.store(None);
        let mut taken = Vec::new();
        if let Some(rx) = core.rx.load_full() {
            while let Ok(msg) = rx.try_recv() {
                taken.push(msg);
            }
        }
        core.rx.store(None);
        map.remove(name);
        Ok(taken)
    }
```

In `src/lib.rs`, extend the re-export:

```rust
pub use bus::{MessageBus, QueueState};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (all suites)

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: state() probe, destroy() and destroy_take() escape hatches

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Drain-driven retirement under load — threaded MPMC end-to-end

**Files:**
- Test: `tests/drain.rs` (no production code expected — these tests pin down behavior already wired up in Tasks 3–5; if any fails, fix the production code, not the test)

**Interfaces:**
- Consumes: the full public API from Tasks 2–5.

- [x] **Step 1: Write the tests**

Create `tests/drain.rs`:

```rust
use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use msgbus::{BusError, MessageBus, QueueState, RecvError};

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
    assert_eq!(bus.state("q"), Ok(QueueState::Closed { pending: 2 }));

    assert_eq!(rx.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Err(RecvError::Closed)); // last consume retires entry

    assert_eq!(bus.create::<u32>("q", 4), Ok(()));
}

#[test]
fn blocked_sender_returns_promptly_on_destroy() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 1).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap(); // buffer now full

    let handle = thread::spawn(move || tx.send(2));
    thread::sleep(Duration::from_millis(100)); // let the thread block in send
    bus.shutdown("q").unwrap(); // retained: 1 pending, sender still parked
    bus.destroy("q").unwrap();

    // The parked send either completed into a slot freed by destroy's drain
    // (Ok — its message was then discarded with the queue) or observed the
    // disconnect (Err(Closed)). Either way it must return, not hang.
    let _ = handle.join().unwrap();
    assert_eq!(bus.create::<u32>("q", 1), Ok(()));
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

- [x] **Step 2: Run the tests**

Run: `cargo test --test drain`
Expected: PASS (3 tests). If a test fails, debug the production code (Tasks 2–5 logic) — the tests encode the specified semantics.

- [x] **Step 3: Run the full suite**

Run: `cargo test`
Expected: PASS

- [x] **Step 4: Commit**

```bash
git add -A
git commit -m "test: drain-driven retirement, destroy wakeups, threaded MPMC end-to-end

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Async API

**Files:**
- Modify: `src/sender.rs`, `src/receiver.rs`
- Test: `tests/async_api.rs`

**Interfaces:**
- Consumes: `Sender<T>`/`Receiver<T>` internals from Task 3.
- Produces: `Sender::send_async(&self, T) -> impl Future<Output = Result<(), SendError<T>>>`, `Receiver::recv_async(&self) -> impl Future<Output = Result<T, RecvError>>` (plain `async fn`s; executor-agnostic — flume futures work on any runtime).

- [x] **Step 1: Write the failing test**

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
    // Drained and retired: name reusable.
    assert_eq!(bus.create::<u32>("q", 4), Ok(()));
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test --test async_api`
Expected: compile FAIL — no method `send_async` on `Sender`

- [x] **Step 3: Implement the async methods**

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

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS (all suites)

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: executor-agnostic async send/recv

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Documentation and polish

**Files:**
- Modify: `src/lib.rs`
- Create: `README.md`

**Interfaces:**
- Consumes: the complete public API. No API changes in this task.

- [x] **Step 1: Write crate-level docs with a runnable doctest**

Prepend to `src/lib.rs`:

```rust
//! Named, typed, bounded MPMC queues over [flume], with a graceful
//! shutdown lifecycle.
//!
//! - `create::<T>(name, capacity)` registers a bounded queue.
//! - Any number of [`Sender`]s and [`Receiver`]s share that queue
//!   (work-sharing: each message is delivered to exactly one receiver).
//! - `shutdown(name)` stops new senders and new sends; receivers —
//!   existing or acquired while messages remain — drain the queue, which
//!   is retired once the last message is consumed. The name is then
//!   reusable.
//! - `state(name)` probes a queue (`Open`/`Closed` with the pending
//!   count); `destroy(name)` force-retires one, discarding messages.
//! - Send/recv never touch the registry lock; the hot path is pure flume
//!   plus one lock-free pointer load.
//!
//! ```
//! use msgbus::{MessageBus, QueueState};
//!
//! let bus = MessageBus::new();
//! bus.create::<String>("greetings", 16).unwrap();
//!
//! let tx = bus.acquire_sender::<String>("greetings").unwrap();
//! let rx = bus.acquire_receiver::<String>("greetings").unwrap();
//!
//! tx.send("hello".to_string()).unwrap();
//! assert_eq!(bus.state("greetings"), Ok(QueueState::Open { pending: 1 }));
//! assert_eq!(rx.recv().unwrap(), "hello");
//!
//! bus.shutdown("greetings").unwrap();
//! assert!(tx.send("too late".to_string()).is_err());
//! ```
```

- [x] **Step 2: Run the doctest**

Run: `cargo test --doc`
Expected: PASS (1 doctest)

- [x] **Step 3: Write README.md**

Create `README.md` covering: what the crate does (the bullet list from the crate docs), the quick-start example (same as the doctest), the state-transition table from **Global Constraints** above (copy it verbatim — it documents every operation's possible results), and the shutdown/destroy semantics paragraphs from **Global Constraints** (drain rules, late receivers, immediate-retire case, the undrained-queue-lives-forever rule with `destroy` as the escape hatch, and the in-flight send caveats).

- [x] **Step 4: Lint, format, full verification**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

Expected: fmt makes no semantic changes; clippy clean; all tests pass. Fix any clippy findings before committing.

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: crate docs with doctest and README

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
