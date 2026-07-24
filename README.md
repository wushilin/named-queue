# named_queue

Named, typed, bounded work-sharing queues over
[flume](https://crates.io/crates/flume), with a graceful shutdown lifecycle.

This crate is an in-process queue registry, not a broker or pub/sub system.
Queues are addressed by name and message type; cloned receivers compete for
work, so each message is delivered to one receiver. Use `QueueRegistry::new()`
for explicit ownership, or the module-level functions for a process-wide default
registry.

- `create::<T>(name, capacity)` registers a bounded queue.
- Any number of `Sender`s and `Receiver`s share that queue; receivers
  work-share rather than broadcast.
- `shutdown(name)` revokes the queue's sender handle. Sends that observe
  shutdown fail, while receivers, existing or acquired while messages remain,
  can drain the queue.
- `state(name)` probes a queue (`Open`/`Closed` with the pending count);
  `destroy(name)` force-retires the registry entry, discarding messages.
- Send/recv never touch the registry lock; the hot path is pure flume plus one
  lock-free pointer load.

```rust
use named_queue::{QueueRegistry, QueueState};

let registry = QueueRegistry::new();
registry.create::<String>("greetings", 16).unwrap();

let tx = registry.acquire_sender::<String>("greetings").unwrap();
let rx = registry.acquire_receiver::<String>("greetings").unwrap();

tx.send("hello".to_string()).unwrap();
assert_eq!(registry.state("greetings"), Ok(QueueState::Open { pending: 1 }));
assert_eq!(rx.recv().unwrap(), "hello");

registry.shutdown("greetings").unwrap();
assert!(tx.send("too late".to_string()).is_err());
```

## Broadcasting Queues

`create_broadcasting::<T>(name, capacity)` registers a pub/sub-style queue
under the same contract — `acquire_sender`, `acquire_receiver`, `shutdown`,
`state`, and `destroy` keep their exact signatures, and `T` only additionally
needs `Clone`:

```rust
use named_queue::QueueRegistry;

let registry = QueueRegistry::new();
registry.create_broadcasting::<String>("events", 64).unwrap();

let tx = registry.acquire_sender::<String>("events").unwrap();
let a = registry.acquire_receiver::<String>("events").unwrap();
let b = registry.acquire_receiver::<String>("events").unwrap();

tx.send("tick".to_string()).unwrap();
assert_eq!(a.recv().unwrap(), "tick"); // every subscriber gets a copy
assert_eq!(b.recv().unwrap(), "tick");
```

Semantics:

- **From now onwards.** Each `acquire_receiver` call starts a fresh
  subscription with its own empty buffer of `capacity`; nothing sent earlier
  is replayed, and no history is kept.
- **Lossy only under lag.** Delivery is lossless while a subscriber stays
  within `capacity` messages of the sender. Beyond that, its oldest buffered
  message is dropped for the newest one, so a stalled subscriber resumes with
  the freshest `capacity` messages.
- **Senders never block.** `send`, `send_async`, and `try_send` complete
  immediately; `try_send` never reports `WouldBlock`. A slow subscriber never
  penalizes fast ones. With zero subscribers a send succeeds and the message
  is discarded.
- **Clones compete.** Cloning a broadcast `Receiver` shares its subscription
  (the clones work-share that buffer). For an independent copy of the stream,
  call `acquire_receiver` again.
- **Shutdown drains.** After `shutdown`, sends fail with `Closed`, existing
  subscribers drain their buffers then see `Closed`, and no new subscribers
  are admitted (a fresh subscription could never hold anything to drain).
  `destroy_take` returns an empty `Vec` for broadcasting queues — every
  subscriber holds its own copy, so there is no canonical pending set.
- `state(name)` reports `pending` as the worst subscriber lag (the largest
  buffered count across subscriptions).

## Default Registry

For small applications, the crate also exposes a process-wide default registry.
The module-level functions mirror the `QueueRegistry` methods:

```rust
named_queue::create::<String>("jobs", 32).unwrap();

let tx = named_queue::acquire_sender::<String>("jobs").unwrap();
let rx = named_queue::acquire_receiver::<String>("jobs").unwrap();

tx.send("index".to_string()).unwrap();
assert_eq!(rx.recv().unwrap(), "index");

named_queue::destroy("jobs").unwrap();
```

The default registry is convenient, but it is shared for the life of the
process. Prefer an explicit `QueueRegistry` when tests, libraries, or
applications need isolated queue namespaces.

## Operation Results

| Operation | Returns |
|---|---|
| `create::<T>(name, cap)` | `Ok(())` \| `Err(QueueAlreadyExists)` |
| `create_broadcasting::<T>(name, cap)` | `Ok(())` \| `Err(QueueAlreadyExists)` — one namespace shared with `create` |
| `acquire_sender::<T>(name)` | `Ok(Sender<T>)` \| `Err(NoSuchQueue \| TypeMismatch \| Shutdown)` |
| `acquire_receiver::<T>(name)` | `Ok(Receiver<T>)` \| `Err(NoSuchQueue \| TypeMismatch \| Shutdown)` — `Shutdown` only when the queue is closed **and** already drained; a closed queue with pending messages still admits receivers |
| `shutdown(name)` | `Ok(())` \| `Err(NoSuchQueue \| Shutdown)` (second call: `Shutdown` while draining, `NoSuchQueue` after retirement) |
| `state(name)` | `Ok(QueueState::Open { pending } \| QueueState::Closed { pending })` \| `Err(NoSuchQueue)` |
| `destroy(name)` | `Ok(())` \| `Err(NoSuchQueue)` |
| `destroy_take::<T>(name)` | `Ok(Vec<T>)` (the unconsumed messages) \| `Err(NoSuchQueue \| TypeMismatch)` |
| `Sender::send` / `send_async` | `Ok(())` \| `Err(SendError::Closed(msg))` (message handed back) |
| `Sender::try_send` | `Ok(())` \| `Err(TrySendError::WouldBlock(msg) \| TrySendError::Closed(msg))` |
| `Receiver::recv` / `recv_async` | `Ok(T)` \| `Err(RecvError::Closed)` |
| `Receiver::try_recv` | `Ok(T)` \| `Err(TryRecvError::WouldBlock \| TryRecvError::Closed)` |

## Lifecycle

`QueueState::Open { pending }` means the queue accepts sends and acquires, with
`pending` buffered messages. `QueueState::Closed { pending }` means the queue
has been shut down and `pending` messages remain consumable. Once a closed
queue is drained, the registry normally reports `Err(NoSuchQueue)`: the queue
is retired when shutdown finds it empty, when a receiver observes
closed-and-drained, or when a receiver is dropped after draining it. A
concurrent caller may briefly observe `Closed { pending: 0 }` or `Shutdown`
before that retirement completes.

After `shutdown(name)`, no new senders are issued and sends that observe the
shutdown fail with `Closed`. Receivers drain remaining messages then get
`Closed`; new receivers may still be acquired while messages remain. The
registry entry is retired when a receiver observes that the closed queue is
drained, or immediately at shutdown if the queue is already empty. The name
becomes reusable only after retirement.

A closed queue with pending messages that nobody drains lives forever by design,
so a late receiver can still come. `destroy(name)` is the escape hatch: it
force-retires the registry entry in any state, discarding pending messages held
by the keeper receiver and waking blocked receivers. `destroy_take::<T>(name)`
is the typed variant that returns the unconsumed messages to the caller instead
of discarding them; on `TypeMismatch` it leaves the queue untouched.

Shutdown is not a cancellation barrier for sends already in progress. A send
that began before shutdown and is blocked on capacity may still complete once a
receiver frees space; that message then drains like any other. Sends that
observe shutdown before entering the queue fail with `Closed` and return the
message.
