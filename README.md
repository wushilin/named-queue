# msgbus

Named, typed, bounded MPMC queues over [flume](https://crates.io/crates/flume),
with a graceful shutdown lifecycle.

- `create::<T>(name, capacity)` registers a bounded queue.
- Any number of `Sender`s and `Receiver`s share that queue (work-sharing: each
  message is delivered to exactly one receiver).
- `shutdown(name)` stops new senders and new sends; receivers, existing or
  acquired while messages remain, drain the queue, which is retired once the
  last message is consumed. The name is then reusable.
- `state(name)` probes a queue (`Open`/`Closed` with the pending count);
  `destroy(name)` force-retires one, discarding messages.
- Send/recv never touch the registry lock; the hot path is pure flume plus one
  lock-free pointer load.

```rust
use msgbus::{MessageBus, QueueState};

let bus = MessageBus::new();
bus.create::<String>("greetings", 16).unwrap();

let tx = bus.acquire_sender::<String>("greetings").unwrap();
let rx = bus.acquire_receiver::<String>("greetings").unwrap();

tx.send("hello".to_string()).unwrap();
assert_eq!(bus.state("greetings"), Ok(QueueState::Open { pending: 1 }));
assert_eq!(rx.recv().unwrap(), "hello");

bus.shutdown("greetings").unwrap();
assert!(tx.send("too late".to_string()).is_err());
```

## Operation Results

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

## Lifecycle

`QueueState::Open { pending }` means the queue accepts sends and acquires, with
`pending` buffered messages. `QueueState::Closed { pending }` means the queue
has been shut down and `pending` messages remain consumable. Closed and nothing
can ever come is reported as `Err(NoSuchQueue)`: a closed queue is retired the
moment it is drained, so `Closed { pending: 0 }` is only observable in a fleeting
race window between drain and retirement.

After `shutdown(name)`, no new senders are issued and all sends fail with
`Closed`. Receivers drain remaining messages then get `Closed`; new receivers may
still be acquired while messages remain. The registry entry is retired when the
last message is consumed, or immediately at shutdown if the queue is already
empty. The name becomes reusable only after retirement.

A closed queue with pending messages that nobody drains lives forever by design,
so a late receiver can still come. `destroy(name)` is the escape hatch: it
force-retires a queue in any state, discarding pending messages and waking
blocked receivers. `destroy_take::<T>(name)` is the typed variant that returns
the unconsumed messages to the caller instead of discarding them; on
`TypeMismatch` it leaves the queue untouched.

In-flight blocking sends that entered `send()` before `shutdown` may still
deliver their message, which then drains like any other. Sends started after
shutdown fail. A sender blocked on a full closed queue stays parked until a
receiver frees space or the queue is destroyed.
