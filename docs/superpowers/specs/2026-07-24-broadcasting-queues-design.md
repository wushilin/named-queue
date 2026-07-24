# Broadcasting Named Queues — Design

Date: 2026-07-24

## Goal

Add pub/sub-style broadcasting queues to `named_queue` alongside the existing
work-sharing queues, with the same public contract:

- `create_broadcasting::<T>(name, capacity)` registers a broadcasting queue.
- `acquire_sender::<T>(name)` and `acquire_receiver::<T>(name)` keep exactly
  their existing signatures and return the existing `Sender<T>` / `Receiver<T>`
  types.
- Every subscriber receives every message sent **after** it subscribed; no
  history is kept or replayed.
- Delivery is lossy per subscriber: a subscriber that does not poll fast
  enough misses messages. Fast subscribers are never penalized by slow ones,
  and senders never block.

## Approach

**Per-subscriber fan-out over flume** (chosen over a shared ring buffer à la
`tokio::sync::broadcast`, which would require hand-rolling blocking + async
wakeups or adding a heavy dependency; flume already provides both).

### `BroadcastCore<T>` (new module `src/broadcast.rs`)

```rust
struct Subscription<T> {
    id: u64,
    tx: flume::Sender<T>,
    rx: flume::Receiver<T>, // kept for drop-oldest eviction and pending()
}

pub(crate) struct BroadcastCore<T: Send + 'static> {
    pub(crate) name: String,
    capacity: usize,                       // min 1; 0 is clamped to 1
    clone_fn: fn(&T) -> T,                 // captured where T: Clone is known
    shutdown: AtomicBool,
    subs: RwLock<Vec<Subscription<T>>>,
    next_id: AtomicU64,
}
```

The `clone_fn` trick: `create_broadcasting` requires `T: Send + Clone +
'static` and stores `|t| t.clone()` as a plain `fn` pointer. `Sender<T>` and
`Receiver<T>` therefore need no `Clone` bound on `T`; only broadcasting
creation does.

### Same public types

`Sender<T>` and `Receiver<T>` become thin enums internally:

- `Sender<T>`: `Queue(Arc<ChannelCore<T>>)` | `Broadcast(Arc<BroadcastCore<T>>)`
- `Receiver<T>`: `Queue { rx, core, registry }` |
  `Broadcast { rx, guard: Arc<SubGuard<T>>, registry }`

All public method signatures are unchanged.

### Semantics

- **send / send_async / try_send** (broadcast): if shut down →
  `Closed(msg)` / `Closed(msg)`; otherwise clone the message into each
  subscriber's buffer and return `Ok`. On a full buffer, evict that
  subscriber's oldest message and retry (bounded retries, then drop the new
  message — lossy is acceptable). Never blocks, never returns `WouldBlock`.
  With zero subscribers the message is discarded and the send still succeeds.
- **acquire_receiver** (broadcast): creates a fresh
  `flume::bounded(capacity)` pair, registers it as a new subscription, and
  returns a `Receiver` draining it. The buffer starts empty → events from now
  onwards. After shutdown, returns `QueueError::Shutdown` (a new subscription
  can never have pending messages to drain).
- **Receiver clone**: clones share the subscription and compete for its
  messages (consistent with existing `Receiver` docs). An independent
  subscriber = another `acquire_receiver` call. The subscription is removed
  from the core when the last clone drops (via an `Arc`-held guard).
- **shutdown**: marks the core shut down and drops all subscriber `tx`
  handles. Existing subscribers drain their buffers, then observe `Closed`.
  Registry entry is retired once drained (same rule as work-sharing queues).
- **state**: `pending` reports the **maximum** buffered count across
  subscribers (worst-case lag); `Open`/`Closed` as today.
- **destroy**: drops all subscriptions, discarding buffered messages.
- **destroy_take**: there is no single canonical pending set (each
  subscriber holds its own copy), so on a broadcasting queue it destroys and
  returns an empty `Vec`. Documented.
- **Registry**: `Entry` is unchanged; `acquire_*` first tries downcasting to
  `ChannelCore<T>`, then `BroadcastCore<T>`, else `TypeMismatch`. A
  broadcasting queue and a work-sharing queue share one namespace.

### Error handling

Reuses the existing `QueueError` / `SendError` / `TrySendError` /
`RecvError` / `TryRecvError` types with identical meanings.

### Testing

New `tests/broadcast.rs`:

- create/acquire happy path; every subscriber sees every message.
- Subscription starts from now: messages sent before `acquire_receiver` are
  not delivered.
- Lossy drop-oldest: overfill a subscriber's buffer, verify it receives the
  newest `capacity` messages; a fast co-subscriber receives all.
- Zero subscribers: send succeeds, message discarded.
- Clones compete; second `acquire_receiver` is independent.
- Shutdown: sends fail, subscribers drain then get `Closed`; late
  `acquire_receiver` fails with `Shutdown`.
- `state` pending = max lag; `destroy`; `TypeMismatch` between queue kinds;
  name collision between `create` and `create_broadcasting`.
- Async send/recv smoke test.

Existing tests must pass unchanged (the refactor to enums must not alter
work-sharing behavior).
