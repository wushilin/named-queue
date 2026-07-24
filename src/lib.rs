//! Named, typed, bounded work-sharing queues over [flume], with a graceful
//! shutdown lifecycle.
//!
//! This crate is an in-process queue registry, not a broker or pub/sub system.
//! Queues are addressed by name and message type; cloned receivers compete for
//! work, so each message is delivered to one receiver. Use `QueueRegistry::new()`
//! for explicit ownership, or the module-level functions for a process-wide
//! default registry.
//!
//! - `create::<T>(name, capacity)` registers a bounded queue.
//! - Any number of [`Sender`]s and [`Receiver`]s share that queue; receivers
//!   work-share rather than broadcast.
//! - `create_broadcasting::<T>(name, capacity)` registers a broadcasting
//!   queue instead, under the same acquire/shutdown/state contract: every
//!   `acquire_receiver` call starts a fresh subscription that sees messages
//!   sent from then on (no history), with its own buffer of `capacity`.
//!   Senders never block; a subscriber lagging more than `capacity` messages
//!   behind loses its oldest ones (per-subscriber drop-oldest). Cloning a
//!   broadcast `Receiver` shares its subscription — clones compete — so call
//!   `acquire_receiver` again for an independent subscriber.
//! - `shutdown(name)` revokes the queue's sender handle. Sends that observe
//!   shutdown fail, while receivers, existing or acquired while messages remain,
//!   can drain the queue.
//! - `state(name)` probes a queue (`Open`/`Closed` with the pending count);
//!   `destroy(name)` force-retires one, discarding messages.
//! - Send/recv never touch the registry lock; the hot path is pure flume
//!   plus one lock-free pointer load.
//!
//! ```
//! use named_queue::{QueueRegistry, QueueState};
//!
//! let registry = QueueRegistry::new();
//! registry.create::<String>("greetings", 16).unwrap();
//!
//! let tx = registry.acquire_sender::<String>("greetings").unwrap();
//! let rx = registry.acquire_receiver::<String>("greetings").unwrap();
//!
//! tx.send("hello".to_string()).unwrap();
//! assert_eq!(registry.state("greetings"), Ok(QueueState::Open { pending: 1 }));
//! assert_eq!(rx.recv().unwrap(), "hello");
//!
//! registry.shutdown("greetings").unwrap();
//! assert!(tx.send("too late".to_string()).is_err());
//! ```

mod broadcast;
mod channel;
mod error;
mod receiver;
mod registry;
mod sender;

pub use error::{QueueError, RecvError, SendError, TryRecvError, TrySendError};
pub use receiver::Receiver;
pub use registry::{QueueRegistry, QueueState};
pub use sender::Sender;

use std::sync::OnceLock;

static DEFAULT_REGISTRY: OnceLock<QueueRegistry> = OnceLock::new();

/// Return the process-wide default queue registry.
///
/// This is a convenience for applications that do not need multiple isolated
/// registries. Prefer QueueRegistry::new when ownership, test isolation, or
/// dependency injection matters.
pub fn default_registry() -> &'static QueueRegistry {
    DEFAULT_REGISTRY.get_or_init(QueueRegistry::new)
}

/// Register a queue in the process-wide default registry.
pub fn create<T: Send + 'static>(name: &str, capacity: usize) -> Result<(), QueueError> {
    default_registry().create::<T>(name, capacity)
}

/// Register a broadcasting queue in the process-wide default registry.
pub fn create_broadcasting<T: Send + Clone + 'static>(
    name: &str,
    capacity: usize,
) -> Result<(), QueueError> {
    default_registry().create_broadcasting::<T>(name, capacity)
}

/// Acquire a sender from the process-wide default registry.
pub fn acquire_sender<T: Send + 'static>(name: &str) -> Result<Sender<T>, QueueError> {
    default_registry().acquire_sender::<T>(name)
}

/// Acquire a receiver from the process-wide default registry.
pub fn acquire_receiver<T: Send + 'static>(name: &str) -> Result<Receiver<T>, QueueError> {
    default_registry().acquire_receiver::<T>(name)
}

/// Shut down a queue in the process-wide default registry.
pub fn shutdown(name: &str) -> Result<(), QueueError> {
    default_registry().shutdown(name)
}

/// Probe a queue in the process-wide default registry.
pub fn state(name: &str) -> Result<QueueState, QueueError> {
    default_registry().state(name)
}

/// Force-retire a queue in the process-wide default registry.
pub fn destroy(name: &str) -> Result<(), QueueError> {
    default_registry().destroy(name)
}

/// Force-retire a typed queue in the process-wide default registry, returning
/// unconsumed messages.
pub fn destroy_take<T: Send + 'static>(name: &str) -> Result<Vec<T>, QueueError> {
    default_registry().destroy_take::<T>(name)
}
