//! Named, typed, bounded MPMC queues over [flume], with a graceful
//! shutdown lifecycle.
//!
//! - `create::<T>(name, capacity)` registers a bounded queue.
//! - Any number of [`Sender`]s and [`Receiver`]s share that queue
//!   (work-sharing: each message is delivered to exactly one receiver).
//! - `shutdown(name)` stops new senders and new sends; receivers, existing
//!   or acquired while messages remain, drain the queue, which is retired
//!   once the last message is consumed. The name is then reusable.
//! - `state(name)` probes a queue (`Open`/`Closed` with the pending count);
//!   `destroy(name)` force-retires one, discarding messages.
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

mod bus;
mod channel;
mod error;
mod receiver;
mod sender;

pub use bus::{MessageBus, QueueState};
pub use error::{BusError, RecvError, SendError, TryRecvError, TrySendError};
pub use receiver::Receiver;
pub use sender::Sender;
