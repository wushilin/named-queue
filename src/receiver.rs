use std::sync::Arc;

use crate::broadcast::BroadcastCore;
use crate::channel::{ChannelControl, ChannelCore};
use crate::error::{RecvError, TryRecvError};
use crate::registry::QueueRegistry;

/// Shared ownership of one broadcast subscription. When the last receiver
/// clone drops, the subscription is retired so senders stop copying
/// messages into its buffer.
pub(crate) struct SubGuard<T: Send + 'static> {
    core: Arc<BroadcastCore<T>>,
    id: u64,
    registry: QueueRegistry,
}

impl<T: Send + 'static> Drop for SubGuard<T> {
    fn drop(&mut self) {
        self.core.unsubscribe(self.id);
        if self.core.is_shutdown() && self.core.pending() == 0 {
            self.registry.remove_broadcast_core(&self.core);
        }
    }
}

enum ReceiverKind<T: Send + 'static> {
    /// Work-sharing queue: all receivers drain the one shared buffer.
    Queue { core: Arc<ChannelCore<T>> },
    /// Broadcasting queue: this receiver (and its clones) drain one
    /// subscription's private buffer.
    Broadcast { guard: Arc<SubGuard<T>> },
}

/// A consumer handle for one named queue. Clones compete for messages: on a
/// work-sharing queue they share the queue's single buffer, on a
/// broadcasting queue they share one subscription's buffer. To receive an
/// independent copy of the broadcast stream, call `acquire_receiver` again
/// instead of cloning.
pub struct Receiver<T: Send + 'static> {
    rx: flume::Receiver<T>,
    kind: ReceiverKind<T>,
    registry: QueueRegistry,
}

impl<T: Send + 'static> Receiver<T> {
    pub(crate) fn new(
        rx: flume::Receiver<T>,
        core: Arc<ChannelCore<T>>,
        registry: QueueRegistry,
    ) -> Self {
        Receiver {
            rx,
            kind: ReceiverKind::Queue { core },
            registry,
        }
    }

    pub(crate) fn new_broadcast(
        rx: flume::Receiver<T>,
        core: Arc<BroadcastCore<T>>,
        id: u64,
        registry: QueueRegistry,
    ) -> Self {
        let guard = Arc::new(SubGuard {
            core,
            id,
            registry: registry.clone(),
        });
        Receiver {
            rx,
            kind: ReceiverKind::Broadcast { guard },
            registry,
        }
    }

    /// Receive a message, blocking while the queue is empty.
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.rx.recv() {
            Ok(msg) => Ok(msg),
            Err(flume::RecvError::Disconnected) => {
                self.on_disconnect();
                Err(RecvError::Closed)
            }
        }
    }

    /// Async receive: awaits while the queue is empty. Executor-agnostic.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        match self.rx.recv_async().await {
            Ok(msg) => Ok(msg),
            Err(flume::RecvError::Disconnected) => {
                self.on_disconnect();
                Err(RecvError::Closed)
            }
        }
    }

    /// Probe-receive: never blocks.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.rx.try_recv() {
            Ok(msg) => Ok(msg),
            Err(flume::TryRecvError::Empty) => Err(TryRecvError::WouldBlock),
            Err(flume::TryRecvError::Disconnected) => {
                self.on_disconnect();
                Err(TryRecvError::Closed)
            }
        }
    }

    /// The queue name this receiver drains.
    pub fn name(&self) -> &str {
        match &self.kind {
            ReceiverKind::Queue { core } => &core.name,
            ReceiverKind::Broadcast { guard } => &guard.core.name,
        }
    }

    /// Retire the registry entry once the queue is shut down and drained.
    fn on_disconnect(&self) {
        match &self.kind {
            ReceiverKind::Queue { core } => self.registry.remove_core(core),
            ReceiverKind::Broadcast { guard } => {
                if guard.core.pending() == 0 {
                    self.registry.remove_broadcast_core(&guard.core);
                }
            }
        }
    }
}

impl<T: Send + 'static> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            rx: self.rx.clone(),
            kind: match &self.kind {
                ReceiverKind::Queue { core } => ReceiverKind::Queue { core: core.clone() },
                ReceiverKind::Broadcast { guard } => ReceiverKind::Broadcast {
                    guard: guard.clone(),
                },
            },
            registry: self.registry.clone(),
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Broadcast cleanup lives in `SubGuard::drop`, which runs when the
        // last clone sharing the subscription goes away.
        if let ReceiverKind::Queue { core } = &self.kind {
            if core.is_shutdown() && self.rx.is_empty() {
                self.registry.remove_core(core);
            }
        }
    }
}
