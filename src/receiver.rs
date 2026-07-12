use std::sync::Arc;

use crate::channel::ChannelCore;
use crate::error::{RecvError, TryRecvError};
use crate::registry::QueueRegistry;

/// A consumer handle for one named queue. Clones compete for messages
/// (work-sharing, not broadcast).
pub struct Receiver<T: Send + 'static> {
    rx: flume::Receiver<T>,
    core: Arc<ChannelCore<T>>,
    registry: QueueRegistry,
}

impl<T: Send + 'static> Receiver<T> {
    pub(crate) fn new(
        rx: flume::Receiver<T>,
        core: Arc<ChannelCore<T>>,
        registry: QueueRegistry,
    ) -> Self {
        Receiver { rx, core, registry }
    }

    /// Receive a message, blocking while the queue is empty.
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.rx.recv() {
            Ok(msg) => Ok(msg),
            Err(flume::RecvError::Disconnected) => {
                self.registry.remove_core(&self.core);
                Err(RecvError::Closed)
            }
        }
    }

    /// Async receive: awaits while the queue is empty. Executor-agnostic.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        match self.rx.recv_async().await {
            Ok(msg) => Ok(msg),
            Err(flume::RecvError::Disconnected) => {
                self.registry.remove_core(&self.core);
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
                self.registry.remove_core(&self.core);
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
            registry: self.registry.clone(),
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.core.is_shutdown() && self.rx.is_empty() {
            self.registry.remove_core(&self.core);
        }
    }
}
