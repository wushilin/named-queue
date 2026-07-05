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
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self.core.tx.load_full() {
            Some(tx) => tx
                .send(msg)
                .map_err(|flume::SendError(m)| SendError::Closed(m)),
            None => Err(SendError::Closed(msg)),
        }
    }

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

    /// Probe-send: never blocks.
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
