use std::sync::Arc;

use crate::broadcast::BroadcastCore;
use crate::channel::ChannelCore;
use crate::error::{SendError, TrySendError};

enum SenderKind<T: Send + 'static> {
    /// Work-sharing queue: sends block (or fail on `try_send`) while full.
    Queue(Arc<ChannelCore<T>>),
    /// Broadcasting queue: sends fan out to every subscriber and never block;
    /// a full subscriber loses its oldest message instead.
    Broadcast(Arc<BroadcastCore<T>>),
}

/// A producer handle for one named queue. Clone freely; all clones feed the
/// same queue. Every send fails with `Closed` once the queue is shut down.
///
/// On a work-sharing queue each message goes to one receiver and a full
/// buffer applies backpressure. On a broadcasting queue each message is
/// copied to every current subscriber, sends never block, and slow
/// subscribers lose their oldest buffered messages instead.
pub struct Sender<T: Send + 'static> {
    kind: SenderKind<T>,
}

impl<T: Send + 'static> Sender<T> {
    pub(crate) fn new(core: Arc<ChannelCore<T>>) -> Self {
        Sender {
            kind: SenderKind::Queue(core),
        }
    }

    pub(crate) fn new_broadcast(core: Arc<BroadcastCore<T>>) -> Self {
        Sender {
            kind: SenderKind::Broadcast(core),
        }
    }

    /// Send a message, blocking while the buffer is full. Broadcasting
    /// queues never block: the message is copied to every subscriber
    /// immediately (dropping each full subscriber's oldest message).
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match &self.kind {
            SenderKind::Queue(core) => match core.tx.load_full() {
                Some(tx) => tx
                    .send(msg)
                    .map_err(|flume::SendError(m)| SendError::Closed(m)),
                None => Err(SendError::Closed(msg)),
            },
            SenderKind::Broadcast(core) => {
                if core.is_shutdown() {
                    return Err(SendError::Closed(msg));
                }
                core.broadcast(&msg);
                Ok(())
            }
        }
    }

    /// Async send: awaits while the buffer is full. Executor-agnostic.
    /// Broadcasting queues complete immediately (see [`send`](Self::send)).
    pub async fn send_async(&self, msg: T) -> Result<(), SendError<T>> {
        match &self.kind {
            SenderKind::Queue(core) => match core.tx.load_full() {
                Some(tx) => tx
                    .send_async(msg)
                    .await
                    .map_err(|flume::SendError(m)| SendError::Closed(m)),
                None => Err(SendError::Closed(msg)),
            },
            SenderKind::Broadcast(_) => self.send(msg),
        }
    }

    /// Probe-send: never blocks. Broadcasting queues never report
    /// `WouldBlock`; a send either succeeds or the queue is closed.
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        match &self.kind {
            SenderKind::Queue(core) => match core.tx.load_full() {
                Some(tx) => tx.try_send(msg).map_err(|e| match e {
                    flume::TrySendError::Full(m) => TrySendError::WouldBlock(m),
                    flume::TrySendError::Disconnected(m) => TrySendError::Closed(m),
                }),
                None => Err(TrySendError::Closed(msg)),
            },
            SenderKind::Broadcast(core) => {
                if core.is_shutdown() {
                    return Err(TrySendError::Closed(msg));
                }
                core.broadcast(&msg);
                Ok(())
            }
        }
    }

    /// The queue name this sender feeds.
    pub fn name(&self) -> &str {
        match &self.kind {
            SenderKind::Queue(core) => &core.name,
            SenderKind::Broadcast(core) => &core.name,
        }
    }
}

impl<T: Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            kind: match &self.kind {
                SenderKind::Queue(core) => SenderKind::Queue(core.clone()),
                SenderKind::Broadcast(core) => SenderKind::Broadcast(core.clone()),
            },
        }
    }
}
