use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

use crate::channel::ChannelControl;

/// One subscriber's private buffer. The keeper `rx` clone serves drop-oldest
/// eviction and `pending()`; the subscriber holds its own clone.
struct Subscription<T> {
    id: u64,
    /// `None` once the queue is shut down: the subscriber drains what is
    /// buffered, then observes disconnect.
    tx: Option<flume::Sender<T>>,
    rx: flume::Receiver<T>,
}

/// Shared state for one named broadcasting queue. Every subscriber owns a
/// bounded buffer created empty at subscribe time, so it sees only messages
/// sent from then on. Delivery is lossy per subscriber: a full buffer loses
/// its oldest message to make room, and senders never block.
pub(crate) struct BroadcastCore<T: Send + 'static> {
    pub(crate) name: String,
    /// Per-subscriber buffer capacity (at least 1).
    capacity: usize,
    /// Captured where `T: Clone` is known, so handle types stay bound-free.
    clone_fn: fn(&T) -> T,
    shutdown: AtomicBool,
    subs: RwLock<Vec<Subscription<T>>>,
    next_id: AtomicU64,
}

impl<T: Send + 'static> BroadcastCore<T> {
    pub(crate) fn new(name: &str, capacity: usize, clone_fn: fn(&T) -> T) -> Self {
        BroadcastCore {
            name: name.to_string(),
            capacity: capacity.max(1),
            clone_fn,
            shutdown: AtomicBool::new(false),
            subs: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(0),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Deliver a copy of `msg` to every live subscriber. A full subscriber
    /// loses its oldest buffered message; if eviction keeps losing the race
    /// against concurrent sends, the message is dropped for that subscriber
    /// (delivery is lossy by contract). Never blocks.
    pub(crate) fn broadcast(&self, msg: &T) {
        let subs = self.subs.read().unwrap();
        for sub in subs.iter() {
            let Some(tx) = &sub.tx else { continue };
            let mut m = (self.clone_fn)(msg);
            for _ in 0..8 {
                match tx.try_send(m) {
                    Ok(()) => break,
                    Err(flume::TrySendError::Full(back)) => {
                        let _ = sub.rx.try_recv();
                        m = back;
                    }
                    Err(flume::TrySendError::Disconnected(_)) => break,
                }
            }
        }
    }

    /// Register a new subscriber and hand back its (empty) buffer.
    pub(crate) fn subscribe(&self) -> (flume::Receiver<T>, u64) {
        let (tx, rx) = flume::bounded(self.capacity);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.subs.write().unwrap().push(Subscription {
            id,
            tx: Some(tx),
            rx: rx.clone(),
        });
        (rx, id)
    }

    /// Retire one subscription; called when its last receiver clone drops.
    pub(crate) fn unsubscribe(&self, id: u64) {
        self.subs.write().unwrap().retain(|s| s.id != id);
    }
}

impl<T: Send + 'static> ChannelControl for BroadcastCore<T> {
    fn shutdown(&self) -> bool {
        self.shutdown.store(true, Ordering::Release);
        let mut subs = self.subs.write().unwrap();
        for sub in subs.iter_mut() {
            sub.tx = None;
        }
        subs.iter().all(|s| s.rx.is_empty())
    }

    fn is_shutdown(&self) -> bool {
        BroadcastCore::is_shutdown(self)
    }

    fn pending(&self) -> usize {
        let subs = self.subs.read().unwrap();
        subs.iter().map(|s| s.rx.len()).max().unwrap_or(0)
    }

    fn destroy(&self) {
        self.shutdown.store(true, Ordering::Release);
        let mut subs = self.subs.write().unwrap();
        for sub in subs.drain(..) {
            drop(sub.tx);
            while sub.rx.try_recv().is_ok() {}
        }
    }
}
