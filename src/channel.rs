use arc_swap::ArcSwapOption;

/// Shared state for one named queue. Senders and receivers hold this via
/// `Arc`; it outlives the registry entry, so late drains still work after
/// the entry has been retired.
pub(crate) struct ChannelCore<T: Send + 'static> {
    pub(crate) name: String,
    /// The only long-lived flume sender. `shutdown` swaps it to `None`;
    /// once in-flight sends finish, flume disconnects and blocked receivers
    /// wake up after draining what is left.
    pub(crate) tx: ArcSwapOption<flume::Sender<T>>,
    /// Keeper receiver: holds queued messages while no receiver is around
    /// and serves as the template `acquire_receiver` clones from, also
    /// after shutdown, so late receivers can drain a closed queue.
    pub(crate) rx: ArcSwapOption<flume::Receiver<T>>,
}

impl<T: Send + 'static> ChannelCore<T> {
    pub(crate) fn new(name: &str, capacity: usize) -> Self {
        let (tx, rx) = flume::bounded(capacity);
        ChannelCore {
            name: name.to_string(),
            tx: ArcSwapOption::from_pointee(tx),
            rx: ArcSwapOption::from_pointee(rx),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.tx.load().is_none()
    }
}

/// Type-erased view of a `ChannelCore<T>` for operations that do not know `T`.
pub(crate) trait ChannelControl: Send + Sync {
    /// Revoke the sender. Returns `true` when the queue is already drained,
    /// meaning the registry entry can be retired immediately.
    fn shutdown(&self) -> bool;
    fn is_shutdown(&self) -> bool;
    /// Messages currently buffered (0 if the keeper is gone).
    fn pending(&self) -> usize;
    /// Force-teardown: revoke the sender, discard buffered messages, drop
    /// the keeper receiver. Caller must also remove the registry entry.
    fn destroy(&self);
}

impl<T: Send + 'static> ChannelControl for ChannelCore<T> {
    fn shutdown(&self) -> bool {
        self.tx.store(None);
        match self.rx.load().as_ref() {
            Some(rx) => rx.is_empty(),
            None => true,
        }
    }

    fn is_shutdown(&self) -> bool {
        ChannelCore::is_shutdown(self)
    }

    fn pending(&self) -> usize {
        self.rx.load().as_ref().map(|rx| rx.len()).unwrap_or(0)
    }

    fn destroy(&self) {
        self.tx.store(None);
        if let Some(rx) = self.rx.load_full() {
            while rx.try_recv().is_ok() {}
        }
        self.rx.store(None);
    }
}
