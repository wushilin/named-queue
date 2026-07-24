use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::broadcast::BroadcastCore;
use crate::channel::{ChannelControl, ChannelCore};
use crate::error::QueueError;
use crate::receiver::Receiver;
use crate::sender::Sender;

/// A registered queue's typed core, either kind.
enum AnyCore<T: Send + 'static> {
    Queue(Arc<ChannelCore<T>>),
    Broadcast(Arc<BroadcastCore<T>>),
}

pub(crate) struct Entry {
    /// The `Arc<ChannelCore<T>>`, kept as `dyn Any` for typed downcasts.
    pub(crate) any: Arc<dyn Any + Send + Sync>,
    /// The same core, viewed type-erased for shutdown/state/destroy.
    pub(crate) control: Arc<dyn ChannelControl>,
    /// For readable `TypeMismatch` errors.
    pub(crate) type_name: &'static str,
}

/// Snapshot of one queue's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueState {
    /// Accepting sends and acquires; `pending` messages currently buffered.
    Open { pending: usize },
    /// Shut down, still draining; `pending` messages remain consumable.
    Closed { pending: usize },
}

/// A registry of named, typed, bounded work-sharing queues. Cheap to clone; clones
/// share the same registry. Send/recv never touch the registry lock.
#[derive(Clone, Default)]
pub struct QueueRegistry {
    map: Arc<RwLock<HashMap<String, Entry>>>,
}

impl QueueRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new bounded queue carrying `T`.
    pub fn create<T: Send + 'static>(&self, name: &str, capacity: usize) -> Result<(), QueueError> {
        let mut map = self.map.write().unwrap();
        if map.contains_key(name) {
            return Err(QueueError::QueueAlreadyExists(name.to_string()));
        }
        let core = Arc::new(ChannelCore::<T>::new(name, capacity));
        map.insert(
            name.to_string(),
            Entry {
                any: core.clone(),
                control: core,
                type_name: std::any::type_name::<T>(),
            },
        );
        Ok(())
    }

    /// Register a new broadcasting queue carrying `T`. Every subscriber gets
    /// its own bounded buffer of `capacity` (at least 1), created empty at
    /// subscribe time, so it sees only messages sent from then on. Delivery
    /// is lossy per subscriber: a full buffer loses its oldest message, and
    /// senders never block.
    pub fn create_broadcasting<T: Send + Clone + 'static>(
        &self,
        name: &str,
        capacity: usize,
    ) -> Result<(), QueueError> {
        let mut map = self.map.write().unwrap();
        if map.contains_key(name) {
            return Err(QueueError::QueueAlreadyExists(name.to_string()));
        }
        let core = Arc::new(BroadcastCore::<T>::new(name, capacity, |t| t.clone()));
        map.insert(
            name.to_string(),
            Entry {
                any: core.clone(),
                control: core,
                type_name: std::any::type_name::<T>(),
            },
        );
        Ok(())
    }

    /// Get a producer handle.
    pub fn acquire_sender<T: Send + 'static>(&self, name: &str) -> Result<Sender<T>, QueueError> {
        let map = self.map.read().unwrap();
        match Self::core_of::<T>(&map, name)? {
            AnyCore::Queue(core) => {
                if core.is_shutdown() {
                    return Err(QueueError::Shutdown(name.to_string()));
                }
                Ok(Sender::new(core))
            }
            AnyCore::Broadcast(core) => {
                if core.is_shutdown() {
                    return Err(QueueError::Shutdown(name.to_string()));
                }
                Ok(Sender::new_broadcast(core))
            }
        }
    }

    /// Get a consumer handle. A closed work-sharing queue admits new
    /// receivers while messages remain to be drained. On a broadcasting
    /// queue this registers a fresh subscription that receives messages
    /// from now on; a shut-down broadcasting queue admits no new
    /// subscribers (a new subscription could never hold anything to drain).
    pub fn acquire_receiver<T: Send + 'static>(
        &self,
        name: &str,
    ) -> Result<Receiver<T>, QueueError> {
        let map = self.map.read().unwrap();
        match Self::core_of::<T>(&map, name)? {
            AnyCore::Queue(core) => {
                let rx = core
                    .rx
                    .load_full()
                    .ok_or_else(|| QueueError::Shutdown(name.to_string()))?;
                if core.is_shutdown() && rx.is_empty() {
                    return Err(QueueError::Shutdown(name.to_string()));
                }
                Ok(Receiver::new((*rx).clone(), core, self.clone()))
            }
            AnyCore::Broadcast(core) => {
                if core.is_shutdown() {
                    return Err(QueueError::Shutdown(name.to_string()));
                }
                let (rx, id) = core.subscribe();
                Ok(Receiver::new_broadcast(rx, core, id, self.clone()))
            }
        }
    }

    /// Shut the named queue down: no new senders are issued, and sends that
    /// observe shutdown fail. Existing receivers and new ones acquired while
    /// messages remain can drain the queue.
    pub fn shutdown(&self, name: &str) -> Result<(), QueueError> {
        let mut map = self.map.write().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| QueueError::NoSuchQueue(name.to_string()))?;
        if entry.control.is_shutdown() {
            return Err(QueueError::Shutdown(name.to_string()));
        }
        if entry.control.shutdown() {
            map.remove(name);
        }
        Ok(())
    }

    /// Probe a queue's lifecycle state.
    pub fn state(&self, name: &str) -> Result<QueueState, QueueError> {
        let map = self.map.read().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| QueueError::NoSuchQueue(name.to_string()))?;
        let pending = entry.control.pending();
        Ok(if entry.control.is_shutdown() {
            QueueState::Closed { pending }
        } else {
            QueueState::Open { pending }
        })
    }

    /// Force-retire the registry entry in any state, discarding pending messages.
    pub fn destroy(&self, name: &str) -> Result<(), QueueError> {
        let mut map = self.map.write().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| QueueError::NoSuchQueue(name.to_string()))?;
        entry.control.destroy();
        map.remove(name);
        Ok(())
    }

    /// Typed force-retire: like [`destroy`](Self::destroy), but hands the
    /// unconsumed messages back to the caller instead of discarding them.
    ///
    /// On a broadcasting queue there is no canonical pending set (every
    /// subscriber holds its own copy), so the queue is destroyed and an
    /// empty `Vec` is returned.
    pub fn destroy_take<T: Send + 'static>(&self, name: &str) -> Result<Vec<T>, QueueError> {
        let mut map = self.map.write().unwrap();
        let mut taken = Vec::new();
        match Self::core_of::<T>(&map, name)? {
            AnyCore::Queue(core) => {
                core.tx.store(None);
                if let Some(rx) = core.rx.load_full() {
                    while let Ok(msg) = rx.try_recv() {
                        taken.push(msg);
                    }
                }
                core.rx.store(None);
            }
            AnyCore::Broadcast(core) => {
                ChannelControl::destroy(&*core);
            }
        }
        map.remove(name);
        Ok(taken)
    }

    fn core_of<T: Send + 'static>(
        map: &HashMap<String, Entry>,
        name: &str,
    ) -> Result<AnyCore<T>, QueueError> {
        let entry = map
            .get(name)
            .ok_or_else(|| QueueError::NoSuchQueue(name.to_string()))?;
        if let Ok(core) = entry.any.clone().downcast::<ChannelCore<T>>() {
            return Ok(AnyCore::Queue(core));
        }
        if let Ok(core) = entry.any.clone().downcast::<BroadcastCore<T>>() {
            return Ok(AnyCore::Broadcast(core));
        }
        Err(QueueError::TypeMismatch {
            name: name.to_string(),
            expected: std::any::type_name::<T>(),
            actual: entry.type_name,
        })
    }

    /// Retire `core`'s entry if it is still the one registered under its name.
    pub(crate) fn remove_core<T: Send + 'static>(&self, core: &Arc<ChannelCore<T>>) {
        let mut map = self.map.write().unwrap();
        let Some(entry) = map.get(&core.name) else {
            return;
        };
        let Ok(existing) = entry.any.clone().downcast::<ChannelCore<T>>() else {
            return;
        };
        if Arc::ptr_eq(&existing, core) {
            map.remove(&core.name);
        }
    }

    /// Retire `core`'s entry if it is still the one registered under its name.
    pub(crate) fn remove_broadcast_core<T: Send + 'static>(&self, core: &Arc<BroadcastCore<T>>) {
        let mut map = self.map.write().unwrap();
        let Some(entry) = map.get(&core.name) else {
            return;
        };
        let Ok(existing) = entry.any.clone().downcast::<BroadcastCore<T>>() else {
            return;
        };
        if Arc::ptr_eq(&existing, core) {
            map.remove(&core.name);
        }
    }
}
