use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::channel::{ChannelControl, ChannelCore};
use crate::error::BusError;
use crate::receiver::Receiver;
use crate::sender::Sender;

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

/// A registry of named, typed, bounded MPMC queues. Cheap to clone; clones
/// share the same registry. Send/recv never touch the registry lock.
#[derive(Clone, Default)]
pub struct MessageBus {
    map: Arc<RwLock<HashMap<String, Entry>>>,
}

impl MessageBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new bounded queue carrying `T`.
    pub fn create<T: Send + 'static>(&self, name: &str, capacity: usize) -> Result<(), BusError> {
        let mut map = self.map.write().unwrap();
        if map.contains_key(name) {
            return Err(BusError::QueueAlreadyExists(name.to_string()));
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

    /// Get a producer handle.
    pub fn acquire_sender<T: Send + 'static>(&self, name: &str) -> Result<Sender<T>, BusError> {
        let map = self.map.read().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        if core.is_shutdown() {
            return Err(BusError::ShutDown(name.to_string()));
        }
        Ok(Sender::new(core))
    }

    /// Get a consumer handle. A closed queue admits new receivers while
    /// messages remain to be drained.
    pub fn acquire_receiver<T: Send + 'static>(&self, name: &str) -> Result<Receiver<T>, BusError> {
        let map = self.map.read().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        let rx = core
            .rx
            .load_full()
            .ok_or_else(|| BusError::ShutDown(name.to_string()))?;
        if core.is_shutdown() && rx.is_empty() {
            return Err(BusError::ShutDown(name.to_string()));
        }
        Ok(Receiver::new((*rx).clone(), core, self.clone()))
    }

    /// Shut the named queue down: no new senders, all sends fail. Existing
    /// receivers and new ones acquired while messages remain drain the queue.
    pub fn shutdown(&self, name: &str) -> Result<(), BusError> {
        let mut map = self.map.write().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        if entry.control.is_shutdown() {
            return Err(BusError::ShutDown(name.to_string()));
        }
        if entry.control.shutdown() {
            map.remove(name);
        }
        Ok(())
    }

    /// Probe a queue's lifecycle state.
    pub fn state(&self, name: &str) -> Result<QueueState, BusError> {
        let map = self.map.read().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        let pending = entry.control.pending();
        Ok(if entry.control.is_shutdown() {
            QueueState::Closed { pending }
        } else {
            QueueState::Open { pending }
        })
    }

    /// Force-retire a queue in any state, discarding pending messages.
    pub fn destroy(&self, name: &str) -> Result<(), BusError> {
        let mut map = self.map.write().unwrap();
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        entry.control.destroy();
        map.remove(name);
        Ok(())
    }

    /// Typed force-retire: like [`destroy`](Self::destroy), but hands the
    /// unconsumed messages back to the caller instead of discarding them.
    pub fn destroy_take<T: Send + 'static>(&self, name: &str) -> Result<Vec<T>, BusError> {
        let mut map = self.map.write().unwrap();
        let core = Self::core_of::<T>(&map, name)?;
        core.tx.store(None);
        let mut taken = Vec::new();
        if let Some(rx) = core.rx.load_full() {
            while let Ok(msg) = rx.try_recv() {
                taken.push(msg);
            }
        }
        core.rx.store(None);
        map.remove(name);
        Ok(taken)
    }

    fn core_of<T: Send + 'static>(
        map: &HashMap<String, Entry>,
        name: &str,
    ) -> Result<Arc<ChannelCore<T>>, BusError> {
        let entry = map
            .get(name)
            .ok_or_else(|| BusError::NoSuchQueue(name.to_string()))?;
        entry
            .any
            .clone()
            .downcast::<ChannelCore<T>>()
            .map_err(|_| BusError::TypeMismatch {
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
}
