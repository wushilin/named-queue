use std::error::Error;
use std::fmt;

/// Errors from registry lifecycle operations: `create`, `acquire_*`, `shutdown`,
/// `state`, `destroy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueError {
    /// `create` was called with a name that is already registered
    /// (including a queue that is shut down but not yet drained).
    QueueAlreadyExists(String),
    /// The named queue does not exist (never created, or already retired).
    NoSuchQueue(String),
    /// The queue exists but carries a different message type.
    TypeMismatch {
        name: String,
        expected: &'static str,
        actual: &'static str,
    },
    /// The queue is shut down. For `acquire_receiver` this means it is also
    /// already drained; a closed queue with pending messages still admits
    /// receivers.
    Shutdown(String),
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::QueueAlreadyExists(name) => write!(f, "queue `{name}` already exists"),
            QueueError::NoSuchQueue(name) => write!(f, "no such queue `{name}`"),
            QueueError::TypeMismatch {
                name,
                expected,
                actual,
            } => write!(f, "queue `{name}` carries `{actual}`, not `{expected}`"),
            QueueError::Shutdown(name) => write!(f, "queue `{name}` is shut down"),
        }
    }
}

impl Error for QueueError {}

/// The queue is shut down; the unsent message is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError<T> {
    Closed(T),
}

impl<T> SendError<T> {
    /// Recover the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            SendError::Closed(msg) => msg,
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed queue")
    }
}

impl<T: fmt::Debug> Error for SendError<T> {}

/// Outcome of a non-blocking send probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The buffer is full; a blocking `send` would wait. Message handed back.
    WouldBlock(T),
    /// The queue is shut down. Message handed back.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Recover the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::WouldBlock(msg) | TrySendError::Closed(msg) => msg,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::WouldBlock(_) => f.write_str("queue is full; send would block"),
            TrySendError::Closed(_) => f.write_str("sending on a closed queue"),
        }
    }
}

impl<T: fmt::Debug> Error for TrySendError<T> {}

/// The queue is shut down and every remaining message has been consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    Closed,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("queue is shut down and drained")
    }
}

impl Error for RecvError {}

/// Outcome of a non-blocking receive probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The queue is currently empty; a blocking `recv` would wait.
    WouldBlock,
    /// The queue is shut down and drained.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::WouldBlock => f.write_str("queue is empty; recv would block"),
            TryRecvError::Closed => f.write_str("queue is shut down and drained"),
        }
    }
}

impl Error for TryRecvError {}
