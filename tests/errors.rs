use msgbus::{BusError, RecvError, SendError, TryRecvError, TrySendError};

#[test]
fn bus_error_display() {
    assert_eq!(
        BusError::QueueAlreadyExists("a".into()).to_string(),
        "queue `a` already exists"
    );
    assert_eq!(
        BusError::NoSuchQueue("b".into()).to_string(),
        "no such queue `b`"
    );
    assert_eq!(
        BusError::ShutDown("c".into()).to_string(),
        "queue `c` is shut down"
    );
    let e = BusError::TypeMismatch {
        name: "d".into(),
        expected: "i32",
        actual: "u8",
    };
    assert_eq!(e.to_string(), "queue `d` carries `u8`, not `i32`");
}

#[test]
fn channel_error_display_and_message_recovery() {
    assert_eq!(
        SendError::Closed(7).to_string(),
        "sending on a closed queue"
    );
    assert_eq!(SendError::Closed(7).into_inner(), 7);
    assert_eq!(
        TrySendError::WouldBlock(7).to_string(),
        "queue is full; send would block"
    );
    assert_eq!(
        TrySendError::Closed(7).to_string(),
        "sending on a closed queue"
    );
    assert_eq!(TrySendError::WouldBlock(7).into_inner(), 7);
    assert_eq!(TrySendError::Closed(7).into_inner(), 7);
    assert_eq!(
        RecvError::Closed.to_string(),
        "queue is shut down and drained"
    );
    assert_eq!(
        TryRecvError::WouldBlock.to_string(),
        "queue is empty; recv would block"
    );
    assert_eq!(
        TryRecvError::Closed.to_string(),
        "queue is shut down and drained"
    );
}
