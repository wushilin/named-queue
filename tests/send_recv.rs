use std::collections::HashSet;

use msgbus::{BusError, MessageBus, TryRecvError, TrySendError};

#[test]
fn roundtrip() {
    let bus = MessageBus::new();
    bus.create::<String>("chat", 8).unwrap();
    let tx = bus.acquire_sender::<String>("chat").unwrap();
    let rx = bus.acquire_receiver::<String>("chat").unwrap();
    tx.send("hi".to_string()).unwrap();
    assert_eq!(rx.recv().unwrap(), "hi");
    assert_eq!(tx.name(), "chat");
    assert_eq!(rx.name(), "chat");
}

#[test]
fn acquire_on_missing_queue_is_no_such_queue() {
    let bus = MessageBus::new();
    assert_eq!(
        bus.acquire_sender::<u8>("nope").err(),
        Some(BusError::NoSuchQueue("nope".into()))
    );
    assert_eq!(
        bus.acquire_receiver::<u8>("nope").err(),
        Some(BusError::NoSuchQueue("nope".into()))
    );
}

#[test]
fn acquire_with_wrong_type_is_type_mismatch() {
    let bus = MessageBus::new();
    bus.create::<String>("events", 4).unwrap();
    match bus.acquire_sender::<u64>("events") {
        Err(BusError::TypeMismatch { name, .. }) => assert_eq!(name, "events"),
        other => panic!("expected TypeMismatch, got {:?}", other.err()),
    }
    match bus.acquire_receiver::<u64>("events") {
        Err(BusError::TypeMismatch { name, .. }) => assert_eq!(name, "events"),
        other => panic!("expected TypeMismatch, got {:?}", other.err()),
    }
}

#[test]
fn try_send_would_block_when_full_and_try_recv_would_block_when_empty() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 2).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();

    assert_eq!(rx.try_recv(), Err(TryRecvError::WouldBlock));
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.try_send(3), Err(TrySendError::WouldBlock(3)));
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(tx.try_send(3), Ok(()));
}

#[test]
fn clones_share_one_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("shared", 8).unwrap();
    let tx1 = bus.acquire_sender::<u32>("shared").unwrap();
    let tx2 = tx1.clone();
    let rx1 = bus.acquire_receiver::<u32>("shared").unwrap();
    let rx2 = rx1.clone();

    tx1.send(1).unwrap();
    tx2.send(2).unwrap();
    let got: HashSet<u32> = [rx1.recv().unwrap(), rx2.recv().unwrap()].into();
    assert_eq!(got, HashSet::from([1, 2]));
}

#[test]
fn messages_do_not_require_clone_or_debug() {
    struct Opaque(Vec<u8>);
    let bus = MessageBus::new();
    bus.create::<Opaque>("blobs", 4).unwrap();
    let tx = bus.acquire_sender::<Opaque>("blobs").unwrap();
    let rx = bus.acquire_receiver::<Opaque>("blobs").unwrap();
    assert!(tx.send(Opaque(vec![1, 2, 3])).is_ok());
    assert_eq!(rx.recv().ok().map(|o| o.0), Some(vec![1, 2, 3]));
}
