use std::thread;
use std::time::Duration;

use named_queue::{QueueError, QueueRegistry, RecvError, SendError, TrySendError};

#[test]
fn shutdown_unknown_name_is_no_such_queue() {
    let registry = QueueRegistry::new();
    assert_eq!(
        registry.shutdown("ghost"),
        Err(QueueError::NoSuchQueue("ghost".into()))
    );
}

#[test]
fn shutdown_cuts_senders_but_lets_receivers_drain() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 8).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    let rx = registry.acquire_receiver::<u32>("q").unwrap();
    tx.send(42).unwrap();

    assert_eq!(registry.shutdown("q"), Ok(()));
    assert_eq!(
        registry.acquire_sender::<u32>("q").err(),
        Some(QueueError::Shutdown("q".into()))
    );
    assert_eq!(
        registry.shutdown("q"),
        Err(QueueError::Shutdown("q".into()))
    );
    assert_eq!(tx.send(1), Err(SendError::Closed(1)));
    assert_eq!(tx.try_send(2), Err(TrySendError::Closed(2)));
    assert_eq!(rx.recv(), Ok(42));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(
        registry.acquire_receiver::<u32>("q").err(),
        Some(QueueError::NoSuchQueue("q".into()))
    );
}

#[test]
fn closed_queue_admits_new_receivers_while_messages_remain() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 8).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    let rx = registry.acquire_receiver::<u32>("q").unwrap();
    tx.send(7).unwrap();
    registry.shutdown("q").unwrap();

    let late = registry.acquire_receiver::<u32>("q").unwrap();
    assert_eq!(rx.recv(), Ok(7));
    assert_eq!(
        registry.acquire_receiver::<u32>("q").err(),
        Some(QueueError::Shutdown("q".into()))
    );
    assert_eq!(late.recv(), Err(RecvError::Closed));
    assert_eq!(registry.create::<u32>("q", 8), Ok(()));
}

#[test]
fn empty_queue_is_retired_immediately_on_shutdown() {
    let registry = QueueRegistry::new();
    registry.create::<u8>("q", 4).unwrap();
    assert_eq!(registry.shutdown("q"), Ok(()));
    assert_eq!(registry.create::<u8>("q", 4), Ok(()));

    let registry2 = QueueRegistry::new();
    registry2.create::<u8>("gone", 4).unwrap();
    registry2.shutdown("gone").unwrap();
    assert_eq!(
        registry2.shutdown("gone"),
        Err(QueueError::NoSuchQueue("gone".into()))
    );
}

#[test]
fn shutdown_with_no_receivers_retains_messages_for_a_late_drain() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 8).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(registry.shutdown("q"), Ok(()));
    assert_eq!(
        registry.create::<u32>("q", 8),
        Err(QueueError::QueueAlreadyExists("q".into()))
    );
    let rx = registry.acquire_receiver::<u32>("q").unwrap();
    assert_eq!(rx.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(registry.create::<u32>("q", 8), Ok(()));
}

#[test]
fn drained_receiver_drop_retires_the_queue_without_a_final_recv() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 4).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    let rx = registry.acquire_receiver::<u32>("q").unwrap();
    tx.send(1).unwrap();
    registry.shutdown("q").unwrap();

    assert_eq!(rx.recv(), Ok(1));
    drop(rx);
    assert_eq!(registry.create::<u32>("q", 4), Ok(()));
}

#[test]
fn blocked_receiver_wakes_with_closed_on_shutdown() {
    let registry = QueueRegistry::new();
    registry.create::<u8>("q", 4).unwrap();
    let rx = registry.acquire_receiver::<u8>("q").unwrap();

    let handle = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(100));
    registry.shutdown("q").unwrap();

    assert_eq!(handle.join().unwrap(), Err(RecvError::Closed));
}
