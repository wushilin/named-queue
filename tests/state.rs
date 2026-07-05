use std::thread;
use std::time::Duration;

use msgbus::{BusError, MessageBus, QueueState, RecvError};

#[test]
fn state_reports_open_with_pending_count() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    assert_eq!(bus.state("q"), Ok(QueueState::Open { pending: 0 }));

    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(bus.state("q"), Ok(QueueState::Open { pending: 2 }));
}

#[test]
fn state_reports_closed_with_remaining_then_no_such_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 4).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    let rx = bus.acquire_receiver::<u32>("q").unwrap();
    tx.send(9).unwrap();
    bus.shutdown("q").unwrap();

    assert_eq!(bus.state("q"), Ok(QueueState::Closed { pending: 1 }));
    assert_eq!(rx.recv(), Ok(9));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(bus.state("q"), Err(BusError::NoSuchQueue("q".into())));
}

#[test]
fn state_of_unknown_queue_is_no_such_queue() {
    let bus = MessageBus::new();
    assert_eq!(
        bus.state("ghost"),
        Err(BusError::NoSuchQueue("ghost".into()))
    );
}

#[test]
fn destroy_discards_pending_and_frees_the_name() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    bus.shutdown("q").unwrap();

    assert_eq!(bus.destroy("q"), Ok(()));
    assert_eq!(bus.state("q"), Err(BusError::NoSuchQueue("q".into())));
    assert_eq!(bus.destroy("q"), Err(BusError::NoSuchQueue("q".into())));
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
}

#[test]
fn destroy_take_returns_unconsumed_messages() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    let tx = bus.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    bus.shutdown("q").unwrap();

    assert_eq!(bus.destroy_take::<u32>("q"), Ok(vec![1, 2]));
    assert_eq!(bus.state("q"), Err(BusError::NoSuchQueue("q".into())));
    assert_eq!(bus.create::<u32>("q", 8), Ok(()));
    assert_eq!(bus.destroy_take::<u32>("q"), Ok(vec![]));
    assert_eq!(
        bus.destroy_take::<u32>("q"),
        Err(BusError::NoSuchQueue("q".into()))
    );
}

#[test]
fn destroy_take_with_wrong_type_is_type_mismatch_and_keeps_the_queue() {
    let bus = MessageBus::new();
    bus.create::<u32>("q", 8).unwrap();
    match bus.destroy_take::<String>("q") {
        Err(BusError::TypeMismatch { name, .. }) => assert_eq!(name, "q"),
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
    assert_eq!(bus.state("q"), Ok(QueueState::Open { pending: 0 }));
}

#[test]
fn destroy_works_on_open_queues_and_wakes_blocked_receivers() {
    let bus = MessageBus::new();
    bus.create::<u8>("q", 4).unwrap();
    let rx = bus.acquire_receiver::<u8>("q").unwrap();

    let handle = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(100));
    assert_eq!(bus.destroy("q"), Ok(()));

    assert_eq!(handle.join().unwrap(), Err(RecvError::Closed));
    assert_eq!(bus.create::<u8>("q", 4), Ok(()));
}
