use named_queue::{
    QueueError, QueueRegistry, QueueState, RecvError, SendError, TryRecvError, TrySendError,
};
use std::time::Duration;

#[test]
fn every_subscriber_sees_every_message() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<String>("news", 8).unwrap();

    let tx = registry.acquire_sender::<String>("news").unwrap();
    let a = registry.acquire_receiver::<String>("news").unwrap();
    let b = registry.acquire_receiver::<String>("news").unwrap();

    tx.send("one".to_string()).unwrap();
    tx.send("two".to_string()).unwrap();

    assert_eq!(a.recv().unwrap(), "one");
    assert_eq!(a.recv().unwrap(), "two");
    assert_eq!(b.recv().unwrap(), "one");
    assert_eq!(b.recv().unwrap(), "two");
}

#[test]
fn subscription_starts_from_now() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("ticks", 8).unwrap();
    let tx = registry.acquire_sender::<i32>("ticks").unwrap();

    tx.send(1).unwrap();
    let late = registry.acquire_receiver::<i32>("ticks").unwrap();
    tx.send(2).unwrap();

    assert_eq!(late.recv().unwrap(), 2);
    assert_eq!(late.try_recv(), Err(TryRecvError::WouldBlock));
}

#[test]
fn send_without_subscribers_succeeds_and_discards() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("void", 4).unwrap();
    let tx = registry.acquire_sender::<i32>("void").unwrap();

    tx.send(42).unwrap();
    tx.try_send(43).unwrap();

    let rx = registry.acquire_receiver::<i32>("void").unwrap();
    assert_eq!(rx.try_recv(), Err(TryRecvError::WouldBlock));
}

#[test]
fn slow_subscriber_loses_oldest_fast_one_gets_all() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("firehose", 2).unwrap();
    let tx = registry.acquire_sender::<i32>("firehose").unwrap();

    let slow = registry.acquire_receiver::<i32>("firehose").unwrap();
    let fast = registry.acquire_receiver::<i32>("firehose").unwrap();

    for i in 1..=5 {
        tx.send(i).unwrap();
        // fast keeps up
        assert_eq!(fast.recv().unwrap(), i);
    }

    // slow's buffer holds only the newest 2 (drop-oldest)
    assert_eq!(slow.recv().unwrap(), 4);
    assert_eq!(slow.recv().unwrap(), 5);
    assert_eq!(slow.try_recv(), Err(TryRecvError::WouldBlock));
}

#[test]
fn sender_never_blocks_on_full_subscriber() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("nb", 1).unwrap();
    let tx = registry.acquire_sender::<i32>("nb").unwrap();
    let _lagging = registry.acquire_receiver::<i32>("nb").unwrap();

    // With a work-sharing queue of capacity 1 this second send would block;
    // broadcasting must not.
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.try_send(3).unwrap();
}

#[test]
fn receiver_clones_compete_new_acquire_is_independent() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("mix", 8).unwrap();
    let tx = registry.acquire_sender::<i32>("mix").unwrap();

    let a = registry.acquire_receiver::<i32>("mix").unwrap();
    let a2 = a.clone(); // shares a's subscription
    let b = registry.acquire_receiver::<i32>("mix").unwrap(); // independent

    tx.send(7).unwrap();

    // Exactly one of {a, a2} gets the message.
    assert_eq!(a.try_recv().unwrap(), 7);
    assert_eq!(a2.try_recv(), Err(TryRecvError::WouldBlock));
    // b has its own copy.
    assert_eq!(b.try_recv().unwrap(), 7);
}

#[test]
fn dropped_subscriber_is_unsubscribed() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("churn", 2).unwrap();
    let tx = registry.acquire_sender::<i32>("churn").unwrap();

    let gone = registry.acquire_receiver::<i32>("churn").unwrap();
    drop(gone);
    tx.send(1).unwrap();
    assert_eq!(registry.state("churn"), Ok(QueueState::Open { pending: 0 }));

    let live = registry.acquire_receiver::<i32>("churn").unwrap();
    tx.send(2).unwrap();
    assert_eq!(live.recv().unwrap(), 2);
}

#[test]
fn shutdown_fails_sends_lets_subscribers_drain() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("closing", 8).unwrap();
    let tx = registry.acquire_sender::<i32>("closing").unwrap();
    let rx = registry.acquire_receiver::<i32>("closing").unwrap();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    registry.shutdown("closing").unwrap();

    assert_eq!(tx.send(3), Err(SendError::Closed(3)));
    assert_eq!(tx.try_send(4), Err(TrySendError::Closed(4)));

    // Unlike a closed work-sharing queue, a closed broadcasting queue never
    // admits new subscribers: a fresh subscription could hold nothing.
    assert_eq!(
        registry.acquire_receiver::<i32>("closing").err(),
        Some(QueueError::Shutdown("closing".to_string()))
    );

    // Buffered messages drain, then Closed; the drained entry is retired.
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(rx.recv().unwrap(), 2);
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(
        registry.acquire_receiver::<i32>("closing").err(),
        Some(QueueError::NoSuchQueue("closing".to_string()))
    );
}

#[test]
fn shutdown_retires_entry_once_drained() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("bye", 4).unwrap();
    let rx = registry.acquire_receiver::<i32>("bye").unwrap();

    registry.shutdown("bye").unwrap();
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    drop(rx);

    // Entry retired: the name is free again.
    registry.create_broadcasting::<i32>("bye", 4).unwrap();
}

#[test]
fn state_reports_worst_subscriber_lag() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("lag", 8).unwrap();
    let tx = registry.acquire_sender::<i32>("lag").unwrap();
    let ahead = registry.acquire_receiver::<i32>("lag").unwrap();
    let _behind = registry.acquire_receiver::<i32>("lag").unwrap();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    ahead.recv().unwrap();
    ahead.recv().unwrap();

    assert_eq!(registry.state("lag"), Ok(QueueState::Open { pending: 2 }));
}

#[test]
fn destroy_discards_and_frees_name() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("doomed", 4).unwrap();
    let tx = registry.acquire_sender::<i32>("doomed").unwrap();
    let rx = registry.acquire_receiver::<i32>("doomed").unwrap();

    tx.send(1).unwrap();
    registry.destroy("doomed").unwrap();

    assert_eq!(tx.send(2), Err(SendError::Closed(2)));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    registry.create::<i32>("doomed", 4).unwrap();
}

#[test]
fn type_mismatch_and_name_collision_across_kinds() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("shared-ns", 4).unwrap();

    assert_eq!(
        registry.create::<i32>("shared-ns", 4),
        Err(QueueError::QueueAlreadyExists("shared-ns".to_string()))
    );
    assert!(matches!(
        registry.acquire_sender::<String>("shared-ns"),
        Err(QueueError::TypeMismatch { .. })
    ));

    // A work-sharing queue is not a broadcasting queue of the same T?
    // Same T, same name: acquire works against whichever kind holds the name.
    registry.create::<i32>("plain", 4).unwrap();
    let tx = registry.acquire_sender::<i32>("plain").unwrap();
    let rx = registry.acquire_receiver::<i32>("plain").unwrap();
    tx.send(9).unwrap();
    assert_eq!(rx.recv().unwrap(), 9);
}

#[test]
fn name_accessor_works() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("named", 4).unwrap();
    let tx = registry.acquire_sender::<i32>("named").unwrap();
    let rx = registry.acquire_receiver::<i32>("named").unwrap();
    assert_eq!(tx.name(), "named");
    assert_eq!(rx.name(), "named");
}

#[test]
fn default_registry_helper() {
    named_queue::create_broadcasting::<u8>("global-bcast", 4).unwrap();
    let tx = named_queue::acquire_sender::<u8>("global-bcast").unwrap();
    let rx = named_queue::acquire_receiver::<u8>("global-bcast").unwrap();
    tx.send(5).unwrap();
    assert_eq!(rx.recv().unwrap(), 5);
    named_queue::destroy("global-bcast").unwrap();
}

#[tokio::test]
async fn async_send_recv() {
    let registry = QueueRegistry::new();
    registry.create_broadcasting::<i32>("async", 4).unwrap();
    let tx = registry.acquire_sender::<i32>("async").unwrap();
    let rx = registry.acquire_receiver::<i32>("async").unwrap();

    tx.send_async(11).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), rx.recv_async())
        .await
        .expect("recv_async timed out")
        .unwrap();
    assert_eq!(got, 11);
}
