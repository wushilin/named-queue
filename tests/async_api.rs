use std::time::Duration;

use named_queue::{QueueRegistry, RecvError};

#[tokio::test(flavor = "multi_thread")]
async fn async_roundtrip() {
    let registry = QueueRegistry::new();
    registry.create::<String>("chat", 8).unwrap();
    let tx = registry.acquire_sender::<String>("chat").unwrap();
    let rx = registry.acquire_receiver::<String>("chat").unwrap();

    tx.send_async("hello".to_string()).await.unwrap();
    assert_eq!(rx.recv_async().await.unwrap(), "hello");
}

#[tokio::test(flavor = "multi_thread")]
async fn send_async_applies_backpressure() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 1).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    let rx = registry.acquire_receiver::<u32>("q").unwrap();

    tx.send_async(1).await.unwrap();
    let pending = tokio::spawn(async move { tx.send_async(2).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(rx.recv_async().await, Ok(1));
    pending.await.unwrap().unwrap();
    assert_eq!(rx.recv_async().await, Ok(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn recv_async_drains_then_sees_closed() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 4).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    let rx = registry.acquire_receiver::<u32>("q").unwrap();

    tx.send_async(9).await.unwrap();
    registry.shutdown("q").unwrap();

    assert_eq!(rx.recv_async().await, Ok(9));
    assert_eq!(rx.recv_async().await, Err(RecvError::Closed));
    assert_eq!(registry.create::<u32>("q", 4), Ok(()));
}
