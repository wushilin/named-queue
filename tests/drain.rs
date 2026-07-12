use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use named_queue::{QueueError, QueueRegistry, QueueState, RecvError};

#[test]
fn name_frees_only_after_last_message_is_consumed() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 4).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    let rx = registry.acquire_receiver::<u32>("q").unwrap();
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    registry.shutdown("q").unwrap();
    assert_eq!(
        registry.create::<u32>("q", 4),
        Err(QueueError::QueueAlreadyExists("q".into()))
    );
    assert_eq!(registry.state("q"), Ok(QueueState::Closed { pending: 2 }));

    assert_eq!(rx.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Err(RecvError::Closed));
    assert_eq!(registry.create::<u32>("q", 4), Ok(()));
}

#[test]
fn blocked_sender_returns_promptly_on_destroy() {
    let registry = QueueRegistry::new();
    registry.create::<u32>("q", 1).unwrap();
    let tx = registry.acquire_sender::<u32>("q").unwrap();
    tx.send(1).unwrap();

    let handle = thread::spawn(move || tx.send(2));
    thread::sleep(Duration::from_millis(100));
    registry.shutdown("q").unwrap();
    registry.destroy("q").unwrap();

    let _ = handle.join().unwrap();
    assert_eq!(registry.create::<u32>("q", 1), Ok(()));
}

#[test]
fn mpmc_end_to_end_under_backpressure() {
    const SENDERS: usize = 4;
    const RECEIVERS: usize = 4;
    const PER_SENDER: usize = 250;

    let registry = QueueRegistry::new();
    registry.create::<usize>("work", 16).unwrap();

    let receivers: Vec<_> = (0..RECEIVERS)
        .map(|_| {
            let rx = registry.acquire_receiver::<usize>("work").unwrap();
            thread::spawn(move || {
                let mut got = Vec::new();
                while let Ok(v) = rx.recv() {
                    got.push(v);
                }
                got
            })
        })
        .collect();

    let senders: Vec<_> = (0..SENDERS)
        .map(|i| {
            let tx = registry.acquire_sender::<usize>("work").unwrap();
            thread::spawn(move || {
                for j in 0..PER_SENDER {
                    tx.send(i * PER_SENDER + j).unwrap();
                }
            })
        })
        .collect();

    for s in senders {
        s.join().unwrap();
    }
    registry.shutdown("work").unwrap();

    let mut all = HashSet::new();
    for r in receivers {
        for v in r.join().unwrap() {
            assert!(all.insert(v), "message {v} delivered twice");
        }
    }
    assert_eq!(all.len(), SENDERS * PER_SENDER);
    assert_eq!(registry.create::<usize>("work", 16), Ok(()));
}
