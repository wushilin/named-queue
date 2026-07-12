use named_queue::{QueueError, QueueRegistry, QueueState};

#[test]
fn create_registers_a_queue() {
    let registry = QueueRegistry::new();
    assert_eq!(registry.create::<String>("events", 16), Ok(()));
}

#[test]
fn create_rejects_duplicate_names_even_across_types() {
    let registry = QueueRegistry::new();
    registry.create::<String>("events", 16).unwrap();
    assert_eq!(
        registry.create::<String>("events", 16),
        Err(QueueError::QueueAlreadyExists("events".into()))
    );
    assert_eq!(
        registry.create::<u64>("events", 16),
        Err(QueueError::QueueAlreadyExists("events".into()))
    );
}

#[test]
fn distinct_names_coexist() {
    let registry = QueueRegistry::new();
    assert_eq!(registry.create::<String>("a", 4), Ok(()));
    assert_eq!(registry.create::<u64>("b", 4), Ok(()));
}

#[test]
fn clones_share_state_but_new_registries_are_independent() {
    let registry = QueueRegistry::new();
    registry.create::<u8>("a", 4).unwrap();
    let clone = registry.clone();
    assert_eq!(
        clone.create::<u8>("a", 4),
        Err(QueueError::QueueAlreadyExists("a".into()))
    );
    let other = QueueRegistry::new();
    assert_eq!(other.create::<u8>("a", 4), Ok(()));
}

#[test]
fn default_registry_convenience_api_uses_one_process_wide_registry() {
    let name = "default-registry-convenience-api";
    let _ = named_queue::destroy(name);

    assert_eq!(named_queue::create::<String>(name, 2), Ok(()));
    let tx = named_queue::acquire_sender::<String>(name).unwrap();
    let rx = named_queue::default_registry()
        .acquire_receiver::<String>(name)
        .unwrap();

    tx.send("hello".to_string()).unwrap();
    assert_eq!(
        named_queue::state(name),
        Ok(QueueState::Open { pending: 1 })
    );
    assert_eq!(rx.recv(), Ok("hello".to_string()));
    assert_eq!(named_queue::destroy(name), Ok(()));
}
