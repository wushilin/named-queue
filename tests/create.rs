use msgbus::{BusError, MessageBus};

#[test]
fn create_registers_a_queue() {
    let bus = MessageBus::new();
    assert_eq!(bus.create::<String>("events", 16), Ok(()));
}

#[test]
fn create_rejects_duplicate_names_even_across_types() {
    let bus = MessageBus::new();
    bus.create::<String>("events", 16).unwrap();
    assert_eq!(
        bus.create::<String>("events", 16),
        Err(BusError::QueueAlreadyExists("events".into()))
    );
    assert_eq!(
        bus.create::<u64>("events", 16),
        Err(BusError::QueueAlreadyExists("events".into()))
    );
}

#[test]
fn distinct_names_coexist() {
    let bus = MessageBus::new();
    assert_eq!(bus.create::<String>("a", 4), Ok(()));
    assert_eq!(bus.create::<u64>("b", 4), Ok(()));
}

#[test]
fn clones_share_state_but_new_buses_are_independent() {
    let bus = MessageBus::new();
    bus.create::<u8>("a", 4).unwrap();
    let clone = bus.clone();
    assert_eq!(
        clone.create::<u8>("a", 4),
        Err(BusError::QueueAlreadyExists("a".into()))
    );
    let other = MessageBus::new();
    assert_eq!(other.create::<u8>("a", 4), Ok(()));
}
