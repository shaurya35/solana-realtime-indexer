use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventId {
    pub signature: String,
    pub absolute_path: Vec<u8>,
    pub event_ordinal: u32,
}

pub type EventLog = Arc<Mutex<Vec<EventId>>>;
