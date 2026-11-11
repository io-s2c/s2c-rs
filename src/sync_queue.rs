use crate::proto::io::{LogEntry, ReplicateRequest};
use std::collections::VecDeque;

pub struct ReplicateQueue {
    queue: VecDeque<LogEntry>,
    size: usize,
    last_received_index: u64,
}

impl ReplicateQueue {
    pub fn new(size: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            size,
            last_received_index: 0,
        }
    }

    pub fn put(&mut self, log_entry: LogEntry) {
        if self.queue.len() >= self.size {
            self.last_received_index = log_entry
                .leader_state
                .as_ref()
                .expect("Invalid entry")
                .commit_index;
            self.queue.push_back(log_entry);
        }
    }

    pub fn pop(&mut self) -> Option<LogEntry> {
        self.queue.pop_front()
    }

    pub fn last_received_index(&self) -> u64 {
        self.last_received_index
    }
}
