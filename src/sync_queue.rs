use std::collections::VecDeque;
use crate::proto::io::ReplicateRequest;

pub struct ReplicateQueue {
    queue: VecDeque<ReplicateRequest>,
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

    pub fn put(&mut self, synchronize_request: ReplicateRequest) {
        if self.queue.len() >= self.size {
            self.last_received_index = synchronize_request.commit_index;
            self.queue.push_back(synchronize_request);
        }
    }

    pub fn pop(&mut self) -> Option<ReplicateRequest> {
        self.queue.pop_front()
    }

    pub fn last_received_index(&self) -> u64 {
        self.last_received_index
    }
}
