use std::sync::Arc;

use object_store::Error;
use tokio::{
    sync::watch::{Receiver, Sender, channel},
    task::{JoinError, JoinHandle, id},
};
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker},
    time::FutureExt,
};

use crate::{error::S2cError, log::S2cLog, object_read_write::ObjectReadWrite};

struct Compactor {
    start_idx: u64,
    object_read_write: Arc<ObjectReadWrite>,
    s2c_log: Arc<S2cLog>,
    compaction_index_rx: Receiver<u64>,
    compaction_index_tx: Sender<u64>,
}

impl Compactor {
    pub fn new(
        start_idx: u64,
        object_read_write: Arc<ObjectReadWrite>,
        s2c_log: Arc<S2cLog>,
    ) -> Self {
        let (compaction_index_tx, compaction_index_rx) = channel(0);
        Self {
            start_idx,
            s2c_log,
            object_read_write,
            compaction_index_rx,
            compaction_index_tx,
        }
    }

    pub async fn start(&mut self) -> Result<(), S2cError> {
        let mut start_idx = self.start_idx;
        loop {
            match self.compaction_index_rx.changed().await {
                Ok(_) => {
                    let idx = *self.compaction_index_rx.borrow_and_update();
                    if let Err(e) = self.s2c_log.delete_range(start_idx, idx).await {
                        return Err(S2cError::NonTransientObjectStoreError(e));
                    }
                    start_idx = idx;
                }
                Err(_) => break,
            }
        }
        Ok(())
    }

    pub async fn compact_to(&self, commit_index: u64) {
        // Ignore closed channel error.
        let _ = self.compaction_index_tx.send(commit_index);
    }
}
