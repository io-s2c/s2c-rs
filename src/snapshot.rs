use crate::context::Context;
use crate::log::S2cLog;
use crate::object_read_write::ObjectReadWrite;
use crate::paths::KeysResolver;
use crate::proto::io::Snapshot;
use crate::{config::S2cOptions, error::S2cError};
use object_store::Error;
use object_store::path::Path;
use prost::Message;
use std::sync::Arc;
use tokio::sync::watch::Receiver;
use tokio_util::bytes::Bytes;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};

pub struct Snapshotter {
    s2c_log: Arc<S2cLog>,
    context: &'static Context,
    object_read_write: Arc<ObjectReadWrite>,
    task_tracker: TaskTracker,
    s2c_options: &'static S2cOptions,
    commit_index_rx: Option<Receiver<u64>>,
    commit_index: u64,
    start_index: u64,
    snapshot_provider: Arc<dyn Sync + Send + Fn(u64) -> Bytes>,
    snapshot_etag: Option<String>,
    epoch: u64,
}

impl Snapshotter {
    pub fn new(
        s2c_log: Arc<S2cLog>,
        object_read_write: Arc<ObjectReadWrite>,
        s2c_options: &'static S2cOptions,
        context: &'static Context,
        commit_index_rx: Receiver<u64>,
        start_index: u64,
        snapshot_provider: Box<dyn Sync + Send + Fn(u64) -> Bytes>,
        snapshot_etag: Option<String>,
        epoch: u64,
    ) -> Self {
        Self {
            s2c_log,
            object_read_write,
            task_tracker: TaskTracker::new(),
            s2c_options,
            context,
            commit_index_rx: Some(commit_index_rx),
            start_index,
            snapshot_provider: snapshot_provider.into(),
            snapshot_etag,
            epoch,
            commit_index: 0,
        }
    }

    pub async fn start(&mut self) -> Result<(), S2cError> {
        let mut commit_index_rx = self.commit_index_rx.take().expect("Already taken");
        let threshold = self.s2c_options.snapshotting_threshold;
        let group_id = self.context.s2c_group_id();
        let epoch = self.epoch;
        let key_resolver = KeysResolver::new(group_id.into());
        loop {
            if self.log_size() >= threshold as u64 {
                let snapshot_index = self.commit_index - 1;
                let snapshot_payload = (self.snapshot_provider)(snapshot_index);
                let snapshot = Snapshot {
                    at_apply_index: snapshot_index,
                    body: snapshot_payload,
                    leader_epoch: epoch,
                };
                let re = match self.snapshot_etag.as_ref() {
                    Some(e_tag) => {
                        self.object_read_write
                            .put_if_match(
                                Path::from(key_resolver.snapshot()),
                                snapshot.encode_to_vec().into(),
                                e_tag.into(),
                            )
                            .await
                    }
                    None => {
                        self.object_read_write
                            .put_if_absent(
                                Path::from(key_resolver.snapshot()),
                                snapshot.encode_to_vec().into(),
                            )
                            .await
                    }
                };

                match re {
                    Ok(_) => {
                        tracing::info!(apply_idex = snapshot_index, "Snapshot persited");
                    }
                    Err(Error::Precondition { .. } | Error::AlreadyExists { .. }) => {
                        // In both cases: either current leader is stale, or a stale leader managed to add a snapshot.
                        // Neither case exposes correctness violations because the SSOT is the log, which is also the real fence.
                        // We check if we are stale and step down if so, otherwise, we don't update our current_log_size
                        // and suppose the leader who added the snapshot failed by the truncation.
                        match self
                            .object_read_write
                            .read(Path::from(key_resolver.snapshot()))
                            .await
                        {
                            Ok(payload) => {
                                let found_epoch = Snapshot::decode(payload)
                                    .expect("Invalid snapshot")
                                    .leader_epoch;
                                if found_epoch > epoch {
                                    // A new leader took over. We step down.
                                    return Err(S2cError::StaleEpoch);
                                } else if found_epoch < epoch {
                                    // A stale leader managed to put a snapshot, ignore
                                } else {
                                    return Err(S2cError::ConcurrentLeaders);
                                }
                            }
                            Err(e) => return Err(S2cError::NonTransientObjectStoreError(e)),
                        }
                    }
                    Err(e) => return Err(S2cError::NonTransientObjectStoreError(e)),
                }
                // TODO: call compactor
                self.start_index = self.commit_index;
            }
            match commit_index_rx.changed().await {
                Ok(_) => {
                    self.commit_index = *commit_index_rx.borrow_and_update();
                }
                Err(_) => {
                    return Ok(());
                }
            }
        }
    }

    fn log_size(&self) -> u64 {
        // We don't consider current commitIndex
        (self.commit_index - 1) - self.start_index
    }
}
