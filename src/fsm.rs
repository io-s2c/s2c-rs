use crate::config::{S2cOptions, S2cRetryOptions};
use crate::network::client::{MessageReader, S2cClient};
use crate::network::group_server::{GroupServer, S2cMessageEnvelope};
use crate::network::server::S2cServer;
use crate::paths::KeysResolver;
use crate::proto::io::{
    Batch, BatchEntry, LeaderState, LogEntry, NodeIdentity, NotLeaderError, S2cMessage,
    StateRequest, ReplicateRequest, ReplicateResponse,
};
use std::ops::Sub;

use crate::backoff::BackoffCounter;
use crate::error::is_transient;
use crate::log::S2cLog;
use crate::network::error::ClientError;
use crate::proto::io::s2c_message::Body;
use crate::proto::io::s2c_message::Body::Follow;
use crate::sync_queue::ReplicateQueue;
use object_store::{Error, ObjectStore};
use prost::Message;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::time::{timeout, Instant};
use tokio_stream::StreamExt;
use tokio_util::bytes::Bytes;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};
use uuid::Uuid;

enum FSMState {
    NOT_JOINED,
    FOLLOWER,
    LEADER,
    STOPPED, // End state
}

pub type MessageReaderFactory = Arc<dyn Fn() -> MessageReader + Send + Sync>;

struct FSM {
    state: FSMState,
    s2c_options: &'static S2cOptions,
    server: &'static S2cServer,
    client: &'static S2cClient,
    local_requests_tx: UnboundedSender<S2cMessageEnvelope>,
    local_requests_tr: UnboundedReceiver<S2cMessageEnvelope>,
    requests_tx: Sender<S2cMessageEnvelope>,
    requests_tr: Receiver<S2cMessageEnvelope>,
    sync_queue: ReplicateQueue,
    bucket: String,
    group: String,
    node_identity: &'static NodeIdentity,
    message_reader_factory: MessageReaderFactory,
    s2c_log: S2cLog,
    keys_resolver: KeysResolver,
    object_store: Arc<dyn ObjectStore>,
    missed_heartbeats: u32,
    commit_index: u64,
    apply_index: u64,
    epoch: u64,
    followers: Vec<NodeIdentity>,
}

impl FSM {
    pub fn new(
        group: String,
        server: &'static S2cServer,
        client: &'static S2cClient,
        local_tx: UnboundedSender<S2cMessageEnvelope>,
        local_tr: UnboundedReceiver<S2cMessageEnvelope>,
        tx: Sender<S2cMessageEnvelope>,
        tr: Receiver<S2cMessageEnvelope>,
        s2c_options: &'static S2cOptions,
        node_identity: &'static NodeIdentity,
        object_store: Arc<dyn ObjectStore>,
        bucket: String,
        message_reader_factory: MessageReaderFactory,
        s2c_log: S2cLog,
    ) -> Self {
        Self {
            bucket,
            state: FSMState::NOT_JOINED,
            server,
            client,
            local_requests_tx: local_tx,
            local_requests_tr: local_tr,
            requests_tx: tx,
            requests_tr: tr,
            sync_queue: ReplicateQueue::new(s2c_options.max_batches_pending_for_apply as usize),
            s2c_options,
            object_store,
            group: group.clone(),
            s2c_log,
            keys_resolver: KeysResolver::new(group),
            node_identity,
            message_reader_factory,
            missed_heartbeats: 0,
            commit_index: 0,
            apply_index: 0,
            epoch: 0,
            followers: Vec::new(),
        }
    }

    async fn start(&mut self) {
        loop {
            match self.state {
                FSMState::NOT_JOINED => self.join().await,
                FSMState::FOLLOWER => self.follower_loop().await,
                FSMState::LEADER => self.leader_loop().await,
                FSMState::STOPPED => break,
            }
        }
    }

    async fn follower_loop(&mut self) {
        loop {
            match timeout(
                Duration::from_millis(self.s2c_options.leader_heartbeat_timeout_ms),
                self.requests_tr.recv(),
            )
            .await
            {
                Err(e) => {
                    self.missed_heartbeats += 1;
                    if self.missed_heartbeats >= self.s2c_options.max_missed_heartbeats {
                        self.state = FSMState::NOT_JOINED;
                        return;
                    }
                }
                Ok(Some(mut msg)) => {
                    let mut s2c_message: Option<S2cMessage> = None;
                    match msg.s2c_message.body.take() {
                        Some(Body::ReplicateRequest(replicate)) => {
                            self.missed_heartbeats = 0;
                            if replicate.commit_index == self.sync_queue.last_received_index() + 1
                            {
                                self.sync_queue.put(replicate);
                            }
                            s2c_message = Some(S2cMessage {
                                correlation_id: msg.s2c_message.correlation_id,
                                body: Some(Body::ReplicateResponse(ReplicateResponse {
                                    apply_index: self.sync_queue.last_received_index(),
                                })),
                                error: None,
                            });
                        }
                        // These could come when the current node is still thought being leader by another node
                        Some(Body::StateRequest { .. } | Body::Follow { .. }) => {
                            s2c_message = Some(S2cMessage {
                                correlation_id: msg.s2c_message.correlation_id,
                                body: None,
                                error: Some(crate::proto::io::s2c_message::Error::NotLeaderError(
                                    NotLeaderError {},
                                )),
                            });
                        }
                        Some(_) => {
                            // ignore
                        }
                        None => {
                            error!("Message has no body {:?}", msg.s2c_message);
                        }
                    }
                    if let Some(follower_response) = s2c_message {
                        match self
                            .requests_tx
                            .send(S2cMessageEnvelope {
                                s2c_message: follower_response,
                                node_identity: Arc::from(self.node_identity.clone()),
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {
                                error!("failed sending response")
                            }
                        }
                    }
                }
                Ok(None) => {
                    debug!("In channel was closed. Ending FSM.");
                    break;
                }
            }
        }
    }

    async fn leader_loop(&mut self) {
        let mut acc: Vec<StateRequest> = Vec::new();
        let flush_interval_nanos = Duration::from_millis(self.s2c_options.flush_interval_ms);
        let mut start = Instant::now();
        let batch_min_size = self.s2c_options.batch_min_count as usize;
        loop {
            let mut due = Vec::new();
            let elapsed = start.elapsed();
            let remaining = flush_interval_nanos
                .checked_sub(elapsed)
                .unwrap_or(Duration::ZERO);
            match timeout(remaining, self.next()).await {
                Ok(received) => match received {
                    Some(msg) => {
                        due.push(msg);
                    }
                    None => {
                        debug!("In channel was closed. Ending FSM.");
                        break;
                    }
                },
                Err(_) => {}
            }

            while let Ok(msg) = self.local_requests_tr.try_recv() {
                due.push(msg);
            }
            while let Ok(msg) = self.requests_tr.try_recv() {
                due.push(msg);
            }
            for msg in due {
                match msg.s2c_message.body {
                    Some(Body::StateRequest(state_request)) => {
                        acc.push(state_request);
                    }
                    Some(Follow(follow)) => self
                        .followers
                        .push(follow.node_identity.expect("Invalid follow request")),
                    // Start follower replicator
                    Some(_) => {
                        // Irrelevant - ignore
                    }
                    None => {
                        error!("Unexpected empty body")
                    }
                }
            }

            if start.elapsed().gt(&flush_interval_nanos) || acc.len() > batch_min_size {
                let mut entries = Vec::new();
                acc.iter().for_each(|state_request| {
                    let mut ve = vec![0; state_request.encoded_len()];
                    state_request.encode(&mut ve).expect("Invalid entry");
                    entries.push(BatchEntry {
                        body: ve,
                        internal: false,
                    });
                });
                let batch = Batch { entries };
                // let log_entry = LogEntry {
                //     batch,
                //     leader_state: LeaderState {
                //         node_identity: Some(self.node_identity.clone()),
                //         followers: self.followers
                //     }
                // }
                match self
                    .s2c_log
                    .append(self.commit_index, Bytes::from(batch.encode_to_vec()))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {}
                }
                acc.clear();
                start = Instant::now();
            }
        }
    }

    async fn next(&mut self) -> Option<S2cMessageEnvelope> {
        let received = tokio::select! {
            m = self.local_requests_tr.recv() => m,
            m = self.requests_tr.recv() => m,
        };
        received
    }

    async fn join(&mut self) {
        let mut backoff = BackoffCounter::new(&self.s2c_options.retry);
        while matches!(self.state, FSMState::NOT_JOINED) {
            match self.discover_leader_state().await {
                Ok(Some(leader_state)) => {
                    let leader_identity = leader_state.node_identity.expect("Invalid leader state");
                    match self.connect(&leader_identity).await {
                        Ok(_) => match self.follow(&leader_identity).await {
                            Ok(_) => self.state = FSMState::FOLLOWER,
                            Err(ClientError::Closed | ClientError::NotConnected) => {
                                error!("Wrong state")
                            }
                            Err(e) => {
                                self.attempt_leadership().await;
                            }
                        },
                        Err(ClientError::Closed | ClientError::NotConnected) => {
                            error!("Wrong state");
                            return;
                        }
                        Err(e) => {
                            self.attempt_leadership().await;
                        }
                    }
                }
                Ok(None) => {
                    self.attempt_leadership().await;
                }
                Err(e) => {
                    if is_transient(&e) {
                        if backoff.can_attempt() {
                            backoff.await_attempt().await;
                        } else {
                            backoff.reset()
                        }
                    } else {
                        error!("Non transient object store error");
                        return;
                    }
                }
            }
        }
    }

    async fn attempt_leadership(&mut self) {
        let (commit_index, epoch) = if self.commit_index > 0 {
            (self.commit_index + 1, self.epoch + 1)
        } else {
            (1, 1)
        };
        let log_entry = LogEntry {
            batch: Some(Batch { entries: vec![] }),
            leader_state: Some(LeaderState {
                commit_index,
                node_identity: Some(self.node_identity.clone()),
                epoch,
                followers: Vec::new(),
            }),
        };

        let payload = Bytes::from(LogEntry::encode_to_vec(&log_entry));
        let unlimited = self.s2c_options.retry.to_unlimited();
        let mut backoff = BackoffCounter::new(&unlimited);
        loop {
            match self.s2c_log.append(commit_index, payload.clone()).await {
                Ok(_) => {
                    self.state = FSMState::LEADER;
                    self.commit_index = commit_index;
                    self.epoch = epoch;
                    return;
                }
                Err(e @ object_store::Error::AlreadyExists { .. }) => {
                    // debug!("")
                    return;
                }
                Err(e) => {
                    if is_transient(&e) {
                        backoff.await_attempt().await;
                    } else {
                        error!("Unexpected error {e}");
                        self.state = FSMState::STOPPED;
                        return;
                    }
                }
            }
        }
    }

    async fn connect(&self, leader_identity: &NodeIdentity) -> Result<(), ClientError> {
        let mut connect_backoff = BackoffCounter::new(&self.s2c_options.retry);
        loop {
            match self.client.connect(&leader_identity).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    if connect_backoff.can_attempt() {
                        connect_backoff.await_attempt().await
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    async fn follow(&self, leader_identity: &NodeIdentity) -> Result<(), ClientError> {
        let mut send_backoff = BackoffCounter::new(&self.s2c_options.retry);
        loop {
            let correlation_id = Uuid::new_v4().to_string();
            let payload = Bytes::from(
                S2cMessage {
                    correlation_id: correlation_id.clone(),
                    body: Some(Follow(crate::proto::io::Follow {
                        node_identity: Some(self.node_identity.clone()),
                        apply_index: self.apply_index,
                        group_id: self.group.clone(),
                    })),
                    error: None,
                }
                .encode_to_vec(),
            );
            match self
                .client
                .send(
                    correlation_id,
                    payload,
                    self.s2c_options.network.connect_timeout_ms,
                )
                .await
            {
                // We ignore the response here, but usually it can be used to get the NodeSequenceNumber
                Ok(_) => return Ok(()),
                Err(e @ ClientError::Io { .. }) => {
                    if send_backoff.can_attempt() {
                        send_backoff.await_attempt().await
                    } else {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn discover_leader_state(&self) -> Result<Option<LeaderState>, Error> {
        let mut backoff = BackoffCounter::new(&self.s2c_options.retry);
        loop {
            match self.s2c_log.get_head().await {
                Ok(Some(head)) => match self.s2c_log.get_at(head).await {
                    Ok(bytes) => return Ok(Some(parse_leader_state(bytes))),
                    Err(e) => {
                        if is_transient(&e) && backoff.can_attempt() {
                            backoff.await_attempt().await;
                        } else {
                            return Err(e);
                        }
                    }
                },
                Ok(None) => return Ok(None),
                Err(e) => {
                    if is_transient(&e) && backoff.can_attempt() {
                        backoff.await_attempt().await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
}

fn parse_leader_state(bytes: Bytes) -> LeaderState {
    let entry = LogEntry::decode(bytes).expect("Invalid entry");
    entry.leader_state.expect("Invalid entry")
}
