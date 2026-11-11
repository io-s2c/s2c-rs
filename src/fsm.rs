use crate::backoff::BackoffCounter;
use crate::config::S2cOptions;
use crate::context::Context;
use crate::fsm::FsmState::Stopped;
use crate::log::S2cLog;
use crate::network::client::{MessageReader, S2cClient};
use crate::network::error::ClientError;
use crate::network::group_server::S2cMessageEnvelope;
use crate::network::server::S2cServer;
use crate::paths::KeysResolver;
use crate::proto::io::s2c_message::Body;
use crate::proto::io::s2c_message::Body::Follow;
use crate::proto::io::{
    Batch, BatchEntry, LeaderState, LogEntry, NodeIdentity, NotLeaderError, ReplicateResponse,
    S2cMessage, StateRequest,
};
use crate::replicator::Replicator;
use crate::sync_queue::ReplicateQueue;
use object_store::{Error, ObjectStore};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, timeout};
use tokio_util::bytes::Bytes;
use tracing::{debug, error};
use uuid::Uuid;

enum FsmState {
    NotJoined,
    Follower,
    Leader,
    Stopped, // End state
}

pub type MessageReaderFactory = Arc<dyn Fn() -> MessageReader + Send + Sync>;

struct Fsm {
    state: FsmState,
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
    s2c_log: Arc<S2cLog>,
    keys_resolver: KeysResolver,
    object_store: Arc<dyn ObjectStore>,
    missed_heartbeats: u32,
    commit_index: u64,
    apply_index: u64,
    epoch: u64,
    context: &'static Context,
    followers: Vec<Replicator>,
    commit_index_tx: tokio::sync::watch::Sender<u64>,
}

impl Fsm {
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
        s2c_log: Arc<S2cLog>,
        context: &'static Context,
    ) -> Self {
        let (commit_index_tx, _) = tokio::sync::watch::channel(0);
        Self {
            bucket,
            state: FsmState::NotJoined,
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
            context,
            followers: Vec::new(),
            commit_index_tx,
        }
    }

    async fn start(&mut self) {
        loop {
            match self.state {
                FsmState::NotJoined => self.join().await,
                FsmState::Follower => self.follower_loop().await,
                FsmState::Leader => self.leader_loop().await,
                FsmState::Stopped => break,
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
                Err(_) => {
                    self.missed_heartbeats += 1;
                    if self.missed_heartbeats >= self.s2c_options.max_missed_heartbeats {
                        self.state = FsmState::NotJoined;
                        return;
                    }
                }
                Ok(Some(mut msg)) => {
                    let mut s2c_message: Option<S2cMessage> = None;
                    match msg.s2c_message.body.take() {
                        Some(Body::ReplicateRequest(replicate)) => {
                            self.missed_heartbeats = 0;
                            let log_entry = LogEntry::decode(Bytes::from(replicate.log_entry))
                                .expect("Invalid entry");
                            if log_entry
                                .leader_state
                                .as_ref()
                                .expect("invalid entry")
                                .commit_index
                                == self.sync_queue.last_received_index() + 1
                            {
                                self.sync_queue.put(log_entry);
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
        // let
        loop {
            tokio::select! {
                next = self.next() => {
                    let body = next.map_or(None, |envelope| envelope.s2c_message.body);
                    match body {
                        Some(Follow(follow)) => {
                            let mut replicator = Replicator::new(
                                self.s2c_log.clone(),
                                follow
                                    .node_identity
                                    .expect("Invalid follow request")
                                    .clone(),
                                self.s2c_options,
                                self.context,
                                Arc::new(self.message_reader_factory.clone()),
                                follow.apply_index,
                                self.commit_index_tx.subscribe(),
                            );
                            replicator.start().await;
                            self.followers.push(replicator);
                      },
                    Some(Body::StateRequest(state_request)) => {
                        acc.push(state_request);
                    }

                    Some(_) => {
                        // Irrelevant - ignore
                    }
                    None => {
                        error!("Unexpected empty body")
                    }
                    }
                },
            }
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
                        body: Bytes::from(ve),
                        internal: false,
                    });
                });
                let batch = Batch { entries };
                let commit_index = self.commit_index + 1;
                let log_entry = LogEntry {
                    batch: Some(batch),
                    leader_state: Some(LeaderState {
                        epoch: self.epoch,
                        commit_index,
                        node_identity: Some(self.node_identity.clone()),
                    }),
                };
                let log_entry_bytes = Bytes::from(log_entry.encode_to_vec());
                match self
                    .s2c_log
                    .append(commit_index, log_entry_bytes.clone())
                    .await
                {
                    Ok(_) => {
                        self.set_commit_index(commit_index);
                        acc.clear();
                    }
                    Err(Error::AlreadyExists { .. }) => {
                        // Another node took over leadership.
                        self.state = FsmState::NotJoined;
                        return;
                    }
                    Err(e) => {
                        error!("Failed appending log entry. Stopping node. {:?}", e);
                        self.state = Stopped;
                        return;
                    }
                }
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
        loop {
            match self.discover_leader_state().await {
                Ok(Some(leader_state)) => {
                    let leader_identity = leader_state.node_identity.expect("Invalid leader state");
                    match self.connect(&leader_identity).await {
                        Ok(_) => match self.follow(&leader_identity).await {
                            Ok(_) => self.state = FsmState::Follower,
                            Err(ClientError::WrongState { client_state }) => {
                                error!("Wrong state {:?}", client_state)
                            }
                            Err(e) => {
                                self.attempt_leadership().await;
                            }
                        },
                        Err(ClientError::WrongState { client_state }) => {
                            error!("Wrong state {:?}", client_state);
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
                    error!("Non transient object store error {e}");
                    break;
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
            }),
        };

        let payload = Bytes::from(LogEntry::encode_to_vec(&log_entry));
        match self.s2c_log.append(commit_index, payload).await {
            Ok(_) => {
                self.state = FsmState::Leader;
                self.set_commit_index(commit_index);
                self.epoch = epoch;
            }
            Err(Error::AlreadyExists { .. }) => {}
            Err(e) => {
                error!("Unexpected error {e}");
                self.state = FsmState::Stopped;
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
                Err(e @ ClientError::NetworkError { .. }) => {
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

    async fn discover_leader_state(&self) -> Result<Option<LeaderState>, Arc<Error>> {
        match self.s2c_log.get_head().await {
            Ok(Some(head)) => match self.s2c_log.get_at(head).await {
                Ok(Some(bytes)) => Ok(Some(parse_leader_state(bytes))),
                // We panic here because this would be a fatal correctness error.
                // The head should never be truncated
                Ok(None) => panic!("Entry at head not found"),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(Arc::from(e)),
        }
    }
    fn set_commit_index(&mut self, commit_index: u64) {
        self.commit_index = commit_index;
        self.commit_index_tx
            .send(commit_index)
            .expect("Commit index channel closed");
    }
}

fn parse_leader_state(bytes: Bytes) -> LeaderState {
    let entry = LogEntry::decode(bytes).expect("Invalid entry");
    entry.leader_state.expect("Invalid entry")
}
