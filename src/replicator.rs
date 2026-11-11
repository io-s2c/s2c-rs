use crate::backoff::BackoffCounter;
use crate::config::{S2cOptions, S2cRetryOptions};
use crate::context::Context;
use crate::fsm::MessageReaderFactory;
use crate::log::S2cLog;
use crate::network::client::S2cClient;
use crate::network::error::ClientError;
use crate::proto::io::s2c_message::Body;
use crate::proto::io::{
    FollowerTooFarBehindError, LogEntry, NodeIdentity, ReplicateRequest, S2cMessage,
};
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio_util::bytes::Bytes;
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};
use uuid::Uuid;

pub struct Replicator {
    s2c_client: Arc<S2cClient>,
    follower_node_identity: NodeIdentity,
    s2c_log: Arc<S2cLog>,
    s2c_options: &'static S2cOptions,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    follower_index: u64,
    commit_index_rx: Option<watch::Receiver<u64>>,
}

impl Replicator {
    pub fn new(
        s2c_log: Arc<S2cLog>,
        follower_node_identity: NodeIdentity,
        s2c_options: &'static S2cOptions,
        context: &'static Context,
        message_reader_factory: Arc<MessageReaderFactory>,
        follower_index: u64,
        leader_index_rx: watch::Receiver<u64>,
    ) -> Self {
        Self {
            s2c_log,
            follower_node_identity,
            s2c_client: Arc::new(S2cClient::new(s2c_options, context, message_reader_factory)),
            s2c_options,
            follower_index,
            cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            commit_index_rx: Some(leader_index_rx),
        }
    }

    pub fn replicated_index(&self) -> u64 {
        self.follower_index
    }

    pub fn follower_node_identity(&self) -> &NodeIdentity {
        &self.follower_node_identity
    }
    // Replicators are fire and forget and don't have return type.
    pub async fn start(&mut self) {
        let s2c_client = self.s2c_client.clone();
        let s2c_log = self.s2c_log.clone();
        let node_id = self.follower_node_identity.clone();
        let s2c_options = self.s2c_options;
        let start_index = self.follower_index;
        let token = self.cancellation_token.clone();
        let leader_index_rx = self
            .commit_index_rx
            .take()
            .expect("leader_index_rx already taken");
        self.task_tracker
            .spawn(async move {
                let work = async {
                    if let Err(e) = Self::connect(&s2c_client, &node_id, &s2c_options.retry).await {
                        error!("Couldn't connect to client {}", e);
                        return;
                    }
                    Self::replicate(
                        &s2c_client,
                        s2c_log,
                        s2c_options,
                        start_index,
                        leader_index_rx,
                    )
                    .await;
                };
            })
            .with_cancellation_token(&self.cancellation_token)
            .await;
    }

    pub async fn stop(&self) {
        self.cancellation_token.cancel();
        self.task_tracker.close();
        self.task_tracker.wait().await;
        self.s2c_client.close().await;
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    async fn connect(
        s2c_client: &S2cClient,
        node_identity: &NodeIdentity,
        s2c_retry_options: &S2cRetryOptions,
    ) -> Result<(), ClientError> {
        let mut backoff = BackoffCounter::new(s2c_retry_options);
        loop {
            match s2c_client.connect(node_identity).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    debug!("Error while connecting..");
                    if backoff.can_attempt() {
                        backoff.await_attempt().await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    async fn replicate(
        s2c_client: &S2cClient,
        s2c_log: Arc<S2cLog>,
        s2c_options: &S2cOptions,
        follower_index: u64,
        mut leader_index_rx: watch::Receiver<u64>,
    ) {
        let mut follower_index = follower_index;
        loop {
            match leader_index_rx.changed().await {
                Ok(leader_index) => {
                    let mut backoff = BackoffCounter::new(&s2c_options.retry);
                    while follower_index <= *leader_index_rx.borrow_and_update() {
                        let next_index = follower_index + 1;
                        loop {
                            let bytes = match s2c_log.get_at(next_index).await {
                                Ok(bytes) => bytes,

                                Err(e) => {
                                    error!(
                                        "Error while reading log entry {:?}. Ending replicator",
                                        e
                                    );
                                    return;
                                }
                            };
                            let msg = match bytes {
                                Some(bytes) => S2cMessage {
                                    correlation_id: Uuid::new_v4().to_string(),
                                    body: Some(Body::ReplicateRequest(ReplicateRequest {
                                        log_entry: bytes.clone(),
                                        follower_too_far_behind_error: None,
                                    })),
                                    error: None,
                                },
                                None => S2cMessage {
                                    correlation_id: Uuid::new_v4().to_string(),
                                    body: Some(Body::ReplicateRequest(ReplicateRequest {
                                        log_entry: Bytes::new(),
                                        follower_too_far_behind_error: Some(
                                            FollowerTooFarBehindError {},
                                        ),
                                    })),
                                    error: None,
                                },
                            };

                            match s2c_client
                                .send(
                                    msg.correlation_id.clone(),
                                    msg.encode_to_vec().into(),
                                    s2c_options.request_timeout_ms as usize,
                                )
                                .await
                            {
                                Ok(response) => {
                                    backoff = BackoffCounter::new(&s2c_options.retry);
                                    if let Some(Body::ReplicateResponse(r)) = response.body {
                                        follower_index = r.apply_index;
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Error while replicating {:?}", e);
                                    if backoff.can_attempt() {
                                        backoff.await_attempt().await;
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    // channel closed
                }
            }
        }
    }
}
