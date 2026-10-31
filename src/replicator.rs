// use crate::backoff::BackoffCounter;
// use crate::config::{S2cOptions, S2cRetryOptions};
// use crate::context::Context;
// use crate::fsm::MessageReaderFactory;
// use crate::network::client::S2cClient;
// use crate::network::error::ClientError;
// use crate::proto::io::{LogEntry, NodeIdentity, S2cMessage};
// use moka::policy::EvictionPolicy;
// use moka::sync::{Cache, CacheBuilder};
// use std::sync::Arc;
// use std::sync::atomic::AtomicU64;
// use prost::Message;
// use tokio_util::bytes::Bytes;
// use tokio_util::future::FutureExt;
// use tokio_util::sync::CancellationToken;
// use tokio_util::task::TaskTracker;
// use tracing::{debug, error};
// use uuid::Uuid;
// use log::log;
// use crate::log::S2cLog;
// use crate::proto::io::s2c_message::Body;
// use crate::proto::io::s2c_message::Body::ReplicateRequest;
//
// pub struct Replicator {
//     s2c_client: Arc<S2cClient>,
//     follower_node_identity: NodeIdentity,
//     s2c_log: Arc<S2cLog>,
//     s2c_options: &'static S2cOptions,
//     context: &'static Context,
//     cancellation_token: CancellationToken,
//     task_tracker: TaskTracker,
//     start_index: u64
// }
//
// impl Replicator {
//     pub fn new(
//         &self,
//         s2c_log: Arc<S2cLog>,
//         follower_node_identity: NodeIdentity,
//         cache: &'static Cache<String, (String, Bytes)>,
//         s2c_options: &'static S2cOptions,
//         context: &'static Context,
//         message_reader_factory: Arc<MessageReaderFactory>,
//         start_index: u64,
//     ) -> Self {
//         Self {
//             s2c_log,
//             follower_node_identity,
//             s2c_client: Arc::new(S2cClient::new(
//                 self.s2c_options,
//                 self.context,
//                 message_reader_factory,
//             )),
//             s2c_options,
//             context,
//             start_index,
//             cancellation_token: CancellationToken::new(),
//             task_tracker: TaskTracker::new(),
//         }
//     }
//
//     pub fn start(&self) {
//         let s2c_client = self.s2c_client.clone();
//         let node_id = self.follower_node_identity.clone();
//         let retry_options = self.s2c_options.retry;
//         let _ = self
//             .task_tracker
//             .spawn(async move {
//                 match Self::connect(&s2c_client, &node_id, &retry_options).await {
//                     Ok(_) => {}
//                     Err(e) => {
//                         error!("Couldn't connect to client {}", e);
//                         return;
//                     }
//                 }
//             })
//             .with_cancellation_token(&self.cancellation_token);
//     }
//
//     async fn connect(
//         s2c_client: &S2cClient,
//         node_identity: &NodeIdentity,
//         s2c_retry_options: &S2cRetryOptions,
//     ) -> Result<(), ClientError> {
//         let mut backoff = BackoffCounter::new(s2c_retry_options);
//         loop {
//             match s2c_client.connect(&node_identity).await {
//                 Ok(_) => (),
//                 Err(e) => {
//                     debug!("Error while connecting..");
//                     if backoff.can_attempt() {
//                         backoff.await_attempt().await;
//                     } else {
//                         return Err(e);
//                     }
//                 }
//             }
//         }
//     }
//
//     async fn replicate(s2c_client: &S2cClient, s2c_log: Arc<S2cLog>, s2c_options: &S2cOptions, start_index: u64) {
//         let mut start_index = start_index;
//         loop {
//             match s2c_log.get_at(start_index).await {
//                 Ok(bytes) => {
//                     let backoff = BackoffCounter::new(&s2c_options.retry);
//
//                     let log_entry = LogEntry::decode(bytes).expect("Invalid log entry");
//
//                     let msg = S2cMessage {
//                         correlation_id: "".to_string(),
//                         error: None,
//                         body: Some(ReplicateRequest(crate::proto::io::ReplicateRequest {
//                             batch: log_entry.batch,
//                             // commit_index: log_entry.batch.,
//                             group_id: "".to_string(),
//                             follower_too_far_behind_error: None,
//                         }))
//                     }
//
//                     match s2c_client.send(Uuid::new_v4().into(), re.clone(), s2c_options.request_timeout_ms as usize).await {
//
//                     }
//                 }
//                 Err(e) => {
//                     error!("Error while reading log entry {:?}. Ending replicator", e);
//                     return
//                 }
//             }
//         }
//     }
// }
