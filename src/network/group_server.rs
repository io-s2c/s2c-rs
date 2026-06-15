use crate::config::S2cOptions;
use crate::network::message_rw::{write_message, S2cMessageReader};
use crate::proto::io::s2c_message::Body;
use crate::proto::io::s2c_message::Error::SlowDownError;
use crate::proto::io::{NodeIdentity, S2cMessage};
use std::collections::HashMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::sync::{Mutex, RwLock};
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub struct S2cMessageEnvelope {
    s2c_message: S2cMessage,
    node_identity: Arc<NodeIdentity>,
}

pub struct GroupServer<T, F>
where
    T: S2cMessageReader + Send + 'static,
    F: Fn() -> T,
{
    group_id: String,
    message_reader_factory: F,
    follow_sender: Arc<Sender<S2cMessageEnvelope>>,
    state_request_sender: Arc<Sender<S2cMessageEnvelope>>,
    synchronize_sender: Arc<Sender<S2cMessageEnvelope>>,
    inner_sender: Arc<Sender<S2cMessageEnvelope>>,
    inner_receiver: Arc<Mutex<Receiver<S2cMessageEnvelope>>>,
    running: Arc<AtomicBool>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    inner_cancellation_token: CancellationToken,
    clients_senders:
        Arc<RwLock<HashMap<Arc<NodeIdentity>, Arc<UnboundedSender<S2cMessageEnvelope>>>>>,
    s2c_options: &'static S2cOptions,
}

impl<T, F> GroupServer<T, F>
where
    T: S2cMessageReader + Send + 'static,
    F: Fn() -> T,
{
    pub fn new(
        group_id: String,
        message_reader_factory: F,
        follow_sender: Sender<S2cMessageEnvelope>,
        state_request_sender: Sender<S2cMessageEnvelope>,
        synchronize_sender: Sender<S2cMessageEnvelope>,
        inner_sender: Arc<Sender<S2cMessageEnvelope>>,
        inner_receiver: Receiver<S2cMessageEnvelope>,
        s2c_options: &'static S2cOptions,
    ) -> Self {
        Self {
            group_id,
            message_reader_factory,
            follow_sender: Arc::new(follow_sender),
            state_request_sender: Arc::new(state_request_sender),
            synchronize_sender: Arc::new(synchronize_sender),
            inner_sender,
            inner_receiver: Arc::new(Mutex::new(inner_receiver)),
            running: Arc::new(AtomicBool::new(false)),
            cancellation_token: CancellationToken::new(),
            inner_cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            clients_senders: Arc::new(RwLock::new(HashMap::new())),
            s2c_options,
        }
    }

    pub async fn start(&self) {
        if self
            .running
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_ok()
        {
            let running = self.running.clone();
            let inner_receiver = self.inner_receiver.clone();
            let cancellation_token = self.cancellation_token.clone();
            let cancellation_token2 = self.cancellation_token.clone();
            let inner_cancellation_token = self.inner_cancellation_token.clone();
            let clients_map = self.clients_senders.clone();
            self.task_tracker.spawn(async move {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        inner_cancellation_token.cancel();
                    }
                    _ = Self::inner_drain_out(inner_receiver, running, clients_map)
                    .with_cancellation_token(&cancellation_token2)
                     => {

                    }
                }
            });
        }
    }

    pub async fn handle_client(
        &self,
        node_identity: NodeIdentity,
        reader: BufReader<OwnedReadHalf>,
        mut writer: BufWriter<OwnedWriteHalf>,
    ) {
        if self.running.load(Acquire) {
            let node_identity = Arc::new(node_identity);
            let mut map = self.clients_senders.write().await;
            if map.get(&node_identity).is_some() {
                return;
            }
            let (sender, receiver) = unbounded_channel::<S2cMessageEnvelope>();
            map.insert(node_identity.clone(), Arc::new(sender));
            drop(map);

            let message_reader = (self.message_reader_factory)();
            let follow_sender = self.follow_sender.clone();
            let synchronize_sender = self.synchronize_sender.clone();
            let state_request_sender = self.state_request_sender.clone();
            let inner_sender = self.inner_sender.clone();
            let running = self.running.clone();
            let running2 = self.running.clone();
            let inner_cancellation_token = self.inner_cancellation_token.clone();
            let inner_cancellation_token2 = self.inner_cancellation_token.clone();
            let cancellation_token = self.cancellation_token.clone();
            let cancellation_token2 = self.cancellation_token.clone();
            self.task_tracker.spawn(async move {
                Self::start_read_loop(
                    reader,
                    message_reader,
                    follow_sender,
                    synchronize_sender,
                    state_request_sender,
                    inner_sender,
                    running,
                    node_identity,
                    cancellation_token,
                )
                .with_cancellation_token(&inner_cancellation_token)
                .await
            });
            self.task_tracker.spawn(async move {
                Self::drain_out(writer, receiver, running2, cancellation_token2)
                    .with_cancellation_token(&inner_cancellation_token2)
                    .await;
            });
        }
    }
    async fn start_read_loop(
        mut reader: BufReader<OwnedReadHalf>,
        mut message_reader: T,
        follow_sender: Arc<Sender<S2cMessageEnvelope>>,
        synchronize_sender: Arc<Sender<S2cMessageEnvelope>>,
        state_request_sender: Arc<Sender<S2cMessageEnvelope>>,
        inner_sender: Arc<Sender<S2cMessageEnvelope>>,
        running: Arc<AtomicBool>,
        node_identity: Arc<NodeIdentity>,
        cancellation_token: CancellationToken,
    ) {
        while running.load(Ordering::Relaxed) {
            match message_reader.read_next_message(&mut reader).await {
                Ok(msg) => {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    // follow_sender and synchronize_sender are unbounded
                    if let Some(Body::Follow(_)) = msg.body {
                        let _ = follow_sender.send(S2cMessageEnvelope {
                            s2c_message: msg,
                            node_identity: node_identity.clone(),
                        });
                    } else if let Some(Body::SynchronizeRequest(_)) = msg.body {
                        let _ = synchronize_sender.send(S2cMessageEnvelope {
                            s2c_message: msg,
                            node_identity: node_identity.clone(),
                        });
                    } else if let Some(Body::StateRequest(_)) = msg.body {
                        if let Err(err) = state_request_sender.try_send(S2cMessageEnvelope {
                            s2c_message: msg,
                            node_identity: node_identity.clone(),
                        }) {
                            if matches!(err, TrySendError::Full(_)) {
                                if let Err(err2) = inner_sender
                                    .send(S2cMessageEnvelope {
                                        s2c_message: S2cMessage {
                                            correlation_id: uuid::Uuid::new_v4().to_string(),
                                            body: None,
                                            error: Some(SlowDownError(Default::default())),
                                        },
                                        node_identity: node_identity.clone(),
                                    })
                                    .await
                                {
                                    tracing::debug!(error = %err, "Error while sending SlowDown message. Exiting read loop");
                                    break;
                                }
                            }
                        }
                    }
                }

                Err(err) => {
                    tracing::debug!(error = %err, "Error in read loop. Exiting loop");
                    cancellation_token.cancel();
                    break;
                }
            }
        }
    }

    async fn drain_out(
        mut writer: BufWriter<OwnedWriteHalf>,
        mut receiver: UnboundedReceiver<S2cMessageEnvelope>,
        running: Arc<AtomicBool>,
        cancellation_token: CancellationToken,
    ) {
        let mut buf = vec![0u8; 1024];
        while running.load(Acquire) {
            if let Some(msg) = receiver.recv().await {
                if let Err(err) = write_message(&mut writer, msg.s2c_message, &mut buf).await {
                    tracing::debug!(err = %err, "Error while writing response. Exiting loop");
                    cancellation_token.cancel();
                    break;
                }
            }
        }
    }

    async fn inner_drain_out(
        inner_receiver: Arc<Mutex<Receiver<S2cMessageEnvelope>>>,
        running: Arc<AtomicBool>,
        clients_senders: Arc<
            RwLock<HashMap<Arc<NodeIdentity>, Arc<UnboundedSender<S2cMessageEnvelope>>>>,
        >,
    ) {
        let mut local_client_senders: HashMap<
            Arc<NodeIdentity>,
            Arc<UnboundedSender<S2cMessageEnvelope>>,
        > = HashMap::new();
        let mut inner_receiver = inner_receiver.lock().await;
        while running.load(Ordering::Relaxed) {
            match inner_receiver.recv().await {
                None => {
                    break;
                }
                Some(msg) => match local_client_senders.get(&msg.node_identity.clone()) {
                    Some(sender) => {
                        let _ = sender.send(msg);
                    }
                    None => match clients_senders.read().await.get(&msg.node_identity) {
                        Some(sender) => {
                            local_client_senders.insert(msg.node_identity, sender.clone());
                        }
                        None => {
                            tracing::error!("No sender found for node identity");
                        }
                    },
                },
            }
        }
    }

    async fn close(&self) {
        if self
            .running
            .compare_exchange(true, false, AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.cancellation_token.cancel();
            self.task_tracker.close();
            self.task_tracker.wait().await;
        }
    }
}
