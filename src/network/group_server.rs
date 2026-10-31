use crate::config::S2cOptions;
use crate::fsm::MessageReaderFactory;
use crate::network::client::MessageReader;
use crate::network::message_rw::{write_bytes, S2cMessageReader};
use crate::proto::io::{NodeIdentity, S2cMessage};
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{unbounded_channel, Receiver, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;

pub struct S2cMessageEnvelope {
    pub s2c_message: S2cMessage,
    pub node_identity: Arc<NodeIdentity>,
}

pub struct GroupServer {
    group_id: String,
    message_reader_factory: MessageReaderFactory,
    all_in: Arc<UnboundedSender<S2cMessageEnvelope>>,
    all_out: Arc<Mutex<Receiver<S2cMessageEnvelope>>>,
    running: Arc<AtomicBool>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    inner_cancellation_token: CancellationToken,
    clients_ins: Arc<RwLock<HashMap<Arc<NodeIdentity>, Arc<UnboundedSender<S2cMessageEnvelope>>>>>,
    s2c_options: &'static S2cOptions,
}

impl GroupServer {
    pub fn new(
        group_id: String,
        message_reader_factory: MessageReaderFactory,
        all_in: UnboundedSender<S2cMessageEnvelope>,
        all_out: Receiver<S2cMessageEnvelope>,
        s2c_options: &'static S2cOptions,
    ) -> Self {
        Self {
            group_id,
            message_reader_factory,
            all_in: Arc::new(all_in),
            all_out: Arc::new(Mutex::new(all_out)),
            running: Arc::new(AtomicBool::new(false)),
            cancellation_token: CancellationToken::new(),
            inner_cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            clients_ins: Arc::new(RwLock::new(HashMap::new())),
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
            let all_out = self.all_out.clone();
            let cancellation_token = self.cancellation_token.clone();
            let inner_cancellation_token = self.inner_cancellation_token.clone();
            let clients_map = self.clients_ins.clone();
            self.task_tracker.spawn(async move {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        inner_cancellation_token.cancel();
                    }
                    _ = Self::inner_drain_out(all_out, running, clients_map)
                    .with_cancellation_token(&cancellation_token)
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
        writer: BufWriter<OwnedWriteHalf>,
    ) {
        if self.running.load(Acquire) {
            let node_identity = Arc::new(node_identity);
            let mut map = self.clients_ins.write().await;
            if map.get(&node_identity).is_some() {
                return;
            }
            let (sender, receiver) = unbounded_channel::<S2cMessageEnvelope>();
            map.insert(node_identity.clone(), Arc::new(sender));
            drop(map);

            let message_reader = (self.message_reader_factory)();
            let all_in = self.all_in.clone();
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
                    all_in,
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
        mut message_reader: MessageReader,
        all_in: Arc<UnboundedSender<S2cMessageEnvelope>>,
        running: Arc<AtomicBool>,
        node_identity: Arc<NodeIdentity>,
        cancellation_token: CancellationToken,
    ) {
        while running.load(Acquire) {
            match message_reader.read_next_message(&mut reader).await {
                Ok(msg) => {
                    if !running.load(Acquire) {
                        break;
                    }
                    let _ = all_in.send(S2cMessageEnvelope {
                        s2c_message: msg,
                        node_identity: node_identity.clone(),
                    });
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
        let mut buf = BytesMut::with_capacity(1024);
        while running.load(Acquire) {
            if let Some(msg) = receiver.recv().await {
                buf.resize(msg.s2c_message.encoded_len(), 0u8);
                if let Err(e) = msg.s2c_message.encode(&mut buf) {
                    error!("Error while encoding message {:?}", msg.s2c_message);
                    continue;
                }
                if let Err(err) = write_bytes(&mut writer, buf.split().freeze()).await {
                    tracing::debug!(err = %err, "Error while writing response. Exiting loop");
                    cancellation_token.cancel();
                    break;
                }
            }
        }
    }

    async fn inner_drain_out(
        all_out: Arc<Mutex<Receiver<S2cMessageEnvelope>>>,
        running: Arc<AtomicBool>,
        clients_ins: Arc<
            RwLock<HashMap<Arc<NodeIdentity>, Arc<UnboundedSender<S2cMessageEnvelope>>>>,
        >,
    ) {
        let mut local_client_ins: HashMap<
            Arc<NodeIdentity>,
            Arc<UnboundedSender<S2cMessageEnvelope>>,
        > = HashMap::new();
        let mut all_out = all_out.lock().await;
        while running.load(Ordering::Relaxed) {
            match all_out.recv().await {
                None => {
                    break;
                }
                Some(msg) => match local_client_ins.get(&msg.node_identity.clone()) {
                    Some(sender) => {
                        let _ = sender.send(msg);
                    }
                    None => match clients_ins.read().await.get(&msg.node_identity) {
                        Some(sender) => {
                            local_client_ins.insert(msg.node_identity, sender.clone());
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
