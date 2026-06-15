use crate::backoff::BackoffCounter;
use crate::config::{S2cOptions, S2cRetryOptions};
use crate::network::group_server::GroupServer;
use crate::network::message_rw::{write_message, S2cMessageReader};
use crate::network::server::ServerState::{Binding, Closed, NotBound, Ready};
use crate::proto::io::s2c_message::Body;
use crate::proto::io::{Handshake, NodeIdentity, S2cMessage};
use std::collections::HashMap;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[repr(u8)]
enum ServerState {
    NotBound,
    Binding,
    Ready,
    Closed,
}

#[derive(Debug)]
struct AtomicServerState(AtomicU8);

impl AtomicServerState {
    pub fn new() -> Self {
        Self(AtomicU8::new(NotBound as u8))
    }

    pub fn load(&self) -> ServerState {
        match self.0.load(Ordering::Acquire) {
            0 => NotBound,
            1 => Binding,
            2 => Ready,
            _ => Closed,
        }
    }

    pub fn store(&self, server_state: ServerState) {
        self.0.store(server_state as u8, Ordering::Release)
    }

    pub fn compare_and_exchange(&self, current: ServerState, new: ServerState) -> bool {
        self.0
            .compare_exchange(
                current as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

type GroupServers<T, F> = Arc<Mutex<HashMap<String, GroupServer<T, F>>>>;

struct S2cServer<T, F>
where
    T: S2cMessageReader + Send + 'static,
    F: Fn() -> T,
{
    s2c_options: &'static S2cOptions,
    group_servers: GroupServers<T, F>,
    server_state: Arc<AtomicServerState>,
    node_identity: NodeIdentity,
    message_reader_factory: Arc<F>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
}

impl<T, F> S2cServer<T, F>
where
    T: S2cMessageReader + Send + 'static,
    F: Fn() -> T + Send + Sync + 'static,
{
    pub fn new(
        s2c_options: &'static S2cOptions,
        node_identity: NodeIdentity,
        message_reader_factory: F,
    ) -> Self {
        Self {
            s2c_options,
            group_servers: Arc::new(Mutex::new(HashMap::new())),
            server_state: Arc::new(AtomicServerState::new()),
            node_identity,
            message_reader_factory: Arc::new(message_reader_factory),
            task_tracker: TaskTracker::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub async fn start(&mut self) -> Result<(), std::io::Error> {
        if self.server_state.compare_and_exchange(NotBound, Binding) {
            let server_state = self.server_state.clone();
            let port = self.node_identity.port;
            let s2c_options = self.s2c_options;
            let tcp_listener = bind(self.node_identity.port, &self.s2c_options.retry).await?;
            let cancellation_token = self.cancellation_token.clone();
            let message_reader_factory = self.message_reader_factory.clone();
            let group_servers = self.group_servers.clone();
            server_state.store(Ready);
            self.task_tracker.spawn(async move {
                Self::serve(
                    server_state,
                    s2c_options,
                    port,
                    tcp_listener,
                    cancellation_token,
                    message_reader_factory,
                    group_servers,
                )
                .await
            });

            Ok(())
        } else {
            tracing::warn!("Wrong state for starting server {:?}", self.server_state);
            Ok(())
        }
    }

    async fn serve(
        server_state: Arc<AtomicServerState>,
        s2c_options: &'static S2cOptions,
        port: i32,
        tcp_listener: TcpListener,
        cancellation_token: CancellationToken,
        message_reader_factory: Arc<F>,
        group_servers: GroupServers<T, F>,
    ) {
        let inner_task_tracker = TaskTracker::new();
        let inner_cancellation_token = CancellationToken::new();

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                inner_cancellation_token.cancel();
                inner_task_tracker.close();
                inner_task_tracker.wait().await;
            }
            _ = Self::do_serve(server_state, s2c_options, port, &inner_task_tracker, tcp_listener, &inner_cancellation_token, message_reader_factory, group_servers) => {

            }
        }
    }

    async fn do_serve(
        server_state: Arc<AtomicServerState>,
        s2c_options: &'static S2cOptions,
        port: i32,
        task_tracker: &TaskTracker,
        tcp_listener: TcpListener,
        cancellation_token: &CancellationToken,
        message_reader_factory: Arc<F>,
        group_servers: GroupServers<T, F>,
    ) {
        let mut tcp_listener = tcp_listener;
        loop {
            start_accept_loop(
                &tcp_listener,
                &*server_state,
                task_tracker,
                &cancellation_token,
                &message_reader_factory,
                s2c_options,
                group_servers.clone(),
            )
            .with_cancellation_token(&cancellation_token)
            .await;
            if !server_state.compare_and_exchange(Ready, NotBound) {
                return;
            }
            if server_state.compare_and_exchange(NotBound, Binding) {
                match bind(port, &s2c_options.retry).await {
                    Ok(listener) => {
                        tcp_listener = listener;
                        server_state.store(Ready);
                    }
                    Err(err) => {
                        tracing::error!("Error while rebinding {}. Closing client", err);
                        server_state.store(Closed);
                        cancellation_token.cancel();
                        task_tracker.close();
                        // Await all client handlers.
                        task_tracker.wait().await;
                        return;
                    }
                }
            } else {
                tracing::debug!("Couldn't rebind {:?}", server_state);
                return;
            }
        }
    }

    pub async fn register_group_server(&self, group_id: String, group_server: GroupServer<T, F>) {
        self.group_servers
            .lock()
            .await
            .insert(group_id, group_server);
    }

    pub async fn close(&self) {
        if matches!(self.server_state.load(), Closed) {
            tracing::warn!("Server already closed");
            return;
        }
        self.server_state.store(Closed);
        self.cancellation_token.cancel();
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }
}

async fn bind(
    port: i32,
    s2c_retry_options: &S2cRetryOptions,
) -> Result<TcpListener, std::io::Error> {
    let mut backoff = BackoffCounter::new("bind", &s2c_retry_options);
    tracing::debug!("Server binding");
    loop {
        match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
            Ok(listener) => return Ok(listener),
            Err(err) => {
                if backoff.can_attempt() {
                    backoff.await_attempt().await;
                } else {
                    return Err(err);
                }
            }
        }
    }
}
async fn start_accept_loop<F, T>(
    tcp_listener: &TcpListener,
    server_state: &AtomicServerState,
    task_tracker: &TaskTracker,
    cancellation_token: &CancellationToken,
    message_reader_factory: &Arc<F>,
    s2c_options: &'static S2cOptions,
    group_servers: GroupServers<T, F>,
) where
    T: S2cMessageReader + Send,
    F: Fn() -> T + Send + Sync + 'static,
{
    loop {
        if matches!(server_state.load(), Closed) {
            return;
        }
        let re = tcp_listener.accept().await;
        if matches!(server_state.load(), Closed) {
            return;
        }
        match re {
            Ok((tcp_stream, sock_addr)) => {
                let cancellation_token = cancellation_token.clone();
                let message_reader_factory = message_reader_factory.clone();
                let group_servers = group_servers.clone();
                task_tracker.spawn(async move {
                    handle_client(
                        tcp_stream,
                        sock_addr,
                        message_reader_factory,
                        s2c_options,
                        group_servers,
                    )
                    .with_cancellation_token(&cancellation_token)
                    .await;
                });
            }
            Err(err) => {
                tracing::error!("Error accepting connection {}. Exiting accept loop", err);
                return;
            }
        }
    }
}

async fn handle_client<T, F>(
    tcp_stream: TcpStream,
    sock_addr: SocketAddr,
    message_reader_factory: Arc<F>,
    s2c_options: &S2cOptions,
    group_servers: GroupServers<T, F>,
) where
    T: S2cMessageReader + Send,
    F: Fn() -> T,
{
    let (read, write) = tcp_stream.into_split();
    let mut reader = BufReader::new(read);
    let mut writer = BufWriter::new(write);
    if let Some(handshake) = handshake(
        &mut reader,
        message_reader_factory(),
        &mut writer,
        s2c_options.network.handshake_timeout_ms,
    )
    .await
    {
        match group_servers.lock().await.get(&handshake.group_id) {
            Some(group_server) => {
                if let Some(node_identity) = handshake.node_identity {
                    let _ = group_server.handle_client(node_identity, reader, writer);
                } else {
                    tracing::error!("Invalid handshake missing NodeIdentity")
                }
            }
            None => {
                tracing::error!("Invalid handshake with unknown group")
            }
        }
    } else {
        tracing::debug!("Couldn't handshake client. Dropping connection");
    }
}

async fn write(
    writer: &mut BufWriter<OwnedWriteHalf>,
    msg: S2cMessage,
) -> Result<(), std::io::Error> {
    write_message(writer, msg, &mut Vec::new()).await?;
    writer.flush().await
}

async fn handshake<T>(
    reader: &mut BufReader<OwnedReadHalf>,
    mut message_reader: T,
    writer: &mut BufWriter<OwnedWriteHalf>,
    timeout: u64,
) -> Option<Handshake>
where
    T: S2cMessageReader,
{
    loop {
        match time::timeout(
            Duration::from_millis(timeout),
            message_reader.read_next_message(reader),
        )
        .await
        {
            Ok(Ok(mut msg)) => {
                let msg2 = msg.clone();
                if let Some(Body::Handshake(handshake)) = msg.body.take() {
                    tracing::debug!("Handshake received: {:?}", handshake);
                    return match write(writer, msg2).await {
                        Ok(_) => {
                            tracing::debug!("Handshake sent");
                            Some(handshake)
                        }
                        Err(_) => {
                            tracing::debug!("Failed sending handshake");
                            None
                        }
                    };
                } else {
                    tracing::warn!("Skipping unexpected non-handshake message {:?}", msg);
                }
            }
            Ok(Err(e)) => {
                tracing::error!("Error receiving handshake message: {}", e);
                return None;
            }
            Err(elapsed) => {
                tracing::debug!("Handshake timed out: {:?}", elapsed);
                return None;
            }
        }
    }
}
