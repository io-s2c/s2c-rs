pub struct S2cOptions {
    pub max_message_size: u64,
    pub snapshotting_threshold: u32,
    pub flush_interval_ms: u64,
    pub batch_min_count: u32,
    pub request_timeout_ms: u64,
    pub leader_heartbeat_timeout_ms: u64,
    pub max_missed_heartbeats: u32,
    pub max_concurrent_state_requests_handling: u32,
    pub leadership_delay_ms: u64,
    pub log_lru_cache_size: u32,
    pub max_batches_pending_for_apply: u32,
    pub max_deduplicated_clients: u32,
    pub read_index: bool,
    pub log_node_identity: bool,
    pub network: S2cNetworkOptions,
    pub retry: S2cRetryOptions,
    pub exactly_once: S2cExactlyOnceOptions,
}

impl Default for S2cOptions {
    fn default() -> Self {
        Self {
            max_message_size: 500 * 1024,
            snapshotting_threshold: 100,
            flush_interval_ms: 2_000,
            batch_min_count: 100,
            request_timeout_ms: 10_000,
            leader_heartbeat_timeout_ms: 5_000,
            max_missed_heartbeats: 10,
            max_concurrent_state_requests_handling: 100,
            leadership_delay_ms: 3_000,
            log_lru_cache_size: 1_000,
            max_batches_pending_for_apply: 1_000,
            max_deduplicated_clients: 1_000,
            read_index: true,
            log_node_identity: false,
            network: S2cNetworkOptions::default(),
            retry: S2cRetryOptions::default(),
            exactly_once: S2cExactlyOnceOptions::default(),
        }
    }
}

pub struct S2cNetworkOptions {
    pub connect_timeout_ms: u64,
    pub throttle_delay_ms: u64,
    pub handshake_timeout_ms: u64,
    pub max_pending_reqs_per_client: u32,
    pub max_pending_resps_per_client: u32,
    pub max_pending_server_responses: u32,
}

impl Default for S2cNetworkOptions {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            throttle_delay_ms: 10_000,
            handshake_timeout_ms: 10_000,
            max_pending_reqs_per_client: 1_000,
            max_pending_resps_per_client: 1_000,
            max_pending_server_responses: 1_000,
        }
    }
}

#[derive(Copy, Clone)]
pub struct S2cRetryOptions {
    pub max_delay_seconds: u64,
    pub base_delay_ms: u64,
    pub max_attempts: Option<u32>,
}

impl Default for S2cRetryOptions {
    fn default() -> Self {
        Self {
            max_delay_seconds: 5,
            base_delay_ms: 300,
            max_attempts: Some(3),
        }
    }
}

pub struct S2cExactlyOnceOptions {
    pub out_of_seq_buffer_size: u32,
    pub out_of_seq_buffer_gc_delay_sec: u64,
    pub enabled: bool,
}

impl Default for S2cExactlyOnceOptions {
    fn default() -> Self {
        Self {
            out_of_seq_buffer_size: 1_000,
            out_of_seq_buffer_gc_delay_sec: 30,
            enabled: true,
        }
    }
}
