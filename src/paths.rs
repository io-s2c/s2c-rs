use std::arch::aarch64::vmaxv_u32;

pub struct KeysResolver {
    group_prefix: String,
    log_prefix: String,
    snapshot: String
}

impl KeysResolver {
    pub fn new(group_id: String) -> Self {
        let group_prefix = format!("s2c/{}/", group_id);
        let snapshot = format!("{}snapshot.pb", group_prefix);
        let log_prefix = format!("{}log/", group_prefix);
        Self {
            group_prefix,
            snapshot,
            log_prefix,
        }
    }

    pub fn group_prefix(&self) -> &str {
        &self.group_prefix
    }
    pub fn log_prefix(&self) -> &str {
        &self.log_prefix
    }
    pub fn log_entry(&self, index: u64) -> String {
        format!("{0}{index}", self.log_prefix())
    }

    pub fn snapshot(&self) -> &str {
        &self.snapshot
    }

    pub fn invert(index: u64) -> u64 {
        u64::MAX - index
    }
}
