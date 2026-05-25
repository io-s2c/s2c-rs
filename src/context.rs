use crate::proto::io::{NodeIdentity, S2cMessage};

pub struct Context {
    s2c_group_id: String,
    port_str: String,
    log_node_identity: bool,
    node_identity: NodeIdentity,
}

impl Context {
    pub fn new(s2c_group_id: impl Into<String>, node_identity: NodeIdentity, log_node_identity: bool) -> Self {

        Self {
            s2c_group_id: s2c_group_id.into(),
            port_str: node_identity.port.to_string(),
            log_node_identity,
            node_identity,
        }

    }

    pub fn s2c_group_id(&self) -> &str {
        &self.s2c_group_id
    }

    pub fn node_identity(&self) -> &NodeIdentity {
        &self.node_identity
    }
    pub fn log_node_identity(&self) -> bool {
        self.log_node_identity
    }

    pub fn as_vec(&self) -> Vec<(&'static str, &str)> {
        let mut vec = vec![("s2c_group_id", self.s2c_group_id.as_str())];
        if self.log_node_identity {
            vec.push(("node_id", self.node_identity.id.as_str()));
            vec.push(("node_address", self.node_identity.address.as_str()));
            vec.push(("node_port", self.port_str.as_str()))
        }
        vec
    }
}
