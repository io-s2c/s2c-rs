use std::collections::HashMap;
use crate::statemachine::S2cStateMachine;

pub struct Rsm {
    apply_index: u64,
    state_machines: HashMap<String, S2cStateMachine>
}

impl Rsm {
    pub async fn apply() {

    }
}