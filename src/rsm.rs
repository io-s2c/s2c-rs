use std::collections::HashMap;
use crate::statemachine::S2cStateMachine;

pub struct RSM {
    apply_index: u64,
    state_machines: HashMap<String, S2cStateMachine>
}

impl RSM {
    pub async fn apply() {

    }
}