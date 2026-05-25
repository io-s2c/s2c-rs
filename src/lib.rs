pub mod network;
mod context;
mod config;
mod error;

pub mod proto {
    pub mod io {
        include!(concat!(env!("OUT_DIR"), "/io.s2c.model.rs"));
    }
}