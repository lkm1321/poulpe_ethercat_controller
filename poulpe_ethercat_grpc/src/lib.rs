pub mod pb {
    tonic::include_proto!("poulpe");
}

pub mod client;
mod feedback;
mod log_throttle;
pub use client::PoulpeRemoteClient;

pub mod server;
pub use server::launch_server;
