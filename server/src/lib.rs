#![feature(get_mut_unchecked)]
#![feature(cursor_remaining)]
#![feature(associated_type_defaults)]
pub mod cache;
pub mod dataset;
pub mod joader;
pub mod job;
pub mod loader;
pub mod local_cache;
pub mod process;
pub mod proto;
pub mod sampler;
pub mod sampler_bitmap;
pub mod service;
pub mod new_joader;
pub mod new_dataset;
pub mod new_service;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Role {
    Leader,
    Follower,
}
