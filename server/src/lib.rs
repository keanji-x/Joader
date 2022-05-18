#![feature(get_mut_unchecked)]
#![feature(cursor_remaining)]
#![feature(associated_type_defaults)]
#![feature(test)]
pub mod job;
pub mod cache;
pub mod process;
pub mod proto;
pub mod sampler;
pub mod service;
pub mod joader;
pub mod dataset;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Role {
    Leader,
    Follower,
}
