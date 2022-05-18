mod dataset_svc;
use std::{collections::HashMap, sync::Arc};

pub use dataset_svc::*;
mod job_svc;
pub use job_svc::*;
mod common;
pub use common::*;
use tokio::sync::Mutex;

pub type IDTable = Arc<Mutex<HashMap<String, u64>>>;
