
mod j_lmdb;
pub use j_lmdb::*;
use std::collections::HashMap;
use std::sync::Mutex;
mod dummy;
use crate::cache::cache::Cache;
use crate::proto::dataset::{create_dataset_request::Type, CreateDatasetRequest};
pub use dummy::*;
use std::{fmt::Debug, sync::Arc};
pub trait Dataset: Sync + Send + Debug {
    fn get_id(&self) -> u32;
    fn get_indices(&self) -> Vec<u32>;
    fn read_batch(&self,
        cache: Arc<Mutex<Cache>>,
        batch_data: HashMap<u32, (usize, usize)>) -> Vec<(u32, u64)>;
    fn read_decode_batch(
        &self,
        _cache: Arc<Mutex<Cache>>,
        _batch_data: HashMap<u32, (usize, usize)>,
    ) -> Vec<(u32, u64)> {
        unimplemented!()
    }
    fn len(&self) -> u64;
}
pub type DatasetRef = Arc<dyn Dataset>;

pub fn build_dataset(request: CreateDatasetRequest, dataset_id: u32) -> DatasetRef {
    let t: Type = unsafe { std::mem::transmute(request.r#type) };
    match t {
        Type::Dummy => dummy::from_proto(request, dataset_id),
        Type::Lmdb => j_lmdb::from_proto(request, dataset_id),
        Type::Filesystem => unimplemented!(),
    }
}

pub fn data_id(dataset_id: u32, data_idx: u32) -> u64 {
    ((dataset_id as u64) << 32) + (data_idx as u64)
}
