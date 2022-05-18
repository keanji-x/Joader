mod j_lmdb;
pub use j_lmdb::*;
mod dummy;
use crate::proto::{dataset::{create_dataset_request::Type, CreateDatasetRequest}, job::Condition};
pub use dummy::*;
use std::{fmt::Debug, sync::Arc};
use crate::proto::job::Data;
pub trait Dataset: Sync + Send + Debug {
    fn get_id(&self) -> u64;
    fn get_indices(&self, cond: Option<Condition>) -> Vec<u32>;
    fn read(&self, _idx: u32) -> Arc<Vec<Data>> {todo!()}
    fn len(&self) -> usize;
}
pub type DatasetRef = Arc<dyn Dataset>;

pub fn build_dataset(request: CreateDatasetRequest, dataset_id: u64) -> DatasetRef {
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
