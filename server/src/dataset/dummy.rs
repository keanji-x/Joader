use super::Dataset;
use super::DatasetRef;
use crate::{
    cache::cache::Cache,
    proto::dataset::{CreateDatasetRequest, DataItem},
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::{fmt::Debug, sync::Arc};

#[derive(Clone, Default, Debug)]
struct DummyDataset {
    _magic: u8,
    items: Vec<DataItem>,
    id: u32,
}

pub fn new_dummy(len: usize, _name: String) -> DatasetRef {
    let mut items = Vec::new();
    for i in 0..len {
        items.push(DataItem {
            keys: vec![i.to_string()],
        })
    }
    Arc::new(DummyDataset {
        _magic: 7u8,
        items,
        id: 0,
    })
}

pub fn from_proto(request: CreateDatasetRequest, id: u32) -> DatasetRef {
    let items = request.items;
    Arc::new(DummyDataset {
        items,
        _magic: 7u8,
        id,
    })
}

fn _len() -> usize {
    256
}

impl Dataset for DummyDataset {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_indices(&self) -> Vec<u32> {
        let start = 0u32;
        let end = self.items.len() as u32;
        (start..end).collect::<Vec<_>>()
    }

    fn read_batch(
        &self,
        _cache: Arc<Mutex<Cache>>,
        batch_data: HashMap<u32, (usize, usize)>,
    ) -> Vec<(u32, u64)> {
        batch_data
            .iter()
            .map(|(&data_idx, (_, _))| (data_idx, data_idx as u64))
            .collect::<Vec<_>>()
    }

    fn read_decode_batch(
        &self,
        _cache: Arc<Mutex<Cache>>,
        batch_data: HashMap<u32, (usize, usize)>,
    ) -> Vec<(u32, u64)> {
        batch_data
            .iter()
            .map(|(&data_idx, (_, _))| (data_idx, data_idx as u64))
            .collect::<Vec<_>>()
    }

    fn len(&self) -> u64 {
        self.items.len() as u64
    }
}
