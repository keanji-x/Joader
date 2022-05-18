use super::Dataset;
use super::DatasetRef;
use crate::proto::job::{Data, data::DataType};
use crate::proto::dataset::{CreateDatasetRequest, DataItem};
use std::{fmt::Debug, sync::Arc};

#[derive(Clone, Default, Debug)]
struct DummyDataset {
    _magic: u8,
    items: Vec<DataItem>,
    id: u64,
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

pub fn from_proto(request: CreateDatasetRequest, id: u64) -> DatasetRef {
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
    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_indices(&self) -> Vec<u32> {
        let start = 0u32;
        let end = self.items.len() as u32;
        (start..end).collect::<Vec<_>>()
    }

    fn read(&self, idx: u32) -> Arc<Vec<Data>> {
        let data = Data {
            bs: idx.to_be_bytes().to_vec(),
            ty: DataType::Uint as i32,
        };
        Arc::new(vec![data])
    }

    fn len(&self) -> usize {
        self.items.len()
    }
}
