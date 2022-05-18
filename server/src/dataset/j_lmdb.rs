use super::Dataset;
use super::DatasetRef;
use crate::process::decode_resize_224_opencv;
use crate::process::msg_unpack;
use crate::process::MsgObject;
use crate::proto::dataset::{CreateDatasetRequest, DataItem};
use crate::proto::job::data::DataType;
use crate::proto::job::Condition;
use crate::proto::job::Data;
use lmdb::Database;
use lmdb::EnvironmentFlags;
use lmdb::Transaction;
use std::path::Path;
use std::{fmt::Debug, sync::Arc};
#[derive(Debug)]
struct LmdbDataset {
    items: Vec<DataItem>,
    id: u64,
    env: Arc<lmdb::Environment>,
    db: Database,
}

pub fn from_proto(request: CreateDatasetRequest, id: u64) -> DatasetRef {
    let location = request.location;
    let items = request.items;
    let p = Path::new(&location);
    let env = lmdb::Environment::new()
        .set_flags(
            EnvironmentFlags::NO_SUB_DIR
                | EnvironmentFlags::READ_ONLY
                | EnvironmentFlags::NO_MEM_INIT
                | EnvironmentFlags::NO_LOCK
                | EnvironmentFlags::NO_SYNC,
        )
        .open_with_permissions(p, 0o600)
        .unwrap();
    Arc::new(LmdbDataset {
        items,
        id,
        db: env.open_db(None).unwrap(),
        env: Arc::new(env),
    })
}

#[inline]
fn preprocess<'a>(data: &'a [u8], key: &str) -> (u64, Vec<u8>) {
    let data = msg_unpack(data);
    let data = match &data[0] {
        MsgObject::Array(data) => data,
        _ => unimplemented!(),
    };
    let image = &data[0];
    let label = match data[1].as_ref() {
        &MsgObject::UInt(b) => b,
        _ => unimplemented!("label error, key: {} {:?}", key, data[0]),
    };
    let content = match image.as_ref() {
        MsgObject::Map(map) => &map["data"],
        err => unimplemented!("image error, key:{} {:?}", key, err),
    };
    let data = match *content.as_ref() {
        MsgObject::Bin(bin) => bin,
        _ => unimplemented!(),
    };
    (label, decode_resize_224_opencv(data))
}

impl Dataset for LmdbDataset {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_indices(&self, cond: Option<Condition>) -> Vec<u32> {
        let start = 0u32;
        let end = self.items.len() as u32;
        match cond {
            Some(cond) => (start..end)
                .filter(|x| cond.eval(self.items[*x as usize].keys[0].as_str()))
                .collect::<Vec<_>>(),
            None => (start..end).collect::<Vec<_>>(),
        }
    }

    fn read(&self, idx: u32) -> Arc<Vec<Data>> {
        let txn = self.env.begin_ro_txn().unwrap();
        let key = self.items[idx as usize].keys[0].clone();
        let data: &[u8] = txn.get(self.db, &key.to_string()).unwrap();
        let (label, image) = preprocess(data.as_ref(), &key);
        let label = Data {
            bs: label.to_be_bytes().to_vec(),
            ty: DataType::Uint as i32,
        };
        let data = Data {
            bs: image,
            ty: DataType::Image as i32,
        };
        Arc::new(vec![label, data])
    }

    fn len(&self) -> usize {
        self.items.len() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::build_dataset;
    use test::Bencher;
    extern crate test;
    #[bench]
    fn test_bench(b: &mut Bencher) {
        b.iter(|| test_tensor());
    }

    #[test]
    fn test_tensor() {
        let len = 4096;
        let location = "data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
        let name = "lmdb".to_string();
        let items = (0..len)
            .map(|x| DataItem {
                keys: vec![x.to_string()],
            })
            .collect::<Vec<_>>();
        let proto = CreateDatasetRequest {
            name,
            location,
            r#type: crate::proto::dataset::create_dataset_request::Type::Lmdb as i32,
            items,
            weights: vec![0],
        };
        let dataset = build_dataset(proto, 0);
        dataset.read(0);
    }
}
