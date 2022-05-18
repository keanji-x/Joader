use super::Dataset;
use super::DatasetRef;
use crate::process::decode_from_memory;
use crate::process::msg_unpack;
use crate::process::random_crop;
use crate::process::MsgObject;
use crate::proto::dataset::{CreateDatasetRequest, DataItem};
use crate::proto::job::data::DataType;
use crate::proto::job::Data;
use lmdb::Database;
use lmdb::EnvironmentFlags;
use lmdb::Transaction;
use opencv::imgcodecs::imdecode;
use opencv::prelude::Mat;
use opencv::prelude::MatTrait;
use std::path::Path;
use std::slice::from_raw_parts;
use std::{fmt::Debug, sync::Arc};
use tch::vision::imagenet::load_image_and_resize224_from_memory;
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
fn preprocess<'a>(data: &'a [u8]) -> (u64, Vec<u8>) {
    let data = msg_unpack(data);
    let data = match &data[0] {
        MsgObject::Array(data) => data,
        _ => unimplemented!(),
    };
    let image = &data[0];
    let label = match data[1].as_ref() {
        &MsgObject::UInt(b) => b,
        _ => unimplemented!(),
    };
    let content = match image.as_ref() {
        MsgObject::Map(map) => &map["data"],
        err => unimplemented!("{:?}", err),
    };
    let data = match *content.as_ref() {
        MsgObject::Bin(bin) => bin,
        _ => unimplemented!(),
    };
    let mut image = decode_from_memory(data);
    random_crop(&mut image);
    image.resize(224).unwrap();
    let data = unsafe { from_raw_parts(image.data_mut(), 224 * 224 * 3).to_vec() };
    (label, data)
}

impl Dataset for LmdbDataset {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_indices(&self) -> Vec<u32> {
        let start = 0u32;
        let end = self.items.len() as u32;
        (start..end).collect::<Vec<_>>()
    }

    fn read(&self, idx: u32) -> Arc<Vec<Data>> {
        let txn = self.env.begin_ro_txn().unwrap();
        let key = self.items[idx as usize].keys[0].clone();
        let data: &[u8] = txn.get(self.db, &key.to_string()).unwrap();
        let (label, image) = preprocess(data.as_ref());
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
    use crate::new_dataset::build_dataset;
    use std::time::SystemTime;
    #[test]
    fn test_tensor() {
        let len = 4096;
        let location = "/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
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
        let now = SystemTime::now();
        dataset.read(0);
        let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        println!("{:}", time);
    }
}
