use super::data_id;
use super::Dataset;
use super::DatasetRef;
use crate::process::get_array_head_size;
use crate::process::get_bin_size_from_len;
use crate::process::get_int_size;
use crate::process::msg_unpack;
use crate::process::MsgObject;
use crate::{
    cache::cache::Cache,
    proto::dataset::{CreateDatasetRequest, DataItem},
};
use lmdb::Database;
use lmdb::Environment;
use lmdb::EnvironmentFlags;
use lmdb::Transaction;
use opencv::imgcodecs::imdecode;
use opencv::imgproc::cvt_color;
use opencv::imgproc::COLOR_BGR2RGB;
use opencv::prelude::Mat;
use opencv::prelude::MatTrait;
use opencv::prelude::MatTraitConst;
use rmp::encode::write_array_len;
use rmp::encode::write_bin_len;
use rmp::encode::write_uint;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::slice::from_raw_parts;
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use std::{fmt::Debug, sync::Arc};
use threadpool::ThreadPool;
#[derive(Debug)]
struct LmdbDataset {
    items: Vec<DataItem>,
    id: u32,
    env: Arc<lmdb::Environment>,
    db: Database,
    pool: Arc<Mutex<ThreadPool>>,
}
pub const POOL_SIZE: usize = 32;
pub fn from_proto(request: CreateDatasetRequest, id: u32) -> DatasetRef {
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
        pool: Arc::new(Mutex::new(ThreadPool::new(POOL_SIZE))),
    })
}

#[inline]
fn process<'a>(data: &'a [u8]) -> (u64, Mat) {
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
    let mat = Mat::from_slice(data).unwrap();
    let image = imdecode(&mat, 1).unwrap();
    (label, image)
}

fn read_decode_one(
    id: u64,
    ref_cnt: usize,
    loader_cnt: usize,
    cache: Arc<Mutex<Cache>>,
    sender: Sender<(u32, u64)>,
    db: Database,
    env: Arc<Environment>,
    data_idx: u32,
    key: String,
) {
    let txn = env.begin_ro_txn().unwrap();
    let data: &[u8] = txn.get(db, &key.to_string()).unwrap();
    let (label, image) = process(data.as_ref());
    let h = image.rows();
    let w = image.cols();
    let img_size = (h * w * image.channels()) as usize;
    // |array [label, w, h, image]
    let array_size = 4;
    let array_head = get_array_head_size(array_size);

    let label_len = get_int_size(label);
    let width_len = get_int_size(w as u64);
    let height_len = get_int_size(h as u64);
    let bin_len = get_bin_size_from_len(img_size as usize);

    let len = array_head + bin_len + label_len + width_len + height_len;
    let (block_slice, idx) = {
        let mut locked_cache = cache.lock().unwrap();
        locked_cache.allocate(len, ref_cnt, id, loader_cnt)
    };
    assert_eq!(block_slice.len(), len);
    let mut writer = Cursor::new(block_slice);
    write_array_len(&mut writer, array_size as u32).unwrap();
    write_uint(&mut writer, label).unwrap();
    write_uint(&mut writer, w as u64).unwrap();
    write_uint(&mut writer, h as u64).unwrap();
    write_bin_len(&mut writer, img_size as u32).unwrap();
    let dst_slice = &mut writer.into_inner()[len - img_size as usize..];
    let mut dst = Mat::default();
    cvt_color(&image, &mut dst, COLOR_BGR2RGB, 0).unwrap();
    let raw = dst.data_mut();
    unsafe {
        let slice: &[u8] = from_raw_parts(raw, img_size);
        dst_slice.copy_from_slice(slice);
    };
    log::debug!("Read and decode data {:?} at {:?} in lmdb", id, idx);
    sender.send((data_idx, idx as u64)).unwrap();
}

fn read_one(
    id: u64,
    ref_cnt: usize,
    loader_cnt: usize,
    cache: Arc<Mutex<Cache>>,
    sender: Sender<(u32, u64)>,
    db: Database,
    env: Arc<Environment>,
    data_idx: u32,
    key: String,
) {
    let txn = env.begin_ro_txn().unwrap();
    let data: &[u8] = txn.get(db, &key.to_string()).unwrap();
    let len = data.len();
    let (block_slice, idx) = {
        let mut cache = cache.lock().unwrap();
        cache.allocate(len, ref_cnt, id, loader_cnt)
    };
    block_slice.copy_from_slice(data);
    log::debug!("Read data {:?} at {:?} in lmdb", id, idx);
    sender.send((data_idx, idx as u64)).unwrap();
}

impl Dataset for LmdbDataset {
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
        cache: Arc<Mutex<Cache>>,
        batch_data: HashMap<u32, (usize, usize)>, // idx, ref_cnt, loader_cnt
    ) -> Vec<(u32, u64)> {
        let mut ret = Vec::new();
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut producer_num = 0;
        // let now = SystemTime::now();
        let pool = self.pool.lock().unwrap();
        for (idx, (ref_cnt, loader_cnt)) in batch_data {
            let data_id = data_id(self.id, idx);
            {
                let mut cache = cache.lock().unwrap();
                if let Some(head_idx) = cache.contains_data(data_id) {
                    cache.mark_unreaded(head_idx, loader_cnt);
                    log::debug!("Hit data {:?} at {:?} in lmdb", idx, head_idx);
                    ret.push((idx, head_idx as u64));
                    continue;
                }
            }
            let thread_cache = cache.clone();
            let thread_sender = sender.clone();
            let key = self.items[idx as usize].keys[0].clone();
            let env = self.env.clone();
            let db = self.db;
            pool.execute(move || {
                read_one(
                    data_id,
                    ref_cnt,
                    loader_cnt,
                    thread_cache,
                    thread_sender,
                    db,
                    env,
                    idx,
                    key,
                )
            });
            producer_num += 1;
        }
        // let time1 = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        for _ in 0..producer_num {
            let item = receiver.recv().unwrap();
            ret.push(item);
        }
        // let time2 = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        // println!("{} {}", time1, time2 - time1);
        ret
    }

    fn read_decode_batch(
        &self,
        cache: Arc<Mutex<Cache>>,
        batch_data: HashMap<u32, (usize, usize)>, // idx, ref_cnt, loader_cnt
    ) -> Vec<(u32, u64)> {
        let mut ret = Vec::new();
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut producer_num = 0;
        // let now = SystemTime::now();
        let pool = self.pool.lock().unwrap();
        for (idx, (ref_cnt, loader_cnt)) in batch_data {
            let data_id = data_id(self.id, idx);
            {
                let mut cache = cache.lock().unwrap();
                if let Some(head_idx) = cache.contains_data(data_id) {
                    cache.mark_unreaded(head_idx, loader_cnt);
                    log::debug!("Hit data {:?} at {:?} in lmdb", idx, head_idx);
                    ret.push((idx, head_idx as u64));
                    continue;
                }
            }
            let thread_cache = cache.clone();
            let thread_sender = sender.clone();
            let key = self.items[idx as usize].keys[0].clone();
            let env = self.env.clone();
            let db = self.db;
            pool.execute(move || {
                read_decode_one(
                    data_id,
                    ref_cnt,
                    loader_cnt,
                    thread_cache,
                    thread_sender,
                    db,
                    env,
                    idx,
                    key,
                )
            });
            producer_num += 1;
        }
        // let time1 = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        for _ in 0..producer_num {
            let item = receiver.recv().unwrap();
            ret.push(item);
        }
        // let time2 = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        // println!("{} {}", time1, time2 - time1);
        ret
    }

    fn len(&self) -> u64 {
        self.items.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use lmdb::Transaction;
    use tokio::join;

    use super::*;
    use crate::cache::head::Head;
    use crate::joader::joader::Joader;
    use crate::loader::create_data_channel;
    use std::process::Command;
    use std::time::SystemTime;

    #[test]
    fn test_read_bacth() {
        println!(
            "{:?} {:?}",
            Command::new("dd")
                .arg("if=/home/xiej/nfs/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!"),
            Command::new("dd")
                .arg("if=/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!")
        );
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        let location = "/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
        let len = 32 * 128;
        let name = "DLCache".to_string();
        let cache = Arc::new(Mutex::new(Cache::new(2048 * 1024 * 1024, &name, 2048)));
        let mut items = Vec::new();
        for i in 0..len as usize {
            items.push(DataItem {
                keys: vec![i.to_string()],
            })
        }
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
        let dataset = Arc::new(LmdbDataset {
            items,
            id: 0,
            db: env.open_db(None).unwrap(),
            env: Arc::new(env),
            pool: Arc::new(Mutex::new(ThreadPool::new(POOL_SIZE))),
        });
        let now = SystemTime::now();
        let batch_size = POOL_SIZE as usize;

        for i in 0..(len / batch_size as usize) {
            let mut batch_data = HashMap::new();
            for idx in i..(i + batch_size) {
                batch_data.insert(idx as u32, (0usize, 1usize));
            }
            dataset.read_batch(cache.clone(), batch_data);
        }
        let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        println!("total{} avg{}", time, time / (len as f32));
    }
    #[test]
    fn test_read_decode_bacth() {
        println!(
            "{:?} {:?}",
            Command::new("dd")
                .arg("if=/home/xiej/nfs/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!"),
            Command::new("dd")
                .arg("if=/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!")
        );
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        let location = "/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
        let len = 32 * 128;
        let name = "DLCache".to_string();
        let cache = Arc::new(Mutex::new(Cache::new(2048 * 1024 * 1024, &name, 2048)));
        let mut items = Vec::new();
        for i in 0..len as usize {
            items.push(DataItem {
                keys: vec![i.to_string()],
            })
        }
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
        let dataset = Arc::new(LmdbDataset {
            items,
            id: 0,
            db: env.open_db(None).unwrap(),
            env: Arc::new(env),
            pool: Arc::new(Mutex::new(ThreadPool::new(POOL_SIZE))),
        });
        let now = SystemTime::now();
        let batch_size = POOL_SIZE as usize;

        for i in 0..(len / batch_size as usize) {
            let mut batch_data = HashMap::new();
            for idx in i..(i + batch_size) {
                batch_data.insert(idx as u32, (0usize, 1usize));
            }
            dataset.read_decode_batch(cache.clone(), batch_data);
        }
        let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        println!("total{} avg{}", time, time / (len as f32));
    }
    #[test]
    fn test_decode() {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        let location = "/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
        let len = 1001;
        let name = "DLCache".to_string();
        let cache = Arc::new(Mutex::new(Cache::new(1024 * 1024 * 1024, &name, 1024)));
        let mut items = Vec::new();
        for i in 0..len as usize {
            items.push(DataItem {
                keys: vec![i.to_string()],
            })
        }
        let p = Path::new(&location);
        let env = lmdb::Environment::new()
            .set_flags(EnvironmentFlags::NO_SUB_DIR | EnvironmentFlags::READ_ONLY)
            .open_with_permissions(p, 0o600)
            .unwrap();
        let dataset = Arc::new(LmdbDataset {
            items,
            id: 0,
            db: env.open_db(None).unwrap(),
            env: Arc::new(env),
            pool: Arc::new(Mutex::new(ThreadPool::new(POOL_SIZE))),
        });
        let mut batch_data = HashMap::new();
        batch_data.insert(10u32, (0usize, 1usize));
        batch_data.insert(20u32, (0usize, 1usize));
        batch_data.insert(30u32, (0usize, 1usize));
        batch_data.insert(40u32, (0usize, 1usize));
        batch_data.insert(50u32, (0usize, 1usize));
        dataset.read_decode_batch(cache, batch_data);
    }
    #[test]
    fn test_lmdb() {
        println!(
            "{:?} {:?}",
            Command::new("dd")
                .arg("if=/home/xiej/nfs/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!"),
            Command::new("dd")
                .arg("if=/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!")
        );
        let location = "/home/xiej/nfs/lmdb-imagenet/ILSVRC-train.lmdb";
        let p = Path::new(&location);
        let env = lmdb::Environment::new()
            .set_flags(
                EnvironmentFlags::NO_SUB_DIR
                    | EnvironmentFlags::READ_ONLY
                    | EnvironmentFlags::NO_META_SYNC
                    | EnvironmentFlags::NO_SYNC
                    | EnvironmentFlags::NO_MEM_INIT
                    | EnvironmentFlags::NO_LOCK
                    | EnvironmentFlags::NO_READAHEAD,
            )
            .set_max_readers(1024)
            .open_with_permissions(p, 0o400)
            .unwrap();
        let now = SystemTime::now();
        let len = 10000;
        let db = env.open_db(None).unwrap();
        let txn = env.begin_ro_txn().unwrap();
        for i in 0..len {
            txn.get(db, &(i.to_string())).unwrap();
            if i != 0 && i % 100 == 0 {
                let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
                print!("read {} data need {}, avg: {}\n", i, time, time / i as f32);
            }
        }
    }

    #[tokio::test]
    async fn test_cache_lmdb() {
        let batch_size = POOL_SIZE;
        println!(
            "{:?} {:?}",
            Command::new("dd")
                .arg("if=/home/xiej/nfs/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!"),
            Command::new("dd")
                .arg("if=/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb")
                .arg("iflag=nocache")
                .arg("count=0")
                .output()
                .expect("cmd exec error!")
        );
        let location = "/home/xiej/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
        let len = 1281166;
        let name = "DLCache".to_string();
        let cache = Arc::new(Mutex::new(Cache::new(1024 * 1024 * 1024, &name, 1024)));
        let mut items = Vec::new();
        for i in 0..len as usize {
            items.push(DataItem {
                keys: vec![i.to_string()],
            })
        }
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
        let dataset = Arc::new(LmdbDataset {
            items,
            id: 0,
            db: env.open_db(None).unwrap(),
            env: Arc::new(env),
            pool: Arc::new(Mutex::new(ThreadPool::new(POOL_SIZE))),
        });
        let mut joader = Joader::new(dataset);
        let (s, mut r) = create_data_channel(0);
        joader.add_loader(0, 1);
        joader.add_data_sender(0, s);
        let thread_cache = cache.clone();
        let reader = tokio::spawn(async move {
            let now = SystemTime::now();
            let mut consume = 0;
            loop {
                let (indices, empty) = r.recv_all().await;
                {
                    let start_ptr = thread_cache.lock().unwrap().start_ptr();
                    for idx in &indices {
                        let addr =
                            unsafe { start_ptr.offset((*idx as isize) * (Head::size() as isize)) };
                        let mut head = Head::from(addr);
                        head.readed(1);
                    }
                }
                consume += indices.len();
                if consume != 0 && consume % 1000 == 0 {
                    let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
                    print!(
                        "read {} data need {}, avg: {}\n",
                        consume,
                        time,
                        time / consume as f32
                    );
                }
                if consume == len || empty {
                    break;
                }
            }
            println!("exist reading.....");
        });

        let writer = tokio::spawn(async move {
            let now = SystemTime::now();
            for i in 0..(len / batch_size) as usize {
                // println!("{:}", i);
                joader.next_batch(cache.clone(), batch_size).await;
                let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
                if i != 0 && (i * batch_size) % 1000 == 0 {
                    print!(
                        "write {} data need {}, avg: {}\n",
                        i * batch_size,
                        time,
                        time / ((i * batch_size) as f32)
                    );
                }
            }
        });
        let res = join!(reader, writer);
        res.0.unwrap();
        res.1.unwrap();
    }
}
