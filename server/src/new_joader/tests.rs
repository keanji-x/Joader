use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;

use super::joader::*;
use crate::new_dataset::build_dataset;
use crate::proto::dataset::{CreateDatasetRequest, DataItem};
use crate::proto::job::Data;
use crate::{
    job::Job, local_cache::cache::Cache, new_dataset::new_dummy,
    new_joader::joader_table::JoaderTable,
};

async fn write(mut jt: JoaderTable, len: usize) {
    let mut cnt = 0;
    loop {
        jt.next().await;
        cnt += 1;
        if cnt == len {
            break;
        }
    }
    assert_eq!(cnt, len);
}

async fn read(mut recv: Receiver<Arc<Vec<Data>>>, len: usize) -> Vec<Arc<Vec<Data>>> {
    let now = SystemTime::now();
    let mut res = Vec::new();
    loop {
        let data = recv.recv().await;
        match data {
            Some(data) => res.push(data),
            None => continue,
        }
        if res.len() == len {
            break;
        }
    }
    let time = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
    println!("get each data cost {:} secs", time / len as f32);
    assert_eq!(res.len(), len);
    res
}

#[tokio::test]
async fn test_joader_dummy() {
    // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
    let cache = Arc::new(Mutex::new(Cache::new()));
    let mut jt = JoaderTable::new(cache);

    let len = 4096;
    let name = "dummy".to_string();
    let dataset = new_dummy(len, name.clone());
    let mut joader = Joader::new(dataset);
    let (job, recv) = Job::new(0);
    joader.add_job(job.clone()).await;
    jt.add_joader(joader);
    tokio::spawn(async move { write(jt, len).await });
    tokio::spawn(async move { read(recv, len).await })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_joader_lmdb() {
    // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
    let cache = Arc::new(Mutex::new(Cache::new()));
    let mut jt = JoaderTable::new(cache);

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
    let mut joader = Joader::new(dataset);
    let (job, recv) = Job::new(0);
    joader.add_job(job.clone()).await;
    jt.add_joader(joader);
    tokio::spawn(async move { write(jt, len).await });
    tokio::spawn(async move { read(recv, len).await })
        .await
        .unwrap();
}
