use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::sleep;

use super::joader::*;
use crate::dataset::build_dataset;
use crate::proto::dataset::{CreateDatasetRequest, DataItem};
use crate::proto::job::{expr, Condition, Data, Expr};
use crate::{cache::cache::Cache, dataset::new_dummy, joader::joader_table::JoaderTable, job::Job};

async fn write(mut jt: JoaderTable, _len: usize) {
    loop {
        jt.next().await;
        if jt.is_empty() {
            break;
        }
    }
}

async fn read(
    _job_id: u64,
    mut recv: Receiver<Arc<Vec<Data>>>,
    len: usize,
    dur: Duration,
) -> Vec<Arc<Vec<Data>>> {
    let now = SystemTime::now();
    let mut res = Vec::new();
    loop {
        let data = recv.recv().await;
        sleep(dur).await;
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
    joader.add_job(job.clone(), None).await;
    jt.add_joader(joader);
    tokio::spawn(async move { write(jt, len).await });
    tokio::spawn(async move { read(0, recv, len, Duration::from_millis(1)).await })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_joader_lmdb() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
    let cache = Arc::new(Mutex::new(Cache::new()));
    let mut jt = JoaderTable::new(cache);

    let len = 2048;
    let location = "/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
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
    joader.add_job(job.clone(), None).await;
    jt.add_joader(joader);
    tokio::spawn(async move { write(jt, len).await });
    tokio::spawn(async move { read(0, recv, len, Duration::from_millis(1)).await })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_joader_multi_lmdb() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
    let cache = Arc::new(Mutex::new(Cache::new()));
    let mut jt = JoaderTable::new(cache);

    let len = 2048;
    let location = "/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
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
    let mut reader = Vec::new();
    for i in 0..5 {
        let (job, recv) = Job::new(i);
        joader.add_job(job.clone(), None).await;
        reader.push(tokio::spawn(async move {
            read(i, recv, len, Duration::from_millis(i)).await
        }));
    }
    jt.add_joader(joader);
    tokio::spawn(async move { write(jt, len).await });
    for r in reader {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_filter() {
    let len = 2048;
    let location = "/data/lmdb-imagenet/ILSVRC-train.lmdb".to_string();
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
    let cond = Condition {
        exprs: vec![
            Expr {
                op: expr::Operation::Geq as i32,
                rhs: "0".to_string(),
            },
            Expr {
                op: expr::Operation::Lt as i32,
                rhs: "16".to_string(),
            },
        ],
    };
    let (job, _recv) = Job::new(0);
    let size = joader.add_job(job.clone(), Some(cond)).await;
    assert_eq!(size, 16);
}
