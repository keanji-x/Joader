use ::joader::cache::cache::Cache;
use ::joader::joader::joader_table::JoaderTable;
use clap::{load_yaml, App};
use joader::proto::dataloader::data_loader_svc_server::DataLoaderSvcServer;
use joader::proto::dataset::dataset_svc_server::DatasetSvcServer;
use joader::proto::distributed::distributed_svc_client::DistributedSvcClient;
use joader::proto::distributed::distributed_svc_server::DistributedSvcServer;
use joader::proto::distributed::{RegisterHostRequest, SampleRequest};
use joader::service::{DataLoaderSvcImpl, DatasetSvcImpl, DistributedSvcImpl, GlobalID};
use joader::Role;
use libc::shm_unlink;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tonic::transport::{Channel, Server};

async fn start_leader(joader_table: Arc<Mutex<JoaderTable>>) {
    log::info!("start leader service ....");
    // sleep(Duration::from_secs_f32(0.1)).await;
    loop {
        {
            let mut joader_table = joader_table.lock().await;
            let empty = joader_table.is_empty();
            if ! empty {
                joader_table.next().await;
                continue;
            }
        };
        log::debug!("sleep ....");
        sleep(Duration::from_millis(1000)).await;
        // we add it it because the mmap block, in the future, we will use io_uring
    }
}

async fn start_follower(
    joader_table: Arc<Mutex<JoaderTable>>,
    mut leader: DistributedSvcClient<Channel>,
    ip: String,
    port: String,
) {
    log::info!("start follower service ... ");
    let request = RegisterHostRequest {
        ip: ip.clone(),
        port: port.parse().unwrap(),
    };
    let resp = leader.register_host(request).await.unwrap();
    log::debug!("Register Host resp: {:?}", resp);
    let sample_request = SampleRequest { ip };
    loop {
        let resp = leader.sample(sample_request.clone()).await.unwrap();
        let sample_res = resp.into_inner().res;
        if sample_res.is_empty() {
            log::debug!("sleep ....");
            sleep(Duration::from_millis(1000)).await;
            continue;
        }
        let mut joader_table = joader_table.lock().await;
        joader_table.remote_read(&sample_res).await;
    }
}

async fn start_server(
    cache_capacity: usize,
    shm_path: &str,
    head_num: u64,
    ip: &str,
    port: &str,
    leader_ip_port: Option<&str>,
    follower_ip_ports: Vec<&str>,
    role: Role,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("start server");
    let id = GlobalID::new();
    let dataset_table = Arc::new(Mutex::new(HashMap::new()));
    let cache = Arc::new(StdMutex::new(Cache::new(
        cache_capacity,
        &shm_path,
        head_num,
    )));
    let joader_table = Arc::new(Mutex::new(JoaderTable::new(cache, shm_path.to_string())));
    let ip_port = ip.to_string() + ":" + port;
    let addr: SocketAddr = ip_port.parse()?;
    let loader_id_table = Arc::new(Mutex::new(HashMap::new()));
    let mut leader = None;
    if role == Role::Follower {
        log::info!("Connect to leader {:?}", leader_ip_port);
        leader = Some(
            DistributedSvcClient::connect(leader_ip_port.unwrap().to_string())
                .await
                .unwrap(),
        );
    }
    log::info!("follower ip {:?}", follower_ip_ports);
    let dataset_svc = DatasetSvcImpl::new(
        joader_table.clone(),
        dataset_table.clone(),
        id.clone(),
        follower_ip_ports.iter().map(|x| x.to_string()).collect(),
        role,
    );
    let del_loaders = Arc::new(Mutex::new(HashSet::new()));

    let data_loader_svc = DataLoaderSvcImpl::new(
        joader_table.clone(),
        del_loaders,
        id.clone(),
        loader_id_table.clone(),
        dataset_table.clone(),
        leader.clone(),
        ip.to_string(),
        role,
    );
    let distributed_svc =
        DistributedSvcImpl::new(id, loader_id_table, dataset_table, joader_table.clone());

    // start joader
    if role == Role::Follower {
        let ip = ip.to_string();
        let port = port.to_string();
        let joader_table = joader_table.clone();
        tokio::spawn(async move { start_follower(joader_table, leader.unwrap(), ip, port).await });
    } else {
        tokio::spawn(async move { start_leader(joader_table).await });
    }
    log::info!("start joader at {:?}......", addr);
    let server = Server::builder()
        .add_service(DatasetSvcServer::new(dataset_svc))
        .add_service(DataLoaderSvcServer::new(data_loader_svc))
        .add_service(DistributedSvcServer::new(distributed_svc))
        .serve(addr);
    server.await?;
    Ok(())
}

fn register_ctrlc(shm_path: &str) {
    log::info!("register ctrlc handler");
    let shm_path = shm_path.to_string();
    ctrlc::set_handler(move || {
        unsafe {
            let shmpath = shm_path.as_ptr() as *const i8;
            shm_unlink(shmpath);
        };
        println!("Close {:?} successfully", shm_path);
        process::exit(1);
    })
    .expect("Error setting Ctrl-C handler");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from(yaml).get_matches();
    let log4rs_config = matches.value_of("log4rs_config").unwrap();
    let ip = matches.value_of("ip").unwrap();
    let port = matches.value_of("port").unwrap();
    let head_num: u64 = matches.value_of("head_num").unwrap().parse().unwrap();
    let cache_capacity: usize = matches.value_of("cache_capacity").unwrap().parse().unwrap();
    let shm_path = matches.value_of("shm_path").unwrap().to_string();
    let role_str = matches.value_of("role").unwrap();
    let mut role = Role::Follower;
    if role_str == "leader" || role_str == "l" {
        role = Role::Leader;
    }
    let leader_ip_port = matches.value_of("leader_ip_port");
    let mut follower_ip_ports = Vec::new();
    if let Some(ips) = matches.values_of("follower_ip_port") {
        follower_ip_ports = ips.collect();
    }
    log4rs::init_file(log4rs_config, Default::default()).unwrap();
    // start ctrlc
    register_ctrlc(&shm_path);
    //start server
    start_server(
        cache_capacity,
        &shm_path,
        head_num,
        ip,
        port,
        leader_ip_port,
        follower_ip_ports,
        role,
    )
    .await?;
    Ok(())
}
