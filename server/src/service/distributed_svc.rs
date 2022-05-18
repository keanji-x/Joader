use crate::dataset;
use crate::joader::joader::Joader;
use crate::joader::joader_table::JoaderTable;
use crate::loader::{create_idx_channel, IdxReceiver};
use crate::proto::distributed::distributed_svc_server::DistributedSvc;
use crate::proto::distributed::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

use super::GlobalID;

#[derive(Debug, Default)]
struct Host {
    recv: HashMap<u64, IdxReceiver>,
    id: u32,
    port: u64,
}

impl Host {
    fn new(id: u32, port: u64) -> Host {
        Host {
            recv: HashMap::new(),
            id,
            port,
        }
    }

    fn add(&mut self, r: IdxReceiver) {
        self.recv.insert(r.get_loader_id(), r);
    }

    fn del(&mut self, loader_id: u64) {
        self.recv.remove(&loader_id);
    }

    async fn recv_all(&mut self) -> Vec<SampleResult> {
        let mut del_loaders = Vec::new();
        let mut ret = Vec::new();
        for (loader_id, v) in self.recv.iter_mut() {
            let (indices, empty) = v.recv_all().await;
            if empty {
                del_loaders.push(*loader_id);
            }
            ret.push(SampleResult {
                loader_id: *loader_id,
                indices,
            });
        }
        for id in del_loaders {
            self.recv.remove(&id);
        }
        ret
    }
}

#[derive(Debug)]
pub struct DistributedSvcImpl {
    id: GlobalID,
    loader_id_table: Arc<Mutex<HashMap<String, u64>>>,
    dataset_table: Arc<Mutex<HashMap<String, u32>>>,
    host_table: Arc<Mutex<HashMap<String, Host>>>,
    joader_table: Arc<Mutex<JoaderTable>>,
}

impl DistributedSvcImpl {
    pub fn new(
        id: GlobalID,
        loader_id_table: Arc<Mutex<HashMap<String, u64>>>,
        dataset_table: Arc<Mutex<HashMap<String, u32>>>,
        joader_table: Arc<Mutex<JoaderTable>>,
    ) -> DistributedSvcImpl {
        Self {
            id,
            loader_id_table,
            dataset_table,
            host_table: Default::default(),
            joader_table,
        }
    }
}

#[async_trait]
impl DistributedSvc for DistributedSvcImpl {
    async fn register_host(
        &self,
        request: Request<RegisterHostRequest>,
    ) -> Result<Response<RegisterHostResponse>, Status> {
        let request = request.into_inner();
        log::info!("Create Host {:?}", request);
        let mut ht = self.host_table.lock().await;
        let mut jt = self.joader_table.lock().await;
        if ht.contains_key(&request.ip) {
            return Err(Status::already_exists(format!("{}", request.ip)));
        }
        let id = self.id.get_host_id().await;
        let port = request.port;
        let host = Host::new(id, port);
        ht.insert(request.ip.clone(), host);

        // update host number
        jt.set_hash_key(ht.len() as u32);
        Ok(Response::new(RegisterHostResponse { host_id: id as u64 }))
    }

    async fn delete_host(
        &self,
        _request: Request<DeleteHostRequest>,
    ) -> Result<Response<DeleteHostResponse>, Status> {
        Err(Status::unimplemented(
            "Delete host has not been implemented",
        ))
    }

    async fn create_sampler(
        &self,
        request: Request<CreateSamplerRequest>,
    ) -> Result<Response<CreateSamplerResponse>, Status> {
        log::info!("Create sampler {:?}", request);
        let request = request.into_inner();
        let mut ht = self.host_table.lock().await;
        let mut jt = self.joader_table.lock().await;
        let mut loader_id_table = self.loader_id_table.lock().await;
        let dt = self.dataset_table.lock().await;
        println!("Host table {:?}", ht);
        let host = ht
            .get_mut(&request.ip)
            .ok_or_else(|| Status::not_found(format!("{} not exited", request.ip)))?;
        println!("Get host id {:?}", request.ip);
        let dataset_id = dt
            .get(&request.dataset_name)
            .ok_or_else(|| Status::not_found(&request.dataset_name))?;

        let joader = jt.get_mut(*dataset_id);
        // 1. If loader not exited, add loader and update loader_id
        let loader_id;
        if loader_id_table.contains_key(&request.name) {
            loader_id = loader_id_table[&request.name];
            println!("Exsited loader {:?}", loader_id);
        } else {
            loader_id = self.id.get_loader_id(*dataset_id).await;
            joader.add_loader(loader_id, request.nums);
            loader_id_table.insert(request.name.clone(), loader_id);
            println!("Create loader {:?}", loader_id);
        }

        // 2. Add sample to loader
        let (is, ir) = create_idx_channel(loader_id);
        joader.add_idx_sender(loader_id, is, host.id.into());
        // 3. Add recv to host
        host.add(ir);

        let length = joader.len();
        Ok(Response::new(CreateSamplerResponse {
            length,
            loader_id,
            dataset_id: *dataset_id,
        }))
    }

    async fn delete_sampler(
        &self,
        request: Request<DeleteSamplerRequest>,
    ) -> Result<Response<DeleteSamplerResponse>, Status> {
        let request = request.into_inner();
        log::info!("Delete sampler {:?}", request);
        let mut ht = self.host_table.lock().await;
        let mut loader_id_table = self.loader_id_table.lock().await;
        let mut jt = self.joader_table.lock().await;
        let dt = self.dataset_table.lock().await;

        let host = ht
            .get_mut(&request.ip)
            .ok_or_else(|| Status::not_found(format!("{} not exited", request.ip)))?;
        let dataset_id = dt
            .get(&request.dataset_name)
            .ok_or_else(|| Status::not_found(&request.dataset_name))?;

        //1. loader remove host
        let joader = jt.get_mut(*dataset_id);
        let loader_id = *loader_id_table
            .get(&request.name)
            .ok_or_else(|| Status::not_found(format!("{} not exited", request.name)))?;
        
        joader.del_idx_sender(loader_id, host.id.into());
        //2. host remove recv
        host.del(loader_id);
        //3. if empty, remove host_id
        if joader.is_loader_empty(loader_id) {
            loader_id_table.remove(&request.name);
        }
        Ok(Response::new(DeleteSamplerResponse {}))
    }

    async fn query_host(
        &self,
        request: Request<QueryHostRequest>,
    ) -> Result<Response<QueryHostResponse>, Status> {
        let request = request.into_inner();
        let mut ht = self.host_table.lock().await;
        let host = ht
            .get_mut(&request.ip)
            .ok_or_else(|| Status::not_found(format!("{} not exited", request.ip)))?;
        Ok(Response::new(QueryHostResponse {
            port: host.port as u64,
        }))
    }

    async fn sample(
        &self,
        request: Request<SampleRequest>,
    ) -> Result<Response<SampleResponse>, Status> {
        let request = request.into_inner();
        let mut ht = self.host_table.lock().await;
        let host = ht
            .get_mut(&request.ip)
            .ok_or_else(|| Status::not_found(format!("{} not exited", request.ip)))?;
        Ok(Response::new(SampleResponse {
            res: host.recv_all().await,
        }))
    }

    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        let r = request.into_inner();
        log::debug!("regeister dataset {:?}", r);
        let request = r.request.unwrap();
        let id = r.dataset_id;
        let mut dt = self.dataset_table.lock().await;
        if dt.contains_key(&request.name) {
            return Err(Status::already_exists(format!(
                "{:?} has already existed",
                request
            )));
        }
        log::debug!("Create dataset {:?}", request);
        dt.insert(request.name.clone(), id);
        // insert dataset to dataset table
        let joader = Joader::new(dataset::build_dataset(request, id));
        self.joader_table.lock().await.add_joader(joader);

        Ok(Response::new(RegisterDatasetResponse {}))
    }
}
