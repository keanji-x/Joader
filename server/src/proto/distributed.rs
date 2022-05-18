#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterHostRequest {
    #[prost(string, tag = "1")]
    pub ip: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub port: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterHostResponse {
    #[prost(uint64, tag = "1")]
    pub host_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterDatasetRequest {
    #[prost(message, optional, tag = "1")]
    pub request: ::core::option::Option<super::dataset::CreateDatasetRequest>,
    #[prost(uint32, tag = "2")]
    pub dataset_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterDatasetResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteHostRequest {
    #[prost(string, tag = "1")]
    pub ip: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub port: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteHostResponse {
    #[prost(uint64, tag = "1")]
    pub host_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SampleResult {
    #[prost(uint64, tag = "1")]
    pub loader_id: u64,
    #[prost(uint32, repeated, tag = "2")]
    pub indices: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryHostRequest {
    #[prost(string, tag = "1")]
    pub ip: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryHostResponse {
    #[prost(uint64, tag = "1")]
    pub port: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSamplerRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub dataset_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub ip: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub nums: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSamplerResponse {
    #[prost(uint64, tag = "1")]
    pub length: u64,
    #[prost(uint64, tag = "2")]
    pub loader_id: u64,
    #[prost(uint32, tag = "3")]
    pub dataset_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSamplerRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub dataset_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub ip: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSamplerResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SampleRequest {
    #[prost(string, tag = "3")]
    pub ip: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SampleResponse {
    #[prost(message, repeated, tag = "1")]
    pub res: ::prost::alloc::vec::Vec<SampleResult>,
}
#[doc = r" Generated client implementations."]
pub mod distributed_svc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct DistributedSvcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DistributedSvcClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DistributedSvcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DistributedSvcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            DistributedSvcClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn create_sampler(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSamplerRequest>,
        ) -> Result<tonic::Response<super::CreateSamplerResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/CreateSampler");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_sampler(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSamplerRequest>,
        ) -> Result<tonic::Response<super::DeleteSamplerResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/DeleteSampler");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn query_host(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryHostRequest>,
        ) -> Result<tonic::Response<super::QueryHostResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/QueryHost");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn sample(
            &mut self,
            request: impl tonic::IntoRequest<super::SampleRequest>,
        ) -> Result<tonic::Response<super::SampleResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/Sample");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn register_host(
            &mut self,
            request: impl tonic::IntoRequest<super::RegisterHostRequest>,
        ) -> Result<tonic::Response<super::RegisterHostResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/RegisterHost");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_host(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteHostRequest>,
        ) -> Result<tonic::Response<super::DeleteHostResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/DeleteHost");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn register_dataset(
            &mut self,
            request: impl tonic::IntoRequest<super::RegisterDatasetRequest>,
        ) -> Result<tonic::Response<super::RegisterDatasetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/distributed.DistributedSvc/RegisterDataset");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod distributed_svc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with DistributedSvcServer."]
    #[async_trait]
    pub trait DistributedSvc: Send + Sync + 'static {
        async fn create_sampler(
            &self,
            request: tonic::Request<super::CreateSamplerRequest>,
        ) -> Result<tonic::Response<super::CreateSamplerResponse>, tonic::Status>;
        async fn delete_sampler(
            &self,
            request: tonic::Request<super::DeleteSamplerRequest>,
        ) -> Result<tonic::Response<super::DeleteSamplerResponse>, tonic::Status>;
        async fn query_host(
            &self,
            request: tonic::Request<super::QueryHostRequest>,
        ) -> Result<tonic::Response<super::QueryHostResponse>, tonic::Status>;
        async fn sample(
            &self,
            request: tonic::Request<super::SampleRequest>,
        ) -> Result<tonic::Response<super::SampleResponse>, tonic::Status>;
        async fn register_host(
            &self,
            request: tonic::Request<super::RegisterHostRequest>,
        ) -> Result<tonic::Response<super::RegisterHostResponse>, tonic::Status>;
        async fn delete_host(
            &self,
            request: tonic::Request<super::DeleteHostRequest>,
        ) -> Result<tonic::Response<super::DeleteHostResponse>, tonic::Status>;
        async fn register_dataset(
            &self,
            request: tonic::Request<super::RegisterDatasetRequest>,
        ) -> Result<tonic::Response<super::RegisterDatasetResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct DistributedSvcServer<T: DistributedSvc> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: DistributedSvc> DistributedSvcServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DistributedSvcServer<T>
    where
        T: DistributedSvc,
        B: Body + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/distributed.DistributedSvc/CreateSampler" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSamplerSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc> tonic::server::UnaryService<super::CreateSamplerRequest>
                        for CreateSamplerSvc<T>
                    {
                        type Response = super::CreateSamplerResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateSamplerRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_sampler(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSamplerSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/distributed.DistributedSvc/DeleteSampler" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSamplerSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc> tonic::server::UnaryService<super::DeleteSamplerRequest>
                        for DeleteSamplerSvc<T>
                    {
                        type Response = super::DeleteSamplerResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSamplerRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_sampler(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSamplerSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/distributed.DistributedSvc/QueryHost" => {
                    #[allow(non_camel_case_types)]
                    struct QueryHostSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc> tonic::server::UnaryService<super::QueryHostRequest> for QueryHostSvc<T> {
                        type Response = super::QueryHostResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryHostRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).query_host(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = QueryHostSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/distributed.DistributedSvc/Sample" => {
                    #[allow(non_camel_case_types)]
                    struct SampleSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc> tonic::server::UnaryService<super::SampleRequest> for SampleSvc<T> {
                        type Response = super::SampleResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SampleRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).sample(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SampleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/distributed.DistributedSvc/RegisterHost" => {
                    #[allow(non_camel_case_types)]
                    struct RegisterHostSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc> tonic::server::UnaryService<super::RegisterHostRequest>
                        for RegisterHostSvc<T>
                    {
                        type Response = super::RegisterHostResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RegisterHostRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).register_host(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RegisterHostSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/distributed.DistributedSvc/DeleteHost" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteHostSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc> tonic::server::UnaryService<super::DeleteHostRequest> for DeleteHostSvc<T> {
                        type Response = super::DeleteHostResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteHostRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_host(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteHostSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/distributed.DistributedSvc/RegisterDataset" => {
                    #[allow(non_camel_case_types)]
                    struct RegisterDatasetSvc<T: DistributedSvc>(pub Arc<T>);
                    impl<T: DistributedSvc>
                        tonic::server::UnaryService<super::RegisterDatasetRequest>
                        for RegisterDatasetSvc<T>
                    {
                        type Response = super::RegisterDatasetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RegisterDatasetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).register_dataset(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RegisterDatasetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: DistributedSvc> Clone for DistributedSvcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: DistributedSvc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: DistributedSvc> tonic::transport::NamedService for DistributedSvcServer<T> {
        const NAME: &'static str = "distributed.DistributedSvc";
    }
}
