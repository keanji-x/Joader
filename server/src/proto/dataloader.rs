#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateDataloaderRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub dataset_name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub nums: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateDataloaderResponse {
    #[prost(uint64, tag = "1")]
    pub length: u64,
    #[prost(string, tag = "2")]
    pub shm_path: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub loader_id: u64,
    #[prost(message, optional, tag = "4")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NextRequest {
    #[prost(uint64, tag = "1")]
    pub loader_id: u64,
    #[prost(int32, tag = "2")]
    pub batch_size: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NextResponse {
    #[prost(uint64, repeated, tag = "2")]
    pub address: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint32, repeated, tag = "3")]
    pub read_off: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteDataloaderRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub dataset_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteDataloaderResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetDataloaderRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub dataset_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetDataloaderResponse {}
#[doc = r" Generated client implementations."]
pub mod data_loader_svc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct DataLoaderSvcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DataLoaderSvcClient<tonic::transport::Channel> {
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
    impl<T> DataLoaderSvcClient<T>
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
        ) -> DataLoaderSvcClient<InterceptedService<T, F>>
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
            DataLoaderSvcClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn create_dataloader(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateDataloaderRequest>,
        ) -> Result<tonic::Response<super::CreateDataloaderResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/dataloader.DataLoaderSvc/CreateDataloader");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn next(
            &mut self,
            request: impl tonic::IntoRequest<super::NextRequest>,
        ) -> Result<tonic::Response<super::NextResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/dataloader.DataLoaderSvc/Next");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_dataloader(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteDataloaderRequest>,
        ) -> Result<tonic::Response<super::DeleteDataloaderResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/dataloader.DataLoaderSvc/DeleteDataloader");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn reset_dataloader(
            &mut self,
            request: impl tonic::IntoRequest<super::ResetDataloaderRequest>,
        ) -> Result<tonic::Response<super::ResetDataloaderResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/dataloader.DataLoaderSvc/ResetDataloader");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod data_loader_svc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with DataLoaderSvcServer."]
    #[async_trait]
    pub trait DataLoaderSvc: Send + Sync + 'static {
        async fn create_dataloader(
            &self,
            request: tonic::Request<super::CreateDataloaderRequest>,
        ) -> Result<tonic::Response<super::CreateDataloaderResponse>, tonic::Status>;
        async fn next(
            &self,
            request: tonic::Request<super::NextRequest>,
        ) -> Result<tonic::Response<super::NextResponse>, tonic::Status>;
        async fn delete_dataloader(
            &self,
            request: tonic::Request<super::DeleteDataloaderRequest>,
        ) -> Result<tonic::Response<super::DeleteDataloaderResponse>, tonic::Status>;
        async fn reset_dataloader(
            &self,
            request: tonic::Request<super::ResetDataloaderRequest>,
        ) -> Result<tonic::Response<super::ResetDataloaderResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct DataLoaderSvcServer<T: DataLoaderSvc> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: DataLoaderSvc> DataLoaderSvcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DataLoaderSvcServer<T>
    where
        T: DataLoaderSvc,
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
                "/dataloader.DataLoaderSvc/CreateDataloader" => {
                    #[allow(non_camel_case_types)]
                    struct CreateDataloaderSvc<T: DataLoaderSvc>(pub Arc<T>);
                    impl<T: DataLoaderSvc>
                        tonic::server::UnaryService<super::CreateDataloaderRequest>
                        for CreateDataloaderSvc<T>
                    {
                        type Response = super::CreateDataloaderResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateDataloaderRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_dataloader(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateDataloaderSvc(inner);
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
                "/dataloader.DataLoaderSvc/Next" => {
                    #[allow(non_camel_case_types)]
                    struct NextSvc<T: DataLoaderSvc>(pub Arc<T>);
                    impl<T: DataLoaderSvc> tonic::server::UnaryService<super::NextRequest> for NextSvc<T> {
                        type Response = super::NextResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::NextRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).next(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = NextSvc(inner);
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
                "/dataloader.DataLoaderSvc/DeleteDataloader" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteDataloaderSvc<T: DataLoaderSvc>(pub Arc<T>);
                    impl<T: DataLoaderSvc>
                        tonic::server::UnaryService<super::DeleteDataloaderRequest>
                        for DeleteDataloaderSvc<T>
                    {
                        type Response = super::DeleteDataloaderResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteDataloaderRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_dataloader(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteDataloaderSvc(inner);
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
                "/dataloader.DataLoaderSvc/ResetDataloader" => {
                    #[allow(non_camel_case_types)]
                    struct ResetDataloaderSvc<T: DataLoaderSvc>(pub Arc<T>);
                    impl<T: DataLoaderSvc>
                        tonic::server::UnaryService<super::ResetDataloaderRequest>
                        for ResetDataloaderSvc<T>
                    {
                        type Response = super::ResetDataloaderResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ResetDataloaderRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).reset_dataloader(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ResetDataloaderSvc(inner);
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
    impl<T: DataLoaderSvc> Clone for DataLoaderSvcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: DataLoaderSvc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: DataLoaderSvc> tonic::transport::NamedService for DataLoaderSvcServer<T> {
        const NAME: &'static str = "dataloader.DataLoaderSvc";
    }
}
