python3 -m grpc_tools.protoc -I../protos --python_out=./proto --grpc_python_out=./proto ../protos/dataloader.proto
python3 -m grpc_tools.protoc -I../protos --python_out=./proto --grpc_python_out=./proto ../protos/dataset.proto
python3 -m grpc_tools.protoc -I../protos --python_out=./proto --grpc_python_out=./proto ../protos/common.proto
python3 -m grpc_tools.protoc -I../protos --python_out=./proto --grpc_python_out=./proto ../protos/distributed.proto
python3 -m grpc_tools.protoc -I../protos --python_out=./proto --grpc_python_out=./proto ../protos/job.proto