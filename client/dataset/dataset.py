import sys
sys.path.append("./proto")
import proto.dataset_pb2 as dataset_pb2
import proto.dataset_pb2_grpc as dataset_pb2_grpc
from enum import Enum



class DatasetType(Enum):
    FILESYSTEM = 0,
    DUMMY = 1,
    LMDB = 2,


class Dataset(object):
    type: DatasetType
    name: str
    items: list

    def __init__(self, name: str, location: str, ty: DatasetType):
        self.name = name
        self.location = location
        if ty == DatasetType.FILESYSTEM:
            self.ty = dataset_pb2.CreateDatasetRequest.FILESYSTEM
        elif ty == DatasetType.DUMMY:
            self.ty = dataset_pb2.CreateDatasetRequest.DUMMY
        elif ty == DatasetType.LMDB:
            self.ty = dataset_pb2.CreateDatasetRequest.LMDB
        else:
            assert False, "Dataset unsupported type!"
        self.items = []

    def add_item(self, item: list):
        self.items.append(dataset_pb2.DataItem(keys=item))

    def create(self, channel):
        client = dataset_pb2_grpc.DatasetSvcStub(channel)
        request = dataset_pb2.CreateDatasetRequest(
            name=self.name,
            location=self.location,
            type=self.ty,
            items=self.items,
            weights=[])
        return client.CreateDataset(request)

    def delete(self, channel):
        client = dataset_pb2_grpc.DatasetSvcStub(channel)
        request = dataset_pb2.DeleteDatasetRequest(name=self.name)
        return client.DeleteDataset(request)

    def __len__(self):
        return len(self.items)
