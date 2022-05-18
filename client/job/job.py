from multiprocessing import Condition
import numpy as np
import proto.job_pb2 as job_pb2
import proto.job_pb2_grpc as job_pb2_grpc
import grpc
import sys
sys.path.append("./proto")


class Job(object):
    def __init__(self, ip, length: int, job_name: str, dataset_name: str, job_id):
        self.length = length
        self.job_id = job_id
        channel = grpc.insecure_channel(
            ip, options=[
                ('grpc.enable_http_proxy', 0),
                ('grpc.max_receive_message_length', 1024*1024*25),])
        self.client = job_pb2_grpc.JobSvcStub(channel)
        self.job_name = job_name
        self.dataset_name = dataset_name

    @staticmethod
    def new(dataset_name: str, name: str, ip: str, start="", end=""):
        expr_list = []
        if start != "":
            expr_list.append(job_pb2.Expr(op=job_pb2.Expr.GEQ, rhs=start))
        if end != "":
            expr_list.append(job_pb2.Expr(op=job_pb2.Expr.LT, rhs=end))

        channel = grpc.insecure_channel(
            ip, options=(('grpc.enable_http_proxy', 0),))
        client = job_pb2_grpc.JobSvcStub(channel)
        cond = job_pb2.Condition(exprs=expr_list)
        request = job_pb2.CreateJobRequest(
            dataset_name=dataset_name, name=name, condition=cond)
        resp = client.CreateJob(request)
        job_id = resp.job_id
        length = resp.length
        return Job(ip, length, name, dataset_name, job_id)

    def transform(self, data: job_pb2.Data):
        if data.ty == job_pb2.Data.UINT:
            return int.from_bytes(data.bs, 'big', signed=False)
        elif data.ty == job_pb2.Data.INT:
            return int.from_bytes(data.bs, 'big', signed=True)
        elif data.ty == job_pb2.Data.IMAGE:
            image = np.frombuffer(data.bs, dtype=np.uint8, count = len(data.bs)).reshape(224, 224, -1)
            return image
        else:
            assert False

    def next(self):
        request = job_pb2.NextRequest(job_id=self.job_id)
        data_list = self.client.Next(request).data
        res = []
        for data in data_list:
            res.append(self.transform(data))
        return res

    def len(self):
        return self.length

