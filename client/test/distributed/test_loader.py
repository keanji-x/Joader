import sys
sys.path.append("./")

from dataset.dataset import Dataset, DatasetType
from loader.loader import Loader
import grpc
import time

channel = grpc.insecure_channel('210.28.134.91:4321')
name = "DummyDataset"
length = 100

if __name__ == "__main__":
    ds = Dataset(name=name, location="", ty=DatasetType.DUMMY)
    now = time.time()
    for i in range(0, length):
        ds.add_item([str(i)])
    ds.create(channel)


    leader = Loader.new(dataset_name=name,
                        name="dummy_loader", ip='210.28.134.91:4321', nums=2)
    leader_res = []
    follower = Loader.new(dataset_name=name, name="dummy_loader", ip="210.28.134.33:4321", nums=2)
    follower_res = []
    now = time.time()
    for i in range(int(length/2)):
        if i != 0 and i % 1000 == 0:
            print("readed {} data in {} avg: {}".format(
                i, time.time() - now, (time.time() - now)/i))
        leader_res.append(leader.next())
        follower_res.append(follower.next())
    print(time.time() - now)
    res = leader_res + follower_res
    print(len(leader_res), len(follower_res))
    res = sorted(res)
    assert res == list(range(length))
    leader.delete()
    follower.delete()
    ds.delete(channel)
