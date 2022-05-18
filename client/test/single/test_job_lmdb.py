import sys
sys.path.append("./")
import time
import torchvision.transforms as transforms
import random
from PIL import Image
import multiprocessing
import cv2
import lmdb
import grpc
import dataset.dataset as ds
import job.job as job
import os

location = "data/lmdb-imagenet/ILSVRC-train.lmdb"
env = lmdb.open("data/lmdb-imagenet/ILSVRC-train.lmdb", subdir=False,
                max_readers=100, readonly=True, lock=False, readahead=False, meminit=False)
txn = env.begin(write=False)
lmdb_len = txn.stat()['entries']
channel = grpc.insecure_channel(
    '210.28.134.91:4321', options=(('grpc.enable_http_proxy', 0),))
name = "ImageNet"
LOSE_KEY = 1281167
keys = []
for i in range(lmdb_len):
    if i != LOSE_KEY:
        keys.append(str(i))
random.shuffle(keys)

normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
# train data augment
transform = transforms.Compose([
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor(),
    normalize,
])


def read(job: job.Job):
    now = time.time()
    for i in range(job.len()):
        if i != 0 and i % 1000 == 0:
            print("readed {} data in {} avg: {}".format(
                i, time.time() - now, (time.time() - now)/i))
        label, image_content = job.next()
        image_content = cv2.cvtColor(image_content, cv2.COLOR_BGR2RGB)
        image_content = Image.fromarray(image_content)
        image_content = transform(image_content)
    print(time.time() - now)

def test_create_job():
    lmdb = ds.Dataset(name=name, location=location, ty=ds.DatasetType.LMDB)
    for k in keys:
        lmdb.add_item([k])
    print(len(keys))
    lmdb.create(channel)
    myjob = job.Job.new(dataset_name=name,
                     name="lmdb_loader", ip='210.28.134.91:4321', start=str(0), end=str(1024))
    p = multiprocessing.Process(target=read, args=(myjob, ))
    p.start()
    p.join()

def test_joader_lmdb():
    lmdb = ds.Dataset(name=name, location=location, ty=ds.DatasetType.LMDB)
    for k in keys:
        lmdb.add_item([k])
    print(len(keys))
    lmdb.create(channel)
    loader = job.Job.new(dataset_name=name,
                     name="lmdb_loader", ip='210.28.134.91:4321', start=1, end=16)
    print(loader.len())
    p = multiprocessing.Process(target=read, args=(loader, ))
    p.start()
    p.join()
    # loader.delete()
    # ds.delete(channel)


if __name__ == "__main__":
    test_create_job()
