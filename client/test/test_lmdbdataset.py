import torch
import os
import cv2
import time
import numpy as np
import torchvision.transforms as transforms
import lmdb
import msgpack
from torch.utils.data import Dataset
from PIL import Image
import torch

class lmdbDataset(Dataset):
    def __init__(self, location, is_train):
        torch.set_num_threads(1)
        self.env = lmdb.open(location, subdir=False, readonly=True, lock=False, readahead=True, meminit=False)
        self.txn = self.env.begin(write=False)
        self.length = self.txn.stat()['entries']
        normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                         std=[0.229, 0.224, 0.225])
        # train data augment
        if is_train:
            self.transform = transforms.Compose([
                transforms.RandomResizedCrop(224),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                normalize,
            ])
        # test data augment
        else:
            self.transform = transforms.Compose([
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                normalize,
            ])

    def __len__(self):
        # the last key is not existed in LVS
        return self.length - 1

    def __getitem__(self, index):
        new_index = str(index).encode()
        data = self.txn.get(new_index)
        now_data = msgpack.loads(data, raw=False)
        data_img = now_data[0]
        label = now_data[1]
        now_arr = np.frombuffer(data_img[b'data'], dtype=np.uint8)
        image_content = cv2.imdecode(now_arr, cv2.IMREAD_COLOR)
        image_content = cv2.cvtColor(image_content, cv2.COLOR_BGR2RGB)
        image_content = Image.fromarray(image_content)
        image_content = self.transform(image_content)
        return image_content, label