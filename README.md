## About The Project
This a dataloader that for multiple training jobs in deep learning. The goal of this project is to avoid redundant data prep work and boost training efficiency.


### Built With


* [Rust](https://www.rust-lang.org/)
* [Python](https://www.python.org/)
* [Opencv](https://opencv.org/)
* [PyTorch](https://pytorch.org/)


<!-- GETTING STARTED -->
## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

Install packages for server
  * Install the [opencv-rust](https://github.com/twistedfall/opencv-rust)
  * Install the [tensorflow-rust](https://github.com/tensorflow/rust)

Install packages for client
  ```sh
  pip install -r requirements.txt 
  ```

### Installation

Build the target of server
  ```sh
    cargo build --release
  ```



<!-- USAGE EXAMPLES -->
## Quick start

1. Run the server
```sh
./server/target/release/joader
```

2. Create a dataset with some keys and conditions
```py
from dataset.dataset import Dataset as JDataset, DatasetType
channel = grpc.insecure_channel('127.0.0.1:4321')
ds = JDataset(name=name, location=location, ty=DatasetType.LMDB)
for k in keys:
    ds.add_item([str(k).encode()])
ds.create(channel)
channel.close()
```

3. Register the job for loading data and read data
```py
job = Job.new(dataset_name, name='job', ip='127.0.0.1:4321')
for _ in range(dataset_len):
    data = job.next
```

4. Train the model with PyTorch

More examples are in `client/test` and unitests are in each file in server

## License

Distributed under the MIT License. See `LICENSE.txt` for more information.
