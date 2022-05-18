use opencv::{prelude::Mat, imgcodecs::imdecode};

pub fn decode_from_memory(data: & [u8]) -> Mat {
    let mat = Mat:: from_slice(data).unwrap();
    imdecode( & mat, 1).unwrap()
}
