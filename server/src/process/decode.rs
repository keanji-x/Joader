use opencv::{prelude::Mat, imgcodecs::imdecode, imgproc::{COLOR_BGR2RGB, cvt_color}};

pub fn decode_rgb_from_memory(data: & [u8]) -> Mat {
    let mat = Mat:: from_slice(data).unwrap();
    let image = imdecode( & mat, 1).unwrap();
    let mut dst = Mat::default();
    cvt_color(&image, &mut dst, COLOR_BGR2RGB, 0).unwrap();
    dst
}
