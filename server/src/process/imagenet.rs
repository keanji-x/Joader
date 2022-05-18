use super::decode_rgb_from_memory;
use image::imageops::FilterType::Triangle;
use opencv::{
    core::{Range, Vector, CV_8UC3},
    imgproc::{resize, INTER_LINEAR},
    prelude::{Mat, MatTraitConst, MatTraitConstManual},
};
use rand::distributions::{Distribution, Uniform};
use std::slice::from_raw_parts;
use tch::vision::imagenet::load_image_and_resize224_from_memory;

pub fn random_crop(image: &Mat) -> Mat {
    // Get parameters for ``crop`` for a random sized crop.
    // Args:
    //     scale (list): range of scale of the origin size cropped
    //     ratio (list): range of aspect ratio of the origin aspect ratio cropped

    // Returns:
    //     tuple: params (i, j, h, w) to be passed to ``crop`` for a random
    //     sized crop.
    pub fn random_parame(h: i32, w: i32, scale: &[f32], ratio: &[f32]) -> (i32, i32, i32, i32) {
        let area = (h * w) as f32;
        let ratio_range = Uniform::from(ratio[0].ln()..ratio[1].ln());
        let scale_range = Uniform::from(scale[0]..scale[1]);
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let target_area = area * ratio_range.sample(&mut rng);
            let aspect_ratio = scale_range.sample(&mut rng).exp();
            let crop_w = ((target_area * aspect_ratio).sqrt()).round() as i32;
            let crop_h = ((target_area / aspect_ratio).sqrt()).round() as i32;
            if crop_w > 0 && crop_w <= w && crop_h > 0 && crop_h <= h {
                let w_range = Uniform::from(0..w - crop_w + 1);
                let h_range = Uniform::from(0..h - crop_h + 1);
                let i = h_range.sample(&mut rng);
                let j = w_range.sample(&mut rng);
                return (i, j, crop_h, crop_w);
            }
        }

        // center crop
        let in_ratio = w as f32 / h as f32;
        let crop_w;
        let crop_h;
        if in_ratio < ratio[0] {
            crop_w = w;
            crop_h = (w as f32 / ratio[0]).round() as i32;
        } else if in_ratio > ratio[1] {
            crop_h = h;
            crop_w = (h as f32 * ratio[1]).round() as i32;
        } else {
            crop_h = h;
            crop_w = w;
        }
        let i = (h - crop_h) / 2;
        let j = (w - crop_w) / 2;
        (i, j, crop_h, crop_w)
    }
    let h = image.rows();
    let w = image.cols();
    let (i, j, h, w) = random_parame(h, w, &[0.08, 1.0], &[0.75, 1.3333333333333333]);
    // println!("{:} {:} {:} {:}", i, j, h, w);
    let h_range = Range::new(i, i + h).unwrap();
    let w_range = Range::new(j, j + w).unwrap();
    Mat::ranges(image, &Vector::from(vec![h_range, w_range])).unwrap()
}

pub fn decode_resize_224_opencv(data: &[u8]) -> Vec<u8> {
    let mut image = decode_rgb_from_memory(data);
    let mut image = random_crop(&mut image);
    let mut dst = unsafe { Mat::new_rows_cols(224, 224, CV_8UC3).unwrap() };
    let size = dst.size().unwrap();
    resize(&mut image, &mut dst, size, 0.0, 0.0, INTER_LINEAR).unwrap();
    dst.data_bytes().unwrap().to_vec()
}

pub fn decode_resize_224_tch(data: &[u8]) -> Vec<u8> {
    let tensor = load_image_and_resize224_from_memory(data).unwrap();
    let data = unsafe { from_raw_parts(tensor.data_ptr() as *mut u8, 224 * 224 * 3).to_vec() };
    data
}

pub fn decode_resize_224_image(data: &[u8]) -> Vec<u8> {
    let image = image::load_from_memory(data).unwrap();
    let image = image.resize(224, 224, Triangle);
    image.as_bytes().to_vec()
}
