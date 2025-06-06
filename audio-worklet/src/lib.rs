use df::tract::{DfParams, DfTract, RuntimeParams};
use wasm_audio::WasmAudioProcessor;

pub mod wasm_audio;

#[no_mangle]
pub unsafe fn df_new(
    model_bytes: &[u8],
    //model_size: usize,
    atten_lim: f32,
) -> Box<WasmAudioProcessor> {
    let channels = 1;
    //let model_bytes = unsafe { std::slice::from_raw_parts(model_ptr, model_size) };
    let r_params = RuntimeParams::default_with_ch(channels).with_atten_lim(atten_lim);
    let df_params = DfParams::from_bytes_tar(model_bytes).expect("Could not load model from path");
    let m = DfTract::new(df_params, &r_params).expect("Could not initialize DeepFilter runtime.");

    Box::new(WasmAudioProcessor::new(m))
}

#[no_mangle]
pub unsafe fn df_set_atten_lim(pdo: &mut WasmAudioProcessor, db: f32) {
    pdo.set_atten_lim(db);
}

#[no_mangle]
pub unsafe fn df_process(pdo: &mut WasmAudioProcessor, input: &[f32], outbuf: &mut [f32]) {
    //let input = unsafe {
    //    std::slice::from_raw_parts(inputbuf, sz)
    //};
    //let output = unsafe {
    //    std::slice::from_raw_parts_mut(outbuf, sz)
    //};
    pdo.process(input, outbuf);
}

#[no_mangle]
pub unsafe fn df_frame_size(pdo: &mut WasmAudioProcessor) -> usize {
    pdo.frame_size()
}

//#[wasm_bindgen]
#[no_mangle]
pub extern "C" fn df_del(_: Option<Box<WasmAudioProcessor>>) {}

// adapted from Glicol
// https://github.com/chaosprint/glicol/blob/7ece81d6fadfc5a8873df2a3ac04f8f915fa1998/rs/wasm/src/lib.rs#L9-L15
#[no_mangle]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(size);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr as *mut u8
}

//fn wasm_gen_start() {
//    set_once()
//}
