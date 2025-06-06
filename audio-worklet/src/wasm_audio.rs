use df::tract::DfTract;
use ndarray::{ArrayView2, ArrayViewMut2};

type InnerProcess = DfTract;

#[repr(C)]
pub struct WasmAudioProcessor(InnerProcess);

impl WasmAudioProcessor {
    #[no_mangle]
    pub fn new(df: InnerProcess) -> Self {
        Self(df)
    }

    #[no_mangle]
    pub fn process(&mut self, input: &[f32], output: &mut [f32]) {
        let m = &mut self.0;
        let input = ArrayView2::from_shape((1, m.hop_size), input).unwrap();

        let output_view = ArrayViewMut2::from_shape((1, m.hop_size), output).unwrap();

        let _lsnr = m.process(input, output_view).expect("Failed to process DF frame");
    }

    #[no_mangle]
    pub fn frame_size(&mut self) -> usize {
        self.0.hop_size
    }

    #[no_mangle]
    pub fn set_atten_lim(&mut self, db: f32) {
        let m = &mut self.0;
        m.set_atten_lim(db);
    }
}

