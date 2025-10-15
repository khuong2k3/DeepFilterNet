use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use df::tract::{DfParams, DfTract, RuntimeParams};
use df::Complex32;
use ndarray::{Array2, ArrayView2, Axis};
// Import necessary PipeWire and SPA FFI types
use ringbuf::producer::PostponedProducer;

use ringbuf::{Consumer, HeapRb, SharedRb};
use rubato::{FftFixedIn, FftFixedOut, Resampler};

pub const DEFAULT_RATE: u32 = 44100;
pub const DEFAULT_CHANNELS: u32 = 2;
pub const DEFAULT_VOLUME: f64 = 0.5;
pub const CHAN_SIZE: usize = std::mem::size_of::<i16>();

pub type RbProd = PostponedProducer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbCons = Consumer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type SendLsnr = Sender<f32>;
pub type RecvLsnr = Receiver<f32>;
pub type SendSpec = Sender<Box<[f32]>>;
pub type RecvSpec = Receiver<Box<[f32]>>;
pub type SendControl = Sender<(DfControl, f32)>;
pub type RecvControl = Receiver<(DfControl, f32)>;

fn init_df(model_bytes: &[u8], channels: usize) -> Result<DfTract> {
    let df_params = DfParams::from_bytes_tar(model_bytes)?;
    let r_params = RuntimeParams::default_with_ch(channels);
    let df = DfTract::new(df_params, &r_params).expect("Could not initialize DeepFilter runtime");

    Ok(df)
}

pub struct RealTimeProcess {
    pub sr: usize,
    pub frame_size: usize,
    pub freq_size: usize,
    should_stop: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
}

pub struct ProcessSetting {
    pub input_sr: usize,
    pub output_sr: usize,
    pub s_lsnr: Option<mpsc::Sender<f32>>,
    pub s_spec: Option<(SendSpec, SendSpec)>,
    pub r_opt: Option<RecvControl>,
}

impl RealTimeProcess {
    pub fn init_from_targz(
        model_bytes: &[u8],
        setting: ProcessSetting,
    ) -> Result<(Self, RbProd, RbCons)> {
        let ch = 1;
        let df_params = DfParams::from_bytes(model_bytes)?;
        let r_params = RuntimeParams::default_with_ch(ch);
        let df =
            DfTract::new(df_params, &r_params).expect("Could not initialize DeepFilter runtime");

        Self::init_df(df, setting)
    }

    pub fn init_from_tar(
        model_bytes: &[u8],
        setting: ProcessSetting,
    ) -> Result<(Self, RbProd, RbCons)> {
        let ch = 1;
        let df = init_df(model_bytes, ch)?;

        Self::init_df(df, setting)
    }

    fn init_df(df: DfTract, setting: ProcessSetting) -> Result<(Self, RbProd, RbCons)> {
        let sr = df.sr;
        let frame_size = df.hop_size;
        let freq_size = df.n_freqs;

        let ProcessSetting {
            input_sr,
            output_sr,
            s_lsnr,
            s_spec,
            r_opt,
        } = setting;

        let in_rb = HeapRb::<f32>::new(frame_size * 100);
        let out_rb = HeapRb::<f32>::new(frame_size * 100);
        let (in_prod, in_cons) = in_rb.split();
        let (out_prod, out_cons) = out_rb.split();
        let mut in_prod = in_prod.into_postponed();
        let out_prod = out_prod.into_postponed();

        let should_stop = Arc::new(AtomicBool::new(false));
        let has_init = Arc::new(AtomicBool::new(false));

        let controls = AtomicControls {
            has_init: has_init.clone(),
            should_stop: should_stop.clone(),
        };

        {
            let padding = vec![0.0f32; frame_size];
            let mut n = 0;
            while n < padding.len() {
                n += in_prod.push_slice(&padding[n..]);
            }
        }

        let df_com = GuiCom {
            s_lsnr,
            s_spec,
            r_opt,
        };

        let worker_handle = Some(thread::spawn(get_worker_fn(
            df, in_cons, out_prod, input_sr, output_sr, controls, df_com,
        )));

        while !has_init.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs_f32(0.01));
        }
        log::info!("DeepFilter Capture init");

        Ok((
            Self {
                sr,
                freq_size,
                frame_size,
                should_stop,
                worker_handle,
            },
            in_prod,
            out_cons,
        ))
    }

    pub fn should_stop(&mut self) -> Result<()> {
        if let Some(h) = self.worker_handle.take() {
            log::info!("Joining DF Worker");
            self.should_stop.swap(true, Ordering::Relaxed);
            h.join().expect("Error during DF worker join");
        }
        Ok(())
    }

    pub fn frame_size(&self) -> usize {
        self.frame_size
    }
}

fn get_worker_fn(
    df: DfTract,
    mut rb_in: RbCons,
    mut rb_out: RbProd,
    input_sr: usize,
    output_sr: usize,
    controls: AtomicControls,
    df_com: GuiCom,
) -> impl FnMut() {
    let (has_init, should_stop) = controls.into_inner();
    let df = Box::into_raw(Box::new(df));
    let df = df as usize;

    let (s_lsnr, mut s_spec, mut r_opt) = df_com.into_inner();

    move || {
        let mut df = unsafe { Box::from_raw(df as *mut DfTract) }; // Rc non-sense

        debug_assert_eq!(df.ch, 1); // Processing for more channels are not implemented yet
        let mut inframe = Array2::zeros((df.ch, df.hop_size));
        let mut outframe = inframe.clone();
        df.process(inframe.view(), outframe.view_mut())
            .expect("Failed to run DeepFilterNet");
        has_init.store(true, Ordering::Relaxed);
        log::info!("Worker init");
        let (mut input_resampler, n_in) = if input_sr != df.sr {
            let r = FftFixedOut::<f32>::new(input_sr, df.sr, df.hop_size, 1, 1)
                .expect("Failed to init input resampler");
            let n_in = r.input_frames_max();
            let buf = r.input_buffer_allocate(true);
            (Some((r, buf)), n_in)
        } else {
            (None, df.hop_size)
        };
        let (mut output_resampler, n_out) = if output_sr != df.sr {
            let r = FftFixedIn::<f32>::new(df.sr, output_sr, df.hop_size, 1, 1)
                .expect("Failed to init output resampler");
            let n_out = r.output_frames_max();
            let buf = r.output_buffer_allocate(true);
            (Some((r, buf)), n_out)
        } else {
            (None, df.hop_size)
        };
        while !should_stop.load(Ordering::Relaxed) {
            if rb_in.len() < n_in {
                // Sleep for half a hop size
                thread::sleep(Duration::from_secs_f32(
                    df.hop_size as f32 / df.sr as f32 / 2.,
                ));
                continue;
            }
            if let Some((ref mut r, ref mut buf)) = input_resampler.as_mut() {
                let n = rb_in.pop_slice(&mut buf[0]);
                debug_assert_eq!(n, n_in);
                debug_assert_eq!(n, r.input_frames_next());
                r.process_into_buffer(buf, &mut [inframe.as_slice_mut().unwrap()], None)
                    .unwrap();
            } else {
                let n = rb_in.pop_slice(inframe.as_slice_mut().unwrap());
                debug_assert_eq!(n, n_in);
                //debug_assert!(n > 0);
            }
            let lsnr = df
                .process(inframe.view(), outframe.view_mut())
                .expect("Failed to run DeepFilterNet");
            let mut n = 0;
            if let Some((ref mut r, ref mut buf)) = output_resampler.as_mut() {
                r.process_into_buffer(&[outframe.as_slice().unwrap()], buf, None).unwrap();
                while n < n_out {
                    n += rb_out.push_slice(&buf[0][n..]);
                    //debug_assert!(n > 0);
                }
            } else {
                let buf = outframe.as_slice().unwrap();
                while n < n_out {
                    n += rb_out.push_slice(&buf[n..]);
                }
            }
            debug_assert_eq!(n, n_out);
            rb_out.sync();
            if let Some(s_lsnr) = &s_lsnr {
                s_lsnr.send(lsnr).unwrap();
            }

            if let Some((ref mut s_noisy, ref mut s_enh)) = s_spec.as_mut() {
                push_spec(df.get_spec_noisy(), s_noisy);
                push_spec(df.get_spec_enh(), s_enh);
            }

            if let Some(ref mut r_opt) = r_opt.as_mut() {
                while let Ok((c, v)) = r_opt.try_recv() {
                    match c {
                        DfControl::AttenLim => df.set_atten_lim(v),
                        DfControl::PostFilterBeta => df.set_pf_beta(v),
                        DfControl::MinThreshDb => df.min_db_thresh = v,
                        DfControl::MaxErbThreshDb => df.max_db_erb_thresh = v,
                        DfControl::MaxDfThreshDb => df.max_db_df_thresh = v,
                    }
                }
            }
        }
    }
}

fn push_spec(spec: ArrayView2<Complex32>, sender: &SendSpec) {
    debug_assert_eq!(spec.len_of(Axis(0)), 1); // only single channel for now
    let out = spec.iter().map(|x| x.norm_sqr().max(1e-10).log10() * 10.).collect::<Vec<f32>>();
    sender.send(out.into_boxed_slice()).expect("Failed to send spectrogram")
}

pub(crate) struct AtomicControls {
    has_init: Arc<AtomicBool>,
    should_stop: Arc<AtomicBool>,
}

impl AtomicControls {
    pub fn into_inner(self) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (self.has_init, self.should_stop)
    }
}

pub(crate) struct GuiCom {
    pub s_lsnr: Option<mpsc::Sender<f32>>,
    pub s_spec: Option<(SendSpec, SendSpec)>,
    pub r_opt: Option<RecvControl>,
}

impl GuiCom {
    pub fn into_inner(
        self,
    ) -> (
        Option<mpsc::Sender<f32>>,
        Option<(SendSpec, SendSpec)>,
        Option<RecvControl>,
    ) {
        (self.s_lsnr, self.s_spec, self.r_opt)
    }
}

#[derive(PartialEq)]
pub enum DfControl {
    AttenLim,
    PostFilterBeta,
    MinThreshDb,
    MaxErbThreshDb,
    MaxDfThreshDb,
}
