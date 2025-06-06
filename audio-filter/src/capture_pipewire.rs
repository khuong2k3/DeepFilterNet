use std::ffi::{c_void, CString};
use std::io::{stdout, Write};
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::Result;
use df::tract::{DfParams, DfTract, RuntimeParams};
use ndarray::Array2;
use pipewire::loop_::LoopRef;
use pipewire::main_loop::MainLoop;
use pw::spa::pod::builder::Builder;
// Import necessary PipeWire and SPA FFI types
use pipewire as pw;
use pw::spa::{self as spa, sys::spa_io_position};
use ringbuf::producer::PostponedProducer;

use ringbuf::{Consumer, HeapRb, SharedRb};
use rubato::{FftFixedIn, FftFixedOut, Resampler};

pub const DEFAULT_RATE: u32 = 44100;
pub const DEFAULT_CHANNELS: u32 = 2;
pub const DEFAULT_VOLUME: f64 = 0.5;
pub const CHAN_SIZE: usize = std::mem::size_of::<i16>();

//pub type RbProd = Producer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbProd = PostponedProducer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbCons = Consumer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;

static mut MODEL: Option<DfTract> = None;

fn init_df(model_path: Option<PathBuf>, channels: usize) -> (usize, usize, usize) {
    unsafe {
        if let Some(m) = MODEL.as_ref() {
            if m.ch == channels {
                return (m.sr, m.hop_size, m.n_freqs);
            }
        }
    }
    // let df_params = DfParams::default();
    let df_params = if let Some(path) = model_path {
        DfParams::new(path).expect("Failed to read DF model")
    } else {
        DfParams::default()
    };
    let r_params = RuntimeParams::default_with_ch(channels);
    let df = DfTract::new(df_params, &r_params).expect("Could not initialize DeepFilter runtime");
    let (sr, frame_size, freq_size) = (df.sr, df.hop_size, df.n_freqs);
    unsafe { MODEL = Some(df) };
    (sr, frame_size, freq_size)
}

struct UserData {
    prods: RbProd,
    cons: RbCons,
}

struct PipewireFilterCapture {
    pub sr: usize,
    pub frame_size: usize,
    pub freq_size: usize,
    should_stop: Arc<AtomicBool>,
    audio_filter: AudioFilter<UserData>,
    worker_handle: Option<JoinHandle<()>>,
}

impl PipewireFilterCapture {
    fn new(mainloop: &MainLoop, model_path: Option<PathBuf>, s_lsnr: Option<mpsc::Sender<f32>>) -> Self {
        let ch = 1;
        let (sr, frame_size, freq_size) = init_df(model_path, ch);
        //let heaprb = HeapRb::<f32>::new(frame_size * 100);
        //let (rb_prod, rb_cons) = heaprb.split();
        //let rb_prod = rb_prod.into_postponed();

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
                n += in_prod.push_iter(&mut padding[n..].iter().cloned());
            }
        }
        let user_data = UserData {
            prods: in_prod,
            cons: out_cons,
        };

        // create a filter with two port
        let mut audio_filter = get_audio_filter(&mainloop, user_data);
        
        let worker_handle = Some(thread::spawn(get_worker_fn(
            in_cons,
            out_prod,
            sr,
            sr,
            controls,
            s_lsnr,
        )));

        let mut buffer = vec![0u8; 1024];
        let spa_builder = Builder::new(&mut buffer);

        audio_filter.process(|user_data, ports, position| {
            let n_samples = position.clock.duration;
            let Some(in_buf) = ports[0].data_mut::<f32>(n_samples as usize) else {
                return;
            };
            let Some(out_buf) = ports[1].data_mut::<f32>(n_samples as usize) else {
                return;
            };

            let mut n = 0;
            while n < n_samples as usize {
                n += user_data.prods.push_iter(&mut in_buf[n..].iter().cloned());
            }
            user_data.prods.sync();

            let mut n = 0;
            while n < n_samples as usize {
                n += user_data.cons.pop_slice(&mut out_buf[n..]);
            }
        });

        let info = Box::new(spa::sys::spa_process_latency_info {
            quantum: 0.,
            rate: 0,
            ns: 10 * pw::spa::sys::SPA_NSEC_PER_MSEC as u64,
        });

        let info = Box::into_raw(info);
        let params = unsafe {
            [spa::sys::spa_process_latency_build(
                spa_builder.as_raw_ptr(),
                spa::sys::SPA_PARAM_ProcessLatency,
                info,
            )]
        };

        while !has_init.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs_f32(0.01));
        }
        log::info!("DeepFilter Capture init");

        audio_filter.connect(&params);

        Self {
            sr,
            freq_size,
            frame_size,
            should_stop,
            audio_filter,
            worker_handle,
        }
    }

    pub fn should_stop(&mut self) -> Result<()> {
        //self.sink.pause()?;
        //self.source.pause()?;
        if let Some(h) = self.worker_handle.take() {
            log::info!("Joining DF Worker");
            self.should_stop.swap(true, Ordering::Relaxed);
            h.join().expect("Error during DF worker join");
        }
        Ok(())
    }
}

fn get_worker_fn(
    mut rb_in: RbCons,
    mut rb_out: RbProd,
    input_sr: usize,
    output_sr: usize,
    controls: AtomicControls,
    s_lsnr: Option<mpsc::Sender<f32>>,
) -> impl FnMut() {
    let (has_init, should_stop) = controls.into_inner();
    move || {
        let mut df = unsafe { MODEL.clone().unwrap() };
        df.set_atten_lim(30.0);
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
        }
    }
}

fn get_frame_size() -> usize {
    let df = unsafe { MODEL.clone().unwrap() };
    df.hop_size
}

fn get_audio_filter<D>(mainloop: &MainLoop, data: D) -> AudioFilter<D> {
    let filter_props = pw::properties::properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CLASS => "Audio/Filter",
        *pw::keys::MEDIA_ROLE => "DSP",
    };

    // create a filter with two port
    let mut data_filter = AudioFilter::new(
        mainloop.loop_(),
        "rust-audio-filter",
        filter_props.clone(),
        data,
    )
    .unwrap();

    let input_port_props = pw::properties::properties! {
        *pw::keys::FORMAT_DSP => "32 bit float mono audio",
        *pw::keys::PORT_NAME => "input",
    };

    data_filter
        .add_port(
            SpaDirection::Input,
            pw::sys::pw_filter_port_flags_PW_FILTER_PORT_FLAG_MAP_BUFFERS, // ???????
            input_port_props,
        )
        .unwrap();

    let output_port_props = pw::properties::properties! {
        *pw::keys::FORMAT_DSP => "32 bit float mono audio",
        *pw::keys::PORT_NAME => "output"
    };

    data_filter
        .add_port(
            SpaDirection::Ouput,
            pw::sys::pw_filter_port_flags_PW_FILTER_PORT_FLAG_MAP_BUFFERS,
            output_port_props,
        )
        .unwrap();

    data_filter
}

fn main() {
    pw::init();
    let main_loop = MainLoop::new(None).unwrap();
    let (lsnr_prod, lsnr_cons) = mpsc::channel();
    let _filter_capture = PipewireFilterCapture::new(&main_loop, None, Some(lsnr_prod));

    // Connect the filter
    // SAFETY: pw_filter_connect expects valid pointers.
    println!("PipeWire audio filter running. Press Ctrl+C to quit.");

    println!("{}", line!());
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(200));
            while let Ok(lsnr) = lsnr_cons.try_recv() {
                print!("\rCurrent SNR: {:>5.1} dB{esc}[1;", lsnr, esc = 27 as char);
            }
            stdout().flush().unwrap();
        }
    });
    main_loop.run();
    println!("Quitting.");
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

struct AudioFilter<D> {
    local_data: Box<FilterLocalData<D>>,
    #[allow(dead_code)]
    filter_events: Box<pw::sys::pw_filter_events>,
    filter: *mut pw::sys::pw_filter,
    _props: Vec<pw::properties::Properties>,
}

impl<D> AudioFilter<D> {
    unsafe extern "C" fn on_process(userdata: *mut c_void, position: *mut spa_io_position) {
        let data_ptr = userdata as *mut FilterLocalData<D>;
        let data = &mut *data_ptr; // Reborrow the Data struct
        let pos_struct = position.read();
        if let Some(process_f) = &mut data.process {
            process_f(&mut data.data, &mut data.port, &pos_struct);
        }
    }

    fn new(
        loop_: &LoopRef,
        name: &str,
        props: pw::properties::Properties,
        data: D,
    ) -> Option<Self> {
        let name = CString::new(name).unwrap();
        let filter_events = Box::new(pw::sys::pw_filter_events {
            version: pw::sys::PW_VERSION_FILTER_EVENTS,
            destroy: None,
            state_changed: None,
            io_changed: None,
            param_changed: None,
            add_buffer: None,
            remove_buffer: None,
            process: Some(Self::on_process),
            drained: None,
            command: None,
        });
        let local_data = Box::new(FilterLocalData {
            data: Box::new(data),
            port: vec![],
            process: None,
        });

        let local_data_ref = Box::into_raw(local_data);

        let filter = unsafe {
            pw::sys::pw_filter_new_simple(
                loop_.as_raw_ptr(),
                name.as_ptr(),
                props.as_raw_ptr(), // Consumes and returns raw properties
                filter_events.as_ref() as *const _ as *mut _, // Cast immutable to mutable for FFI (common pattern for const structs in C)
                local_data_ref as *mut c_void, // Pass the leaked `Data` struct as userdata
            )
        };
        if filter.is_null() {
            return None;
        }
        let local_data = unsafe { Box::from_raw(local_data_ref) };
        Some(Self {
            local_data,
            filter,
            filter_events,
            _props: vec![props],
        })
    }

    fn add_port(
        &mut self,
        spa_direction: SpaDirection,
        port_flags: pw::sys::pw_filter_port_flags,
        props: pw::properties::Properties,
    ) -> Option<()> {
        let port = unsafe {
            pw::sys::pw_filter_add_port(
                self.filter,
                spa_direction.into(),
                port_flags,
                //pw::sys::pw_filter_port_flags_PW_FILTER_PORT_FLAG_MAP_BUFFERS,
                0, // No extra port data
                props.as_raw_ptr(),
                ptr::null_mut(), // No extra parameters for this port
                0,
            ) as *mut pw::sys::pw_port
        };
        if port.is_null() {
            return None;
        }
        self.local_data.port.push(FilterPort(port));
        self._props.push(props);
        Some(())
    }

    fn process(&mut self, f: impl FnMut(&mut D, &mut [FilterPort], &spa_io_position) + 'static) {
        self.local_data.process = Some(Box::new(f));
    }

    fn connect(&mut self, params: &[*mut spa::sys::spa_pod]) -> Option<()> {
        let connect_result = unsafe {
            pw::sys::pw_filter_connect(
                self.filter,
                pw::sys::pw_filter_flags_PW_FILTER_FLAG_RT_PROCESS,
                &params.as_ptr() as *const *const _ as *mut *const _,
                params.len() as u32,
            )
        };
        if connect_result < 1 {
            return None;
        }
        Some(())
    }
}

impl<D> Drop for AudioFilter<D> {
    fn drop(&mut self) {
        unsafe { pw::sys::pw_filter_destroy(self.filter) };
    }
}

struct FilterLocalData<D> {
    data: Box<D>,
    port: Vec<FilterPort>,
    process: Option<Box<dyn FnMut(&mut D, &mut [FilterPort], &spa_io_position)>>,
}

#[repr(transparent)]
struct FilterPort(*mut pw::sys::pw_port);

impl FilterPort {
    fn data_mut<T>(&self, n_samples: usize) -> Option<&mut [T]> {
        let data_ptr =
            unsafe { pw::sys::pw_filter_get_dsp_buffer(self.0 as *mut c_void, n_samples as u32) }
                as *mut T;

        if data_ptr.is_null() {
            return None;
        }
        unsafe { Some(std::slice::from_raw_parts_mut(data_ptr, n_samples)) }
    }
}

impl Drop for FilterPort {
    fn drop(&mut self) {}
}

#[derive(Clone, Copy)]
enum SpaDirection {
    Input,
    Ouput,
}

impl Into<u32> for SpaDirection {
    fn into(self) -> u32 {
        match self {
            Self::Input => pw::spa::sys::SPA_DIRECTION_INPUT,
            Self::Ouput => pw::spa::sys::SPA_DIRECTION_OUTPUT,
        }
    }
}
