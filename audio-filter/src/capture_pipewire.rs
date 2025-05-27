use std::ffi::{c_void, CString};
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use df::tract::{DfParams, DfTract, RuntimeParams};
use libc::fnmatch;
//use libspa::pod::builder::Builder;
use pipewire::loop_::LoopRef;
use pipewire::main_loop::MainLoop;
use pw::spa::pod::builder::Builder;
// Import necessary PipeWire and SPA FFI types
use pipewire as pw;
use pw::spa::{self as spa, sys::spa_io_position};
use ringbuf::producer::PostponedProducer;
//use ringbuf::storage::Heap;
//use ringbuf::traits::{Consumer, Producer, Split};
//use ringbuf::wrap::caching::Caching;
use ringbuf::{Consumer, HeapRb, Producer, SharedRb};

pub const DEFAULT_RATE: u32 = 44100;
pub const DEFAULT_CHANNELS: u32 = 2;
pub const DEFAULT_VOLUME: f64 = 0.5;
pub const CHAN_SIZE: usize = std::mem::size_of::<i16>();

//pub type RbProd = Producer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbProd = PostponedProducer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbCons = Consumer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;

static mut MODEL: Option<Arc<DfTract>> = None;

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
    unsafe { MODEL = Some(Arc::new(df)) };
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
}

impl PipewireFilterCapture {
    fn new(model_path: Option<PathBuf>) -> Self {
        let ch = 1;
        let (sr, frame_size, freq_size) = init_df(model_path, ch);
        let heaprb = HeapRb::<f32>::new(frame_size * 100);
        let (rb_prod, rb_cons) = heaprb.split();
        let rb_prod = rb_prod.into_postponed();

        let should_stop = Arc::new(AtomicBool::new(false));

        Self {
            sr, freq_size, frame_size, should_stop
        }
    }
}

fn get_frame_size() -> usize {
    let df = unsafe { MODEL.clone().unwrap() };
    df.hop_size
}

fn get_audio_filter(mainloop: &MainLoop) -> AudioFilter<f64> {
    let filter_props = pw::properties::properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Filter",
        *pw::keys::MEDIA_ROLE => "DSP",
    };

    // create a filter with two port
    let mut data_filter = AudioFilter::new(
        mainloop.loop_(),
        "rust-audio-filter",
        filter_props.clone(),
        0.0,
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

    let mut buffer = vec![0u8; 1024];
    let spa_builder = Builder::new(&mut buffer);
    //spa_builder.push_object(frame, type_, id)

    println!("{:?}", line!());
    let info = Box::new(spa::sys::spa_process_latency_info {
        quantum: 0.,
        rate: 0,
        ns: 10 * pw::spa::sys::SPA_NSEC_PER_MSEC as u64,
    });

    let info = Box::into_raw(info);
    println!("{:?}", line!());
    let params = unsafe {
        [spa::sys::spa_process_latency_build(
            spa_builder.as_raw_ptr(),
            spa::sys::SPA_PARAM_ProcessLatency,
            info,
        )]
    };

    println!("{:?}", line!());

    data_filter.process(|userdata, ports, position| {
        let n_samples = position.clock.duration;
        let Some(in_buf) = ports[0].data_mut::<f32>(n_samples as usize) else {
            return;
        };
        let Some(out_buf) = ports[1].data_mut::<f32>(n_samples as usize) else {
            return;
        };

        println!("number of samples: {:?}", n_samples);
        //userdata.prods.push_iter(in_buf.iter().cloned());

        for (o, i) in out_buf.iter_mut().zip(in_buf.iter()) {
            *o = 2.0 * *i;
        }
    });
    data_filter.connect(&params);

    data_filter
}

fn main() {
    pw::init();
    let main_loop = MainLoop::new(None).unwrap();

    let filter_props = pw::properties::properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Filter",
        *pw::keys::MEDIA_ROLE => "DSP",
    };

    // create a filter with two port
    let mut data_filter = AudioFilter::new(
        main_loop.loop_(),
        "rust-audio-filter",
        filter_props.clone(),
        0.0,
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

    let mut buffer = vec![0u8; 1024];
    let spa_builder = Builder::new(&mut buffer);
    //spa_builder.push_object(frame, type_, id)

    println!("{:?}", line!());
    let info = Box::new(spa::sys::spa_process_latency_info {
        quantum: 0.,
        rate: 0,
        ns: 10 * pw::spa::sys::SPA_NSEC_PER_MSEC as u64,
    });

    let info = Box::into_raw(info);
    println!("{:?}", line!());
    let params = unsafe {
        [spa::sys::spa_process_latency_build(
            spa_builder.as_raw_ptr(),
            spa::sys::SPA_PARAM_ProcessLatency,
            info,
        )]
    };

    println!("{:?}", line!());

    data_filter.process(|userdata, ports, position| {
        let n_samples = position.clock.duration;
        let Some(in_buf) = ports[0].data_mut::<f32>(n_samples as usize) else {
            return;
        };
        let Some(out_buf) = ports[1].data_mut::<f32>(n_samples as usize) else {
            return;
        };

        println!("number of samples: {:?}", n_samples);
        //userdata.prods.push_iter(in_buf.iter().cloned());

        for (o, i) in out_buf.iter_mut().zip(in_buf.iter()) {
            *o = 2.0 * *i;
        }
    });
    data_filter.connect(&params);
    // Connect the filter
    // SAFETY: pw_filter_connect expects valid pointers.
    println!("PipeWire audio filter running. Press Ctrl+C to quit.");

    main_loop.run();

    println!("Quitting.");
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
