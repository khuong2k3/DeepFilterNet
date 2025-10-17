use std::f32::consts::TAU;
use std::fmt::Display;
use std::future::Future;
use std::io::{stdout, BufWriter, Write};
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::{fs, thread};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{BuildStreamError, Stream, StreamConfig, SupportedStreamConfigRange};
use crossbeam_channel::{bounded, unbounded};
use df::realtime;
use df::tract::{DfParams, DfTract};
use iced::widget::{
    button, column, container, horizontal_space, image, mouse_area, pick_list, row, slider, stack,
    text, tooltip, vertical_space,
};
use iced::{theme, Length, Point, Subscription};
use iced::{Element, Task};
use image_rs::{imageops, Rgba, RgbaImage};
use itertools::{izip, Itertools};
use ndarray::Array2;
use ringbuf::ring_buffer::{RbWrap, RbWriteCache};
use ringbuf::Producer;
use rustfft::num_complex::{Complex32, ComplexFloat};
use rustfft::Fft;

use ringbuf::{producer::PostponedProducer, Consumer, SharedRb};

use crate::audio::AudioSamples;

pub type RbProd = PostponedProducer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbCons = Consumer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type SendLsnr = Sender<f32>;
pub type RecvLsnr = Receiver<f32>;
pub type SendSpec = Sender<Box<[f32]>>;
pub type RecvSpec = Receiver<Box<[f32]>>;
pub type RingPod =
    Producer<f32, RbWrap<RbWriteCache<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>>>;
pub type RingCon = Consumer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
//pub type SendControl = Sender<(DfControl, f32)>;
//pub type RecvControl = Receiver<(DfControl, f32)>;

type Receiver<T> = crossbeam_channel::Receiver<T>;
type Sender<T> = crossbeam_channel::Sender<T>;
mod audio;
mod cmap;
const DB_MIN: f32 = -90.0;
const DB_MAX: f32 = -10.0;

const SAMPLE_FORMAT: cpal::SampleFormat = cpal::SampleFormat::F32;
const SAMPLE_SIZE: usize = size_of::<f32>();

const N_FFT: usize = 512;

mod filter;

fn main() -> Result<(), iced::Error> {
    env_logger::init();
    log::set_max_level(log::LevelFilter::Info);

    iced::application("A cool counter", AudioEdit::update, AudioEdit::view)
        .font(include_bytes!("../font/file-font.ttf").as_slice())
        .subscription(AudioEdit::subscription)
        .theme(AudioEdit::theme)
        .run_with(AudioEdit::new)
}

#[derive(Clone, Copy, Debug)]
enum Error {
    FileLoadError,
    NoFileSelected,
}

enum AudioSinkCtrl {
    ChangeTimesamp(usize),
}

struct ModelInfo {
    atten_db: f32,
    filter_beta: f32,
    #[allow(unused)]
    r_lsnr: mpsc::Receiver<f32>,
    s_opt: crossbeam_channel::Sender<(realtime::DfControl, f32)>,
    r_enh: crossbeam_channel::Receiver<Box<[f32]>>,
    //r_noisy: crossbeam_channel::Receiver<Box<[f32]>>,
    process: realtime::RealTimeProcess,
    in_pod: Arc<Mutex<RingPod>>,
    out_con: Arc<Mutex<RingCon>>,
}

#[derive(Default)]
struct AudioViewCtrl {
    on_change_freq: bool,
}

struct AudioEdit {
    samples: Option<AudioSamples>,
    au_sink: AudioSink,
    audio_ctl: Option<Sender<AudioSinkCtrl>>,
    fft: FftProcess,
    audio_ft: FtAudio,
    spec: SpecImage,
    ft_image: image::Handle,
    spec_image: image::Handle,
    model_info: ModelInfo,
    timestamp: usize,
    audio_view_ctrl: AudioViewCtrl,
    visualizer: AudioVisualizer,
    is_playing: bool,
}

//impl Default for ModelInfo {
//    fn default() -> Self {
//        Self {
//            atten_db: 30.0,
//            filter_beta: 40.0,
//        }
//    }
//}
//
const MODEL_BYTES: &[u8] = include_bytes!("../../models/DeepFilterNet3_ll_onnx.tar.gz");
static mut MODEL: Option<DfTract> = None;

fn init_df(model_bytes: &[u8]) -> anyhow::Result<()> {
    let df_params = DfParams::from_bytes(model_bytes)?;
    let r_params = df::tract::RuntimeParams::default_with_ch(1);
    let df = DfTract::new(df_params, &r_params).expect("Could not initialize DeepFilter runtime");

    unsafe {
        MODEL = Some(df);
    }
    Ok(())
}

impl Default for AudioEdit {
    fn default() -> Self {
        let au_sink = AudioSink::new().unwrap();
        let fft = FftProcess::new(N_FFT);

        let (s_lsnr, r_lsnr) = mpsc::channel();
        let (s_opt, r_opt) = crossbeam_channel::unbounded();
        let (s_enh, r_enh) = crossbeam_channel::unbounded();
        init_df(MODEL_BYTES).unwrap();
        //let (s_noisy, r_noisy) = crossbeam_channel::unbounded();

        let (_process, in_pod, out_con) = df::realtime::RealTimeProcess::init_from_targz(
            MODEL_BYTES,
            df::realtime::ProcessSetting {
                input_sr: 16_000,
                output_sr: 16_000,
                s_lsnr: Some(s_lsnr),
                s_spec_noisy: None,
                s_spec_enh: Some(s_enh),
                r_opt: Some(r_opt),
            },
        )
        .expect("Can't init model");
        //println!("{}", _process.frame_size());
        let audioft = FtAudio::new(200, 300, _process.n_fft / 2 + 1);
        let spec = SpecImage::new(300, _process.n_fft as u32 / 2 + 1, DB_MIN, DB_MAX);

        s_opt.send((realtime::DfControl::OutputSr, _process.sr as f32)).unwrap();
        Self {
            au_sink,
            samples: None,
            ft_image: audioft.image_handle(),
            spec_image: spec.image_handle(),
            audio_ft: audioft,
            audio_ctl: None,
            spec,
            fft,
            model_info: ModelInfo {
                atten_db: 100.0,
                filter_beta: 0.02,
                r_lsnr,
                s_opt,
                r_enh,
                //r_noisy,
                process: _process,
                in_pod: Arc::new(Mutex::new(in_pod)),
                out_con: Arc::new(Mutex::new(out_con)),
            },
            is_playing: false,
            timestamp: 0,
            audio_view_ctrl: Default::default(),
            visualizer: AudioVisualizer::FT,
        }
    }
}

impl AudioEdit {
    fn new() -> (Self, Task<Message>) {
        (Self::default(), Task::none())
    }

    fn theme(&self) -> theme::Theme {
        iced::Theme::Dark
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::LoadFile(path) => Task::perform(load_audio_file(path), Message::FileLoaded),
            Message::FileLoaded(samples) => {
                match samples {
                    Ok(samples) => {
                        self.model_info
                            .s_opt
                            .send((realtime::DfControl::InputSr, samples.sr() as f32))
                            .unwrap();
                        self.audio_ctl = self
                            .au_sink
                            .load(
                                samples.clone(),
                                self.model_info.in_pod.clone(),
                                self.model_info.out_con.clone(),
                                self.model_info.process.frame_size(),
                                self.model_info.s_opt.clone(),
                            )
                            .ok();
                        self.samples = Some(samples);
                        self.is_playing = true;
                    }
                    Err(_) => {}
                }
                Task::none()
            }
            Message::PickFile => Task::perform(pick_file(), |v| {
                if let Ok(v) = v {
                    Message::LoadFile(v)
                } else {
                    Message::None
                }
            }),
            Message::Save => {
                self.au_sink.pause().unwrap();
                if let Some(samples) = &self.samples {
                    let save_fut = set_save_file(
                        samples.clone(),
                        //self.model_info.in_pod.clone(),
                        //self.model_info.out_con.clone(),
                        //self.model_info.process.frame_size,
                    );

                    Task::perform(save_fut, |path| match path {
                        Some(path) => Message::SaveFile(path),
                        None => Message::None,
                    })
                } else {
                    Task::none()
                }
            }
            Message::SaveFile(pathbuf) => {
                let task = save_file(
                    pathbuf,
                    self.samples.clone().unwrap(),
                    self.model_info.process.frame_size,
                );
                Task::perform(task, |_| Message::None)
            }
            // audio message
            Message::AudioSinkMessage(messages) => {
                for message in messages {
                    match message {
                        AudioSinkMessage::FileEnded => {
                            log::info!("Audio stream ended");
                            self.au_sink.pause().unwrap();
                        }
                        AudioSinkMessage::Duration(timestamp) => {
                            self.timestamp = timestamp;
                        }
                        AudioSinkMessage::Destroyed => {
                            self.au_sink.destroy_stream().unwrap();
                            self.samples = None;
                        }
                    }
                }
                Task::none()
            }
            Message::ChangeTimestamp(timestamp) => {
                if let Some(audio_ctl) = &mut self.audio_ctl {
                    self.au_sink.pause().unwrap();
                    self.timestamp = timestamp as usize;
                    let _ = audio_ctl.send(AudioSinkCtrl::ChangeTimesamp(self.timestamp));
                }
                Task::none()
            }
            Message::Pause => {
                self.is_playing = false;
                self.au_sink.pause().unwrap();
                Task::none()
            }
            Message::Play => {
                self.au_sink.play().unwrap();
                self.is_playing = true;
                Task::none()
            }
            // real time message
            Message::Tick => {
                let mut tasks = vec![];
                tasks.push(Task::perform(async {}, |_| Message::ProcessTick));
                if let Some(messages) = self.au_sink.audio_message() {
                    tasks.push(Task::perform(messages, Message::AudioSinkMessage));
                }

                Task::batch(tasks)
            }
            Message::ProcessTick => {
                let rev_length = self.model_info.r_enh.len();
                if rev_length > 0 {
                    let spec = self.model_info.r_enh.iter().take(rev_length).collect::<Vec<_>>();
                    self.update_spec(spec.iter(), rev_length);
                    self.update_ft(spec.last().unwrap().iter().cloned());
                }

                Task::none()
            }
            // event message
            Message::MouseAudioViewPress => {
                self.audio_view_ctrl.on_change_freq = true;
                Task::none()
            }
            Message::MouseAudioView(_) => {
                //if let (Some(configs), true) =
                //    (&self.au_sink.configs, self.audio_view_ctrl.on_change_freq)
                //{
                //    //let freq_bin = p.x.min(self.audio_ft.freq_bins() as f32) / self.audio_ft.freq_bins() as f32;
                //    //let sr = configs.sample_rate.0 as f32;
                //    //let freq = freq_bin * sr;
                //    //
                //    //self.au_sink.update_cutoff(freq);
                //}

                Task::none()
            }
            Message::MouseAudioViewRelease => {
                self.audio_view_ctrl.on_change_freq = false;
                Task::none()
            }
            // model message
            Message::AttenChange(atten) => {
                self.model_info.atten_db = atten;
                Task::none()
            }
            Message::PostFilterBeta(beta) => {
                self.model_info.filter_beta = beta;
                Task::none()
            }
            Message::UpdateModel => {
                self.model_info
                    .s_opt
                    .send((realtime::DfControl::AttenLim, self.model_info.atten_db))
                    .unwrap();
                self.model_info
                    .s_opt
                    .send((
                        realtime::DfControl::PostFilterBeta,
                        self.model_info.filter_beta,
                    ))
                    .unwrap();
                Task::none()
            }
            Message::PickVisualizer(visualizer) => {
                self.visualizer = visualizer;
                Task::none()
            }
            Message::None => Task::none(),
        }
    }

    fn view(&self) -> Element<'_, Message> {
        let ft_handle = match self.visualizer {
            AudioVisualizer::FT => image(self.ft_image.clone()),
            AudioVisualizer::Spec => image(self.spec_image.clone()),
        }
        .width(Length::Fill)
        .height(Length::Fill)
        .content_fit(iced::ContentFit::Fill);

        let timestamp = self.timestamp as f32;
        let (duration, sr) = if let Some(samples) = &self.samples {
            (samples.duration() as f32, samples.sr() as f32)
        } else {
            (0.0, 1.0)
        };

        let timestap_second = timestamp / sr;
        let timestamp_minute = timestap_second / 60.0;
        let timestamp_hour = timestamp_minute / 60.0;

        let timestemp_slider = tooltip(
            slider(0.0..=duration, timestamp, Message::ChangeTimestamp).on_release(Message::Play),
            text!(
                "{}:{}:{}",
                timestamp_hour as u32,
                (timestamp_minute as u32) % 60,
                (timestap_second as u32) % 60,
            ),
            tooltip::Position::FollowCursor,
        )
        .style(container::rounded_box);

        let audio_view = mouse_area(ft_handle)
            .on_press(Message::MouseAudioViewPress)
            .on_release(Message::MouseAudioViewRelease)
            .on_move(Message::MouseAudioView)
            .on_exit(Message::MouseAudioViewRelease);

        //let space = horizontal_space().width(Length::Fill);

        let pick_file = button("Pick file").on_press(Message::PickFile);
        let audio_view_row = stack![
            audio_view,
            row![
                column![
                    text!("Noise Attenuation [dB]"),
                    slider(0.0..=100.0, self.model_info.atten_db, Message::AttenChange)
                        .on_release(Message::UpdateModel),
                    text!("Post Filter Beta"),
                    slider(
                        0.0..=1.0,
                        self.model_info.filter_beta,
                        Message::PostFilterBeta
                    )
                    .step(0.001)
                    .on_release(Message::UpdateModel),
                ]
                .width(Length::Fixed(300.0))
                .padding([10, 10]),
                vertical_space().width(Length::Fill),
                column![
                    button("Save").on_press(Message::Save),
                    button("Clear file")
                        .on_press(Message::AudioSinkMessage(vec![AudioSinkMessage::Destroyed])),
                    pick_file,
                    pick_list(
                        [AudioVisualizer::FT, AudioVisualizer::Spec,],
                        self.visualizer.clone().into(),
                        Message::PickVisualizer
                    ),
                ]
                .spacing(10),
            ]
        ];

        //if self.samples.is_some() {
        //    let filter_selector = pick_list(
        //        [FilterType::Lowpass],
        //        None::<FilterType>,
        //        Message::PickFilter,
        //    );
        //
        //    let mut filter_row: Vec<Element<'_, Message>> = vec![];
        //    {
        //        let filter = self.au_sink.filters.lock().unwrap();
        //        if filter.lowpass.is_some() {
        //            filter_row.push(text!("{}", FilterType::Lowpass).into());
        //        }
        //    }
        //
        //    let filter_row = row(filter_row);
        //    let filter_selector_ctrl = column![filter_selector, filter_row];
        //    audio_view_row = audio_view_row.push(filter_selector_ctrl);
        //}

        //let play = button("Play").on_press(Message::Play);
        //let pause = button("Pause").on_press(Message::Pause);

        let play_button = if self.is_playing {
            button("Pause").on_press(Message::Pause)
        } else {
            button("Play").on_press(Message::Play)
        };

        let play_ctrl = row![
            horizontal_space().width(Length::Fill),
            play_button,
            horizontal_space().width(Length::Fill),
        ]
        .spacing(5);

        column![
            //control,
            audio_view_row,
            timestemp_slider,
            play_ctrl,
            //spec_view
        ]
        .spacing(10)
        .padding([10, 10])
        .into()
    }

    #[allow(unused)]
    fn samples2ffts<'a, I>(&mut self, samples: I) -> Vec<Box<[f32]>>
    where
        I: Iterator<Item = &'a Box<[f32]>>,
    {
        samples
            .map(|sample| {
                let fft_samples = self.fft.process(sample.iter().cloned());
                fft_samples.iter().map(|v| amp2db(v.abs())).collect::<Box<[f32]>>()
            })
            .collect()
    }

    fn update_ft<I>(&mut self, fft_samples: I)
    where
        I: Iterator<Item = f32>,
    {
        self.audio_ft.update(fft_samples);
        self.ft_image = self.audio_ft.image_handle();
    }

    fn update_spec<'a, I>(&mut self, fft_samples: I, n_sample: usize)
    where
        I: Iterator<Item = &'a Box<[f32]>>,
    {
        self.spec.update(fft_samples, n_sample);
        self.spec_image = self.spec.image_handle();
    }

    #[allow(unused)]
    fn samples(&self) -> impl Future<Output = Vec<Box<[f32]>>> {
        let samples = self.au_sink.samples();

        async move { samples.await }
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(std::time::Duration::from_millis(20)).map(|_| Message::Tick)
    }
}

async fn load_audio_file(path: impl AsRef<Path>) -> Result<AudioSamples, Error> {
    AudioSamples::load(path).map_err(|_| Error::FileLoadError)
}

struct SpecImage {
    im: RgbaImage,
    n_frames: u32,
    n_freqs: u32,
    vmin: f32,
    vmax: f32,
}

impl SpecImage {
    fn new(n_frames: u32, n_freqs: u32, vmin: f32, vmax: f32) -> Self {
        Self {
            // Store image transposed so we can iterate over rows quickly
            im: RgbaImage::new(n_freqs, n_frames),
            n_frames,
            n_freqs,
            vmin,
            vmax,
        }
    }
    fn w(&self) -> usize {
        self.n_frames as usize
    }
    fn h(&self) -> usize {
        self.n_freqs as usize
    }

    fn update<'a, I>(&mut self, specs: I, mut n_specs: usize)
    where
        I: Iterator<Item = &'a Box<[f32]>>,
    {
        if n_specs == 0 {
            return;
        }
        if n_specs >= self.n_frames as usize {
            // Just drop a few
            n_specs = self.n_frames as usize - 1;
        }
        for (spec, im_row) in specs.take(n_specs).zip(self.im.rows_mut()) {
            for (s, x) in spec.iter().take(self.n_freqs as usize).zip(im_row) {
                // clamp and normalize
                let v = (s.min(self.vmax).max(self.vmin) - self.vmin) / (self.vmax - self.vmin);
                *x = Rgba(cmap::CMAP_INFERNO[(v * 255.) as usize]);
            }
        }
        let (w, h) = (self.w(), self.h());
        self.im.rotate_left((w - n_specs) * 4 * h);
    }
    fn image_handle(&self) -> image::Handle {
        let imt_buf = imageops::rotate270(&self.im).as_raw().to_vec();
        image::Handle::from_rgba(self.n_frames, self.n_freqs, imt_buf)
        //::from_pixels(self.n_frames, self.n_freqs, imt_buf)
    }
}

#[derive(Clone, Copy, Debug)]
enum AudioSinkMessage {
    FileEnded,
    Duration(usize),
    Destroyed,
}

//#[derive(Default)]
//struct Filters {
//    lowpass: Option<LowPassFilter>,
//}

#[allow(unused)]
struct AudioSink {
    host: cpal::Host,
    device: cpal::Device,
    stream: Option<Stream>,
    configs: Option<StreamConfig>,
    in_pod: Sender<Box<[f32]>>,
    out_cons: Receiver<Box<[f32]>>,
    //filters: Arc<Mutex<Filters>>,
    s_mess: Sender<AudioSinkMessage>,
    r_mess: Receiver<AudioSinkMessage>,
}

impl AudioSink {
    fn new() -> Option<Self> {
        let host = cpal::default_host();
        let device = host.default_output_device()?;
        let (in_pod, out_cons) = unbounded();
        //let filters = Arc::new(Mutex::new(Default::default()));

        let (s_mess, r_mess) = bounded::<AudioSinkMessage>(3);

        Some(Self {
            host,
            device,
            stream: None,
            configs: None,
            in_pod,
            out_cons,
            s_mess,
            r_mess,
            //filters,
        })
    }

    fn samples(&self) -> impl Future<Output = Vec<Box<[f32]>>> {
        let out_cons = self.out_cons.clone();

        async move {
            let n_samples = out_cons.len();
            out_cons.iter().take(n_samples).collect::<Vec<_>>()
        }
    }

    //fn update_cutoff(&mut self, fc: f32) {
    //    if let Some(filter) = self.filters.lock().unwrap().lowpass.as_mut() {
    //        filter.update_cutoff(fc);
    //    }
    //}

    fn audio_message(&self) -> Option<impl Future<Output = Vec<AudioSinkMessage>>> {
        let Some(_) = self.stream else {
            return None;
        };
        let mess_len = self.r_mess.len();
        if mess_len > 1 {
            let r_mess = self.r_mess.clone();
            Some(async move { r_mess.iter().take(mess_len).collect() })
        } else {
            None
        }
    }

    fn load(
        &mut self,
        samples: AudioSamples,
        in_pod_model: Arc<Mutex<RingPod>>,
        out_con_model: Arc<Mutex<RingCon>>,
        frame_size: usize,
        s_opt: crossbeam_channel::Sender<(realtime::DfControl, f32)>,
    ) -> Result<Sender<AudioSinkCtrl>, BuildStreamError> {
        let sr = samples.sr();
        let in_channels = samples.channels();

        let settings = get_stream_config(
            &self.device,
            sr as u32,
            frame_size,
            //samples.channels,
            StreamDirection::Output,
        )
        .expect("No suitable audio output config found.");
        s_opt
            .send((realtime::DfControl::OutputSr, settings.sample_rate.0 as f32))
            .unwrap();

        let channels = settings.channels;
        let samples = samples.audio;

        self.configs = Some(settings.clone());

        let mut current_frame = 0;
        log::info!("audio size {}", samples.len());
        let s_mess = self.s_mess.clone();

        let (audio_ctl, audio_rev) = unbounded();

        //println!("channels: {:?}", settings);
        let stream = self.device.build_output_stream::<f32, _, _>(
            &settings,
            move |output, _info| {
                let n_ctl_mess = audio_rev.len();

                if n_ctl_mess > 1 {
                    for message in audio_rev.iter().take(n_ctl_mess) {
                        match message {
                            AudioSinkCtrl::ChangeTimesamp(timestamp) => {
                                current_frame = timestamp * in_channels as usize;
                                //println!("current_frame: {}", current_frame);
                            }
                        }
                    }
                }

                if current_frame >= samples.len() {
                    log::info!("Stream ended");
                    s_mess.send(AudioSinkMessage::FileEnded).unwrap();
                    current_frame = samples.len();
                }

                let mut samples_frame = samples[current_frame..].iter();

                s_mess
                    .send(AudioSinkMessage::Duration(
                        current_frame / in_channels as usize,
                    ))
                    .unwrap();

                let mut rev_au = vec![0.0; output.len() / channels as usize].into_boxed_slice();
                //let mut filter_guard = lowpass_filter.lock().unwrap();

                for i in 0..(rev_au.len()) {
                    let mut value = 0.0;
                    for _ in 0..in_channels {
                        value += *samples_frame.next().unwrap_or(&0.0);
                    }
                    current_frame += in_channels as usize;
                    value /= in_channels as f32;

                    rev_au[i] = value;
                }

                let mut i = 0;
                let mut in_pod_model = in_pod_model.lock().unwrap();
                while i < rev_au.len() {
                    i += in_pod_model.push_slice(&rev_au[i..]);
                }
                in_pod_model.sync();

                let mut out_con_model = out_con_model.lock().unwrap();
                let mut output_au = vec![0.0; rev_au.len()];
                let mut i = 0;
                while i < rev_au.len() {
                    i += out_con_model.pop_slice(&mut output_au[i..]);
                }

                for (i, frame) in output.chunks_mut(channels as usize).enumerate() {
                    for j in 0..(channels as usize) {
                        frame[j] = output_au[i];
                    }
                }
                //in_pod.send(rev_au).unwrap();
            },
            |_| {},
            None,
        )?;
        log::info!("Audio stream created");

        self.stream = Some(stream);

        Ok(audio_ctl)
    }

    fn destroy_stream(&mut self) -> Result<(), cpal::PauseStreamError> {
        self.pause()?;
        self.stream.take();
        Ok(())
    }

    fn play(&mut self) -> Result<(), cpal::PlayStreamError> {
        if let Some(stream) = &self.stream {
            log::info!("Play audio");
            stream.play()?;
        }
        Ok(())
    }

    fn pause(&mut self) -> Result<(), cpal::PauseStreamError> {
        if let Some(stream) = &self.stream {
            log::info!("Pause audio");
            stream.pause()?;
        }
        Ok(())
    }
}

struct FftProcess {
    fft: Arc<dyn Fft<f32>>,
    n_fft: usize,
    scratch: Vec<Complex32>,
    window: Vec<f32>,
}

impl FftProcess {
    fn new(n_fft: usize) -> Self {
        let mut planner: rustfft::FftPlanner<f32> = rustfft::FftPlanner::new();
        let fft = planner.plan_fft_forward(n_fft);
        let scratch = vec![Complex32::ZERO; n_fft];
        let window = hann_window(n_fft).collect();

        Self {
            n_fft,
            fft,
            scratch,
            window,
        }
    }

    fn process<I>(&mut self, samples_frame: I) -> Vec<Complex32>
    where
        I: Iterator<Item = f32>,
    {
        let mut complex_frame = samples_frame
            .take(self.n_fft)
            .zip(self.window.iter())
            .map(|(value, w)| Complex32 {
                re: value * *w,
                im: 0.0,
            })
            .collect::<Vec<_>>();

        self.fft.process_with_scratch(&mut complex_frame, &mut self.scratch);

        complex_frame
    }
}

struct FtAudio {
    im: RgbaImage,
    n_fft: usize,
    height: u32,
    width: u32,
    bar_width: u32,
}

impl FtAudio {
    fn new(width: u32, height: u32, n_fft: usize) -> Self {
        let bar_width = 1;
        //2 * (width / n_fft as u32).max(1);
        Self {
            im: RgbaImage::new(height, n_fft as u32),
            n_fft,
            width,
            height,
            bar_width,
        }
    }

    fn update<I>(&mut self, fft_db: I)
    where
        I: Iterator<Item = f32>,
    {
        let bar_width = self.bar_width as usize;

        let visual_db = fft_db
            //.take(self.n_fft / 2 + 1)
            .map(|value| dbnorm(value) * self.width as f32 * 0.9)
            .map(|value| value as usize);

        for (vi_db, im_rows) in visual_db.zip(self.im.rows_mut().chunks(bar_width).into_iter()) {
            for row in im_rows {
                for (i, x) in row.enumerate() {
                    if i < vi_db {
                        *x = Rgba(cmap::CMAP_INFERNO[100]);
                    } else {
                        *x = Rgba(cmap::CMAP_INFERNO[0]);
                    }
                }
            }
        }
    }

    fn image_handle(&self) -> image::Handle {
        let img_buf = imageops::rotate270(&self.im).as_raw().to_vec();
        image::Handle::from_rgba(self.n_fft as u32, self.height, img_buf)
    }
}

fn get_stream_config(
    device: &cpal::Device,
    sample_rate: u32,
    frame_size: usize,
    direction: StreamDirection,
) -> Option<StreamConfig> {
    let mut configs = Vec::new();
    let all_configs = get_all_configs(device, direction);
    for c in all_configs.iter() {
        if c.channels() == 1 && c.sample_format() == SAMPLE_FORMAT {
            log::debug!("Found audio {} config: {:?}", direction, &c);
            configs.push(*c);
        }
    }
    // Further add multi-channel configs if no mono was found. The signal will be downmixed later.
    for c in all_configs.iter() {
        if c.channels() >= 2 && c.sample_format() == SAMPLE_FORMAT {
            log::debug!("Found audio source config: {:?}", &c);
            configs.push(*c);
        }
    }
    assert!(
        !configs.is_empty(),
        "No suitable audio {} config found.",
        direction
    );
    let sr = cpal::SampleRate(sample_rate);
    for c in configs.iter() {
        if sr >= c.min_sample_rate() && sr <= c.max_sample_rate() {
            let mut c: StreamConfig = (*c).with_sample_rate(sr).into();
            c.buffer_size = cpal::BufferSize::Fixed((frame_size * SAMPLE_SIZE) as u32);
            return Some(c);
        }
    }

    if let Some(c) = configs.first() {
        let mut c: StreamConfig = (*c).with_max_sample_rate().into();
        c.buffer_size = cpal::BufferSize::Fixed(
            frame_size as u32 * c.sample_rate.0 / sample_rate * SAMPLE_SIZE as u32,
        );

        log::warn!("Using best matching config {:?}", c);
        return Some(c);
    }
    None
}

#[allow(unused)]
#[derive(Clone, Copy)]
enum StreamDirection {
    Input,
    Output,
}

impl Display for StreamDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamDirection::Input => write!(f, "input"),
            StreamDirection::Output => write!(f, "output"),
        }
    }
}

fn get_all_configs(
    device: &cpal::Device,
    direction: StreamDirection,
) -> Vec<SupportedStreamConfigRange> {
    match direction {
        StreamDirection::Input => device
            .supported_input_configs()
            .expect("Failed to get input configs")
            .collect::<Vec<SupportedStreamConfigRange>>(),
        StreamDirection::Output => device
            .supported_output_configs()
            .expect("Failed to get output configs")
            .collect::<Vec<SupportedStreamConfigRange>>(),
    }
}

async fn set_save_file(samples: AudioSamples) -> Option<PathBuf> {
    let path_buf = samples.path().to_owned();
    let file_name = path_buf.file_name().unwrap();

    let file_handle = rfd::AsyncFileDialog::new()
        .set_file_name(file_name.to_str().unwrap())
        .set_directory(path_buf.parent().unwrap())
        .save_file()
        .await?;

    return Some(file_handle.path().to_path_buf());
}

async fn save_file(path: PathBuf, samples: AudioSamples, frame_size: usize) {
    let channels = samples.channels() as usize;
    let n_samples = samples.audio.len() / channels;

    let mut inframe = Array2::zeros((1, frame_size));
    let mut outframe = inframe.clone();
    let n_samples = (n_samples as f32 / frame_size as f32).ceil() as usize * frame_size;
    let mut out_audio = vec![0.0; n_samples];
    let mut index = 0;
    let mut df = unsafe { MODEL.clone().unwrap() };
    for frame in &samples
        .audio
        .chunks(channels)
        .map(|sample| sample.iter().sum::<f32>() / channels as f32)
        .chunks(frame_size)
    {
        inframe
            .as_slice_mut()
            .unwrap()
            .iter_mut()
            .zip(frame)
            .for_each(|(in_f, f)| *in_f = f);
        df.process(inframe.view(), outframe.view_mut()).unwrap();
        out_audio[index..index + frame_size].copy_from_slice(outframe.as_slice().unwrap());
        index += frame_size;
    }

    let out_audio = outframe.as_slice().unwrap();
    let buf_writer = BufWriter::new(fs::File::create(&path).unwrap());
    let out_samples = samples.with_path_audio_channel(path, out_audio, 1);
    out_samples.save(buf_writer)
}

//struct AudioImage {
//    im: RgbaImage,
//
//    im_height: u32,
//    im_width: u32,
//    frame_size: u32,
//
//    channels: u32,
//    n_channels: u32,
//}
//
//impl AudioImage {
//    fn new(n_width: u32, n_height: u32, channels: u32, n_channels: u32) -> Self {
//        Self {
//            im: RgbaImage::new(n_width, n_height),
//            im_width: n_width,
//            im_height: n_height,
//            frame_size: (n_height / 40).max(1),
//
//            channels,
//            n_channels,
//        }
//    }
//
//    fn w(&self) -> usize {
//        self.im_width as usize
//    }
//    fn h(&self) -> usize {
//        self.im_height as usize
//    }
//
//    fn update<I>(&mut self, audio: I, n_samples: usize)
//    where
//        I: Iterator<Item = f32>,
//    {
//        let sample_width = 100;
//        let binding = audio
//            .skip(self.channels as usize)
//            .step_by(self.n_channels as usize)
//            .take(n_samples)
//            .chunks(sample_width);
//
//        let audio_maxs = binding
//            .into_iter()
//            .map(|xs| (xs.max_by(f32::total_cmp).unwrap().abs() * self.im_width as f32) as u32);
//
//        let width_size = 1;
//
//        for (value, im_rows) in audio_maxs
//            .take(n_samples)
//            .zip(self.im.rows_mut().chunks(width_size).into_iter())
//        {
//            for row in im_rows {
//                for (i, x) in row.enumerate() {
//                    if i < value as usize {
//                        *x = Rgba(cmap::CMAP_INFERNO[100]);
//                    } else {
//                        *x = Rgba(cmap::CMAP_INFERNO[0]);
//                    }
//                }
//            }
//        }
//    }
//
//    fn image_handle(&self) -> image::Handle {
//        let imt_buf = imageops::rotate270(&self.im).as_raw().to_vec();
//        image::Handle::from_rgba(self.im_height, self.im_width, imt_buf)
//        //::from_pixels(self.n_frames, self.n_freqs, imt_buf)
//    }
//}

pub fn hann_window(length: usize) -> impl Iterator<Item = f32> {
    (0..length).map(move |v| 0.5 * (1.0 - (TAU * v as f32 / (length as f32 - 1.0)).cos()))
}

pub fn rect_window(length: usize) -> impl Iterator<Item = f32> {
    (0..length).map(|_| 1.0)
}

pub fn dbnorm(value: f32) -> f32 {
    (value.min(DB_MAX).max(DB_MIN) - DB_MIN) / (DB_MAX - DB_MIN)
}

pub fn amp2db(amplitude: f32) -> f32 {
    let abs_amplitude = amplitude.abs();

    // Define a very small positive number to act as a floor.
    // This prevents `log10(0)` which is -infinity and causes NaN or inf.
    const MIN_AMPLITUDE: f32 = f32::EPSILON; // Smallest positive non-zero f64

    let clamped_amplitude = abs_amplitude.max(MIN_AMPLITUDE);

    // Apply the decibel formula: 20 * log10(clamped_amplitude / reference_amplitude)
    // Since we assume normalized amplitude, reference_amplitude is 1.0.
    20.0 * clamped_amplitude.log10()
}

pub fn mean<I>(iter: I) -> f32
where
    I: IntoIterator<Item = f32>,
{
    let mut len = 0;

    iter.into_iter().fold(0.0, |a, b| {
        len += 1;
        a + b
    }) / len as f32
}

async fn pick_file() -> Result<PathBuf, Error> {
    Ok(rfd::AsyncFileDialog::new()
        .add_filter("*", &["mp3", "wav"])
        .pick_file()
        .await
        .ok_or(Error::NoFileSelected)?
        .path()
        .to_path_buf())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AudioVisualizer {
    FT,
    Spec,
}

impl ToString for AudioVisualizer {
    fn to_string(&self) -> String {
        match self {
            AudioVisualizer::FT => "FT View",
            AudioVisualizer::Spec => "Spec View",
        }
        .to_string()
    }
}

#[derive(Debug, Clone)]
enum Message {
    None,
    FileLoaded(Result<AudioSamples, Error>),
    LoadFile(PathBuf),
    Save,
    SaveFile(PathBuf),
    PickFile,
    Play,
    Pause,
    #[allow(unused)]
    MouseAudioView(Point),
    Tick,
    ProcessTick,
    AudioSinkMessage(Vec<AudioSinkMessage>),
    PickVisualizer(AudioVisualizer),
    //PickFilter(FilterType),
    AttenChange(f32),
    PostFilterBeta(f32),
    UpdateModel,
    ChangeTimestamp(f32),

    MouseAudioViewPress,
    MouseAudioViewRelease,
}
