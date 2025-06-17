use std::f32::consts::TAU;
use std::fmt::Display;
use std::future::Future;
use std::mem::MaybeUninit;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{BuildStreamError, Stream, StreamConfig, SupportedStreamConfigRange};
use crossbeam_channel::{bounded, unbounded};
use filter::{Filter, FilterType, LowPassFilter};
use iced::widget::{
    button, column, container, horizontal_space, image, mouse_area, pick_list, row, slider, text,
    tooltip, Image,
};
use iced::{theme, Length, Point, Subscription};
use iced::{Element, Task};
use image_rs::{imageops, Rgba, RgbaImage};
use itertools::Itertools;
use rustfft::num_complex::{Complex32, ComplexFloat};
use rustfft::Fft;

use ringbuf::{producer::PostponedProducer, Consumer, SharedRb};

use df::realtime::{DfControl, ProcessSetting, RealTimeProcess};

pub type RbProd = PostponedProducer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type RbCons = Consumer<f32, Arc<SharedRb<f32, Vec<MaybeUninit<f32>>>>>;
pub type SendLsnr = Sender<f32>;
pub type RecvLsnr = Receiver<f32>;
pub type SendSpec = Sender<Box<[f32]>>;
pub type RecvSpec = Receiver<Box<[f32]>>;
//pub type SendControl = Sender<(DfControl, f32)>;
//pub type RecvControl = Receiver<(DfControl, f32)>;

type Receiver<T> = crossbeam_channel::Receiver<T>;
type Sender<T> = crossbeam_channel::Sender<T>;
const DB_MIN: f32 = -90.0;
const DB_MAX: f32 = 10.0;

type SampleType = Arc<[f32]>;
const SAMPLE_FORMAT: cpal::SampleFormat = cpal::SampleFormat::F32;
const SAMPLE_SIZE: usize = size_of::<f32>();

const N_FFT: usize = 480;

mod cmap;
mod filter;

fn main() -> Result<(), iced::Error> {
    env_logger::init();
    log::set_max_level(log::LevelFilter::Info);

    iced::application("A cool counter", AudioEdit::update, AudioEdit::view)
        //.font(include_bytes!("../font/file-font.ttf").as_slice())
        .subscription(AudioEdit::subscription)
        .theme(AudioEdit::theme)
        .run_with(AudioEdit::new)
}

#[derive(Clone, Copy, Debug)]
enum Error {
    FileLoadError,
    NoFileSelected,
    SaveFileError,
}

#[derive(Debug, Clone)]
enum Message {
    None,
    FileLoaded(Result<(AudioSamples, PathBuf), Error>),
    LoadFile(PathBuf),
    PickFile,
    SaveFile(PathBuf),
    PickSaveFile,
    Play,
    Pause,
    MouseAudioView(Point),
    Tick,
    ProcessTick(Vec<Box<[f32]>>),
    AudioSinkMessage(Vec<AudioSinkMessage>),
    PickFilter(FilterType),
    ChangeTimestamp(f32),
    AttenChange(f32),

    MouseAudioViewPress,
    MouseAudioViewRelease,
}

enum AudioSinkCtrl {
    ChangeTimesamp(usize),
}

#[derive(Clone, Debug)]
struct AudioSamples {
    audio: SampleType,
    sr: u32,
    channels: u16,
    duration: u32,
}

struct ModelInfo {
    atten_db: f32,
}

#[derive(Default)]
struct AudioViewCtrl {
    on_change_freq: bool,
}

struct AudioEdit {
    //samples: Option<AudioSamples>,
    path: Option<PathBuf>,
    au_sink: AudioSink,
    audio_ctl: Option<Sender<AudioSinkCtrl>>,
    fft: FftProcess,
    audio_ft: FtAudio,
    spec_noise: SpecImage,
    spec_enh: SpecImage,
    ft_image: image::Handle,
    im_spec_enh: image::Handle,
    im_spec_noise: image::Handle,
    model_info: ModelInfo,
    timestamp: usize,
    audio_view_ctrl: AudioViewCtrl,
}

impl Default for ModelInfo {
    fn default() -> Self {
        Self { atten_db: 30.0 }
    }
}

impl Default for AudioEdit {
    fn default() -> Self {
        let audioft = FtAudio::new(300, 512, N_FFT);
        let spec_noise = SpecImage::new(300, N_FFT as u32 / 2 + 1, DB_MIN, DB_MAX);
        let spec_enh = SpecImage::new(300, N_FFT as u32 / 2 + 1, DB_MIN, DB_MAX);
        let au_sink = AudioSink::new().unwrap();
        let fft = FftProcess::new(N_FFT);

        Self {
            au_sink,
            ft_image: audioft.image_handle(),
            im_spec_noise: spec_noise.image_handle(),
            im_spec_enh: spec_enh.image_handle(),
            audio_ft: audioft,
            spec_enh,
            audio_ctl: None,
            spec_noise,
            fft,
            path: None,
            model_info: Default::default(),
            timestamp: 0,
            audio_view_ctrl: Default::default(),
        }
    }
}

impl AudioEdit {
    fn new() -> (Self, Task<Message>) {
        //clean_freesound_33711.wav;
        //output_sine_quiet.wav
        (
            Self::default(),
            Task::none(),
            //Task::batch([Task::perform(
            //    load_audio_file(format!(
            //        "{}/output_sine_quiet.wav",
            //        env!("CARGO_MANIFEST_DIR")
            //    )),
            //    Message::FileLoaded,
            //)]),
        )
    }

    fn theme(&self) -> theme::Theme {
        iced::Theme::Dark
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::LoadFile(path) => Task::perform(load_audio_file(path), Message::FileLoaded),
            Message::FileLoaded(samples) => {
                match samples {
                    Ok((samples, path)) => {
                        self.audio_ctl = Some(self.au_sink.load(samples.clone()).unwrap());
                        self.path = Some(path);
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
            Message::PickSaveFile => {
                let save_async = save_file();

                Task::perform(save_async, |path| {
                    path.map(Message::SaveFile).unwrap_or(Message::None)
                })
            }
            Message::SaveFile(path) => {
                if let Some(samples) = self.au_sink.process_samples() {
                    samples.write_to_file(path);
                }
                Task::none()
            }
            // audio message
            Message::AudioSinkMessage(messages) => {
                for message in messages {
                    match message {
                        AudioSinkMessage::FileEnded => {
                            log::info!("Audio stream ended");
                            self.au_sink.pause().unwrap();
                            //self.au_sink.destroy_stream().unwrap();
                        }
                        AudioSinkMessage::Duration(timestamp) => {
                            self.timestamp = timestamp;
                        }
                    }
                }
                Task::none()
            }
            Message::PickFilter(filter) => {
                self.au_sink.set_filter(filter);
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
                self.au_sink.pause().unwrap();
                Task::none()
            }
            Message::Play => {
                self.au_sink.play().unwrap();
                Task::none()
            }
            // real time message
            Message::Tick => {
                let mut tasks = vec![];
                let samples = self.samples();
                tasks.push(Task::perform(samples, Message::ProcessTick));
                if let Some(messages) = self.au_sink.audio_message() {
                    tasks.push(Task::perform(messages, Message::AudioSinkMessage));
                }

                Task::batch(tasks)
            }
            Message::ProcessTick(samples) => {
                if samples.len() > 0 {
                    let samples_fft = self.samples2ffts(samples.iter());
                    self.update_ft(samples_fft.last().unwrap().iter().cloned());
                    self.update_spec_enh();
                    self.update_spec_noise();
                }

                Task::none()
            }
            // event message
            Message::MouseAudioViewPress => {
                self.audio_view_ctrl.on_change_freq = true;
                Task::none()
            }
            Message::MouseAudioView(p) => {
                if let (Some(configs), true) =
                    (&self.au_sink.configs, self.audio_view_ctrl.on_change_freq)
                {
                    let freq_bin = p.x.min(self.audio_ft.w() as f32) / self.audio_ft.w() as f32;
                    let sr = configs.sample_rate.0 as f32;
                    let freq = freq_bin * sr;

                    self.au_sink.update_cutoff(freq);
                }

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
            Message::None => Task::none(),
        }
    }

    fn view(&self) -> Element<'_, Message> {
        let (duration, sr, have_samples) =
            if let Some(samples) = &self.au_sink.samples.lock().unwrap().as_ref() {
                (samples.audio.len() as f32, samples.sr as f32, true)
            } else {
                (0.0, 1.0, false)
            };

        let pick_file = button("Pick file").on_press(Message::PickFile);
        let save_file = button("Save")
            .style(if have_samples {
                button::primary
            } else {
                button::secondary
            })
            .on_press(if have_samples {
                Message::PickSaveFile
            } else {
                Message::None
            });

        let control = row![horizontal_space().width(Length::Fill), save_file, pick_file]
            .spacing(10)
            .width(Length::Fill)
            .padding([10, 5]);

        let ft_handle = Image::new(self.ft_image.clone())
            .width(self.audio_ft.w() as f32)
            .height(self.audio_ft.h() as f32);

        let timestamp = self.timestamp as f32;

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

        let space = horizontal_space().width(Length::Fill);

        let mut audio_view_row = row![space, audio_view].spacing(10);

        if have_samples {
            let filter_selector = pick_list(
                [FilterType::Lowpass],
                None::<FilterType>,
                Message::PickFilter,
            );

            let mut filter_row: Vec<Element<'_, Message>> = vec![];
            {
                if let Ok(filter) = self.au_sink.filters.lock() {
                    if filter.lowpass.is_some() {
                        filter_row.push(text!("{}", FilterType::Lowpass).into());
                    }
                }
            }

            let filter_row = row(filter_row);
            let filter_selector_ctrl = column![filter_selector, filter_row];
            audio_view_row = audio_view_row.push(filter_selector_ctrl);
        }

        let play = button("play").on_press(Message::Play);
        let pause = button("pause").on_press(Message::Pause);
        let play_ctrl = row![
            horizontal_space().width(Length::Fill),
            play,
            pause,
            horizontal_space().width(Length::Fill),
        ]
        .spacing(5);

        let noise_handle = Image::new(self.im_spec_noise.clone())
            .width(self.spec_noise.w() as f32)
            .height(self.spec_noise.h() as f32);

        let enh_handle = Image::new(self.im_spec_enh.clone())
            .width(self.spec_enh.w() as f32)
            .height(self.spec_enh.h() as f32);

        let spec_view = row![container(noise_handle), container(enh_handle),].spacing(10);
        //let spec_view = row![container(noise_handle), container(enh_handle),].spacing(10);

        column![
            control,
            audio_view_row,
            timestemp_slider,
            play_ctrl,
            spec_view
        ]
        .spacing(10)
        .padding([0, 10])
        .into()
    }

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

    //fn update_spec<'a, I>(&mut self, fft_samples: I, n_sample: usize)
    //where
    //    I: Iterator<Item = &'a Box<[f32]>>,
    //{
    //    self.spec.update(fft_samples, n_sample);
    //    self.spec_image = self.spec.image_handle();
    //}

    fn update_spec_noise(&mut self) {
        let len = self.au_sink.r_noise.len();
        let specs = self.au_sink.r_noise.iter().take(len);

        self.spec_noise.update(specs, len);
        self.im_spec_noise = self.spec_noise.image_handle();
    }

    fn update_spec_enh(&mut self) {
        let len = self.au_sink.r_enh.len();
        let specs = self.au_sink.r_enh.iter().take(len);

        self.spec_enh.update(specs, len);
        self.im_spec_enh = self.spec_enh.image_handle();
    }

    fn samples(&self) -> impl Future<Output = Vec<Box<[f32]>>> {
        let samples = self.au_sink.samples();

        async move { samples.await }
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(std::time::Duration::from_millis(20)).map(|_| Message::Tick)
    }
}

async fn load_audio_file(path: impl AsRef<Path>) -> Result<(AudioSamples, PathBuf), Error> {
    let pathbuf = path.as_ref().to_owned();
    log::info!("Load file {:?}", pathbuf);

    let mut reader = hound::WavReader::open(path).map_err(|_| Error::FileLoadError)?;
    let spec = reader.spec();
    log::info!("Audio file loaded");
    let sr = spec.sample_rate;
    let channels = spec.channels;

    let samples = match spec.sample_format {
        hound::SampleFormat::Int => reader
            .samples::<i16>()
            .flatten()
            .map(|s| s as f32 / i16::MAX as f32)
            .collect::<SampleType>(),
        hound::SampleFormat::Float => reader.samples::<f32>().flatten().collect::<SampleType>(),
    };

    Ok((
        AudioSamples {
            sr,
            channels,
            audio: samples,
            duration: reader.duration(),
        },
        pathbuf,
    ))
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

    fn update<I>(&mut self, specs: I, mut n_specs: usize)
    where
        I: Iterator<Item = Box<[f32]>>,
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
}

#[derive(Default)]
struct Filters {
    lowpass: Option<LowPassFilter>,
}

struct AudioSink {
    host: cpal::Host,
    device: cpal::Device,
    stream: Option<Stream>,
    configs: Option<StreamConfig>,
    in_pod: Sender<Box<[f32]>>,
    out_cons: Receiver<Box<[f32]>>,
    filters: Arc<Mutex<Filters>>,

    samples: Arc<Mutex<Option<AudioSamples>>>,
    proc: RealTimeProcess,
    proc_producer: Arc<Mutex<(RbProd, RbCons)>>,
    proc_ctrl: Sender<(DfControl, f32)>,

    s_mess: Sender<AudioSinkMessage>,
    r_mess: Receiver<AudioSinkMessage>,
    r_noise: Receiver<Box<[f32]>>,
    r_enh: Receiver<Box<[f32]>>,
}

impl AudioSink {
    fn new() -> Option<Self> {
        let host = cpal::default_host();
        let device = host.default_output_device()?;
        //let heap = HeapRb::<S>::new(get_frame_size() * 100);
        //let (in_pod, out_cons) = heap.split();
        let (in_pod, out_cons) = unbounded();
        let filters = Arc::new(Mutex::new(Default::default()));

        let (s_mess, r_mess) = bounded::<AudioSinkMessage>(3);
        let (proc_ctrl, proc_rev) = unbounded();
        let (s_noise, r_noise) = unbounded();
        let (s_enh, r_enh) = unbounded();

        let (proc, mut inpod, outpod) = RealTimeProcess::init_from_targz(
            include_bytes!("../../models/DeepFilterNet3_ll_onnx.tar.gz"),
            ProcessSetting {
                input_sr: 44100,
                output_sr: 44100,
                s_lsnr: None,
                s_spec: Some((s_noise, s_enh)),
                r_opt: Some(proc_rev),
                //s_lsnr: Option<Sender<f32>>,
                //s_spec: Option<(Sender<Box<[f32]>>, Sender<Box<[f32]>>)>,
                //r_opt: Option<Receiver<(DfControl, f32)>>,
            },
        )
        .unwrap();
        {
            let padding = vec![0.0; 2 * proc.frame_size()];
            let mut i = 0;
            while i < padding.len() {
                i += inpod.push_slice(&padding[i..]);
                inpod.sync();
            }
        }

        let proc_producer = Arc::new(Mutex::new((inpod, outpod)));

        Some(Self {
            host,
            device,
            stream: None,
            configs: None,
            in_pod,
            out_cons,
            s_mess,
            r_mess,
            r_noise,
            r_enh,
            filters,
            samples: Arc::new(Mutex::new(None)),
            proc_ctrl,

            proc_producer,
            proc,
        })
    }

    fn samples(&self) -> impl Future<Output = Vec<Box<[f32]>>> {
        let out_cons = self.out_cons.clone();

        async move {
            let n_samples = out_cons.len();
            out_cons.iter().take(n_samples).collect::<Vec<_>>()
        }
    }

    fn update_cutoff(&mut self, fc: f32) {
        if let Some(filter) = self.filters.lock().unwrap().lowpass.as_mut() {
            filter.update_cutoff(fc);
        }
    }

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

    fn set_filter(&self, filter_type: FilterType) {
        if let Some(samples) = self.samples.lock().unwrap().as_ref() {
            match filter_type {
                FilterType::Lowpass => {
                    self.filters.lock().unwrap().lowpass = Some(LowPassFilter::new(
                        1000.0,
                        samples.sr as f32,
                        samples.channels as usize,
                    ));
                }
            }
        }
    }

    fn process_samples(&self) -> Option<AudioSamples> {
        let mut proc_guard = self.proc_producer.lock().unwrap();
        let sample_guard = self.samples.lock().unwrap();
        let Some(samples) = sample_guard.as_ref() else {
            return None;
        };
        self.proc_ctrl.send((DfControl::InputSr, samples.sr as f32)).unwrap();
        self.proc_ctrl.send((DfControl::OutputSr, samples.sr as f32)).unwrap();

        let input_audio: Vec<f32> = samples
            .audio
            .chunks(samples.channels as usize)
            .map(|s| mean(s.iter().cloned()))
            .collect();

        let mut output_audio = vec![0.0; input_audio.len()];
        let frame_size = self.proc.frame_size();

        for (input, output) in
            input_audio.chunks(frame_size).zip(output_audio.chunks_mut(frame_size))
        {
            let mut i = 0;
            while i < frame_size {
                i += proc_guard.0.push_slice(&input[i..]);
            }
            proc_guard.0.sync();

            i = 0;
            while i < frame_size {
                i += proc_guard.1.pop_slice(&mut output[i..]);
            }
        }

        let mut out_audio = vec![0.0; output_audio.len() * samples.channels as usize];
        for i in 0..output_audio.len() {
            out_audio[2 * i] = output_audio[i];
            out_audio[2 * i + 1] = output_audio[i];
        }

        Some(AudioSamples {
            audio: out_audio.into(),
            sr: samples.sr,
            channels: samples.channels,
            duration: samples.duration,
        })
    }

    fn load(&mut self, samples: AudioSamples) -> Result<Sender<AudioSinkCtrl>, BuildStreamError> {
        let sr = samples.sr;
        let mut inner_samples = self.samples.lock().unwrap();
        let input_channels = samples.channels;
        *inner_samples = Some(samples);

        let samples = inner_samples.as_ref().unwrap();

        let settings = get_stream_config(
            &self.device,
            sr as u32,
            samples.channels,
            StreamDirection::Output,
        )
        .expect("No suitable audio output config found.");
        let channels = samples.channels;

        self.configs = Some(settings.clone());
        let mut current_frame = 0;

        let in_pod = self.in_pod.clone();

        let lowpass_filter = self.filters.clone();
        let s_mess = self.s_mess.clone();

        let (audio_ctl, audio_rev) = unbounded();

        let samples = self.samples.clone();

        let proc = self.proc_producer.clone();

        self.proc_ctrl.send((DfControl::InputSr, sr as f32)).unwrap();
        self.proc_ctrl.send((DfControl::OutputSr, sr as f32)).unwrap();

        let output_channels = settings.channels;

        if output_channels != input_channels {
            log::info!("Have diffenct channels size");
        }

        let stream = self.device.build_output_stream::<f32, _, _>(
            &settings,
            move |output, _info| {
                let n_ctl_mess = audio_rev.len();
                let samples_audio = if let Some(samples) = samples.lock().unwrap().as_ref() {
                    samples.clone()
                } else {
                    return;
                };

                let samples = samples_audio.audio;

                if n_ctl_mess > 1 {
                    for message in audio_rev.iter().take(n_ctl_mess) {
                        match message {
                            AudioSinkCtrl::ChangeTimesamp(timestamp) => {
                                current_frame = timestamp * channels as usize;
                                //println!("current_frame: {}", current_frame);
                            }
                        }
                    }
                }

                let mut samplers_iter = samples[current_frame..].iter();

                if current_frame >= samples.len() {
                    log::info!("Stream ended");
                    s_mess.send(AudioSinkMessage::FileEnded).unwrap();
                }

                s_mess
                    .send(AudioSinkMessage::Duration(
                        current_frame / channels as usize,
                    ))
                    .unwrap();

                let mut rev_au = vec![0.0; output.len() / output_channels as usize].into_boxed_slice();
                let mut filter_guard = lowpass_filter.lock().unwrap();

                for (i, frame) in output.chunks_mut(output_channels as usize).enumerate() {
                    let mut sample = 0.0;
                    for i in 0..output_channels as usize {
                        if input_channels != 1 {
                            if let Some(&sample_n) = samplers_iter.next() {
                                sample = sample_n;
                                current_frame += 1;
                            } else {
                                //If we've reached the end of the WAV file, fill the rest of the buffer with silence (0.0).
                                sample = 0.0;
                            }
                        } else if i == 0 {
                            if let Some(&samples_n) = samplers_iter.next() {
                                sample = samples_n;
                                current_frame += 1;
                            } else {
                                sample = 0.0;
                            }
                        }

                        frame[i] = sample;

                        if let Some(filter) = filter_guard.lowpass.as_mut() {
                            frame[i] = filter.process(frame[i], i);
                        }
                    }

                    rev_au[i] = mean(frame.iter().cloned());
                }

                let mut proc_guard = proc.lock().unwrap();
                let (inpod, outpod) = proc_guard.deref_mut();

                let mut i = 0;
                while i < rev_au.len() {
                    i += inpod.push_slice(&rev_au[i..]);
                }
                inpod.sync();

                i = 0;
                let mut filtered = vec![0.0; rev_au.len()];
                while i < rev_au.len() {
                    i += outpod.pop_slice(&mut filtered[i..]);
                }

                let mut samplers_iter = filtered.iter();
                for frame in output.chunks_mut(channels as usize) {
                    if let Some(&sample) = samplers_iter.next() {
                        for c in frame {
                            *c = sample;
                        }
                    }
                }

                if filtered.len() < get_frame_size() {
                    filtered = filtered.into_iter().pad_using(N_FFT, |_| 0.0).collect();
                }

                in_pod.send(filtered.into()).unwrap();
            },
            |_| {},
            None,
        )?;

        log::info!("Audio stream created");

        self.stream = Some(stream);

        Ok(audio_ctl)
    }

    //fn destroy_stream(&mut self) -> Result<(), cpal::PauseStreamError> {
    //    self.pause()?;
    //    self.stream.take();
    //
    //    Ok(())
    //}

    fn play(&self) -> Result<(), cpal::PlayStreamError> {
        if let Some(stream) = &self.stream {
            log::info!("Play audio");
            stream.play()?;
        }
        Ok(())
    }

    fn pause(&self) -> Result<(), cpal::PauseStreamError> {
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
        let bar_width = 2 * (height / n_fft as u32).max(1);
        Self {
            im: RgbaImage::new(width, height),
            n_fft,
            width,
            height,
            bar_width,
        }
    }

    fn w(&self) -> u32 {
        self.height
    }
    fn h(&self) -> u32 {
        self.width
    }

    fn update<I>(&mut self, fft_db: I)
    where
        I: Iterator<Item = f32>,
    {
        let bar_height = self.bar_width as usize;

        let visual_db = fft_db
            .take(self.n_fft / 2 + 1)
            .map(|value| dbnorm(value) * self.width as f32 * 0.9)
            .map(|value| value as usize);

        for (vi_db, im_rows) in visual_db.zip(self.im.rows_mut().chunks(bar_height).into_iter()) {
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
        let imt_buf = imageops::rotate270(&self.im).as_raw().to_vec();
        image::Handle::from_rgba(self.height, self.width, imt_buf)
        //::from_pixels(self.n_frames, self.n_freqs, imt_buf)
    }
}

fn get_frame_size() -> usize {
    N_FFT
}

fn get_stream_config(
    device: &cpal::Device,
    sample_rate: u32,
    n_channels: u16,
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
            c.buffer_size = cpal::BufferSize::Fixed((get_frame_size() * SAMPLE_SIZE) as u32);
            return Some(c);
        }
    }

    if let Some(c) = configs.first() {
        let mut c: StreamConfig = (*c).with_max_sample_rate().into();
        c.buffer_size = cpal::BufferSize::Fixed(
            get_frame_size() as u32 * c.sample_rate.0 / sample_rate * SAMPLE_SIZE as u32,
        );

        log::warn!("Using best matching config {:?}", c);
        return Some(c);
    }
    None
}

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
        .pick_file()
        .await
        .ok_or(Error::NoFileSelected)?
        .path()
        .to_path_buf())
}

async fn save_file() -> Result<PathBuf, Error> {
    Ok(rfd::AsyncFileDialog::new()
        .save_file()
        .await
        .ok_or(Error::SaveFileError)?
        .path()
        .to_path_buf())
}

impl AudioSamples {
    fn write_to_file(&self, path: impl AsRef<Path>) {
        let spec = hound::WavSpec {
            channels: self.channels,
            sample_rate: self.sr,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };

        let mut writer = hound::WavWriter::create(path, spec).unwrap();
        for s in self.audio.iter() {
            writer.write_sample((s * i16::MAX as f32) as i16).unwrap();
        }

        writer.finalize().unwrap();
    }
}
