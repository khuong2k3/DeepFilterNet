use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::anyhow;
use rodio::Source;

type SampleType = Arc<[f32]>;

#[derive(Clone, Debug)]
pub struct AudioSamples {
    path: PathBuf,
    pub audio: SampleType,
    sr: u32,
    channels: u16,
    duration: u32,
}

//pub enum AudioReader {
//    Wav(hound::WavReader<BufReader<File>>),
//    Mp3(rodio::)
//}

impl AudioSamples {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let extention = path.as_ref().extension().ok_or(anyhow!("File not found"))?;
        if extention == "wav" {
            load_wav_file(path)
        } else if extention == "mp3" {
            load_mp3_file(path)
        } else {
            Err(anyhow!("File not found"))
        }
    }

    pub fn with_path_audio_channel(&self, path: impl AsRef<Path>, audio: Arc<[f32]>, channels: u16) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            sr: self.sr,
            channels,
            duration: self.duration,
            audio,
        }
    }

    pub fn save<T: std::io::Write + std::io::Seek>(&self, mut buf_writer: BufWriter<T>) {
        let extention = self.path.extension().unwrap();
        if extention ==  "wav" {
            let mut writer = hound::WavWriter::new(buf_writer, hound::WavSpec { 
                channels: self.channels, 
                sample_rate: self.sr, 
                bits_per_sample: 16, 
                sample_format: hound::SampleFormat::Int
            }).unwrap();
            let mut writer = writer.get_i16_writer(self.audio.len() as u32);
            for sample in self.audio.iter() {
                writer.write_sample((sample * i16::MAX as f32) as i16);
            }
        } else if extention == "mp3" {
            let mut writer = lame::Lame::new().unwrap();
            writer.set_channels(self.channels as u8).unwrap();
            writer.set_sample_rate(self.sr).unwrap();
            writer.init_params().unwrap();

            let sample_size = self.audio.len() / self.channels as usize;
            let mut pcm_left = Vec::with_capacity(sample_size);
            let mut pcm_right = Vec::with_capacity(sample_size);

            for sample in self.audio.chunks(self.channels as usize) {
                let sample = sample.iter().sum::<f32>() / sample.len() as f32;
                let sample = (sample * i16::MAX as f32) as i16;
                pcm_left.push(sample);
                pcm_right.push(sample);
            }
            let mp3_buffer_size = (self.audio.len() as f32 * 1.25) as usize + 7200;
            let mut mp3_buffer = vec![0; mp3_buffer_size];

            let bytes_written = writer.encode(&pcm_left, &pcm_right, &mut mp3_buffer).unwrap();
            buf_writer.write_all(&mp3_buffer[..bytes_written]).unwrap();
        }
    }

    //pub fn frame(&self, start: usize) -> &[f32] {
    //    &self.audio[start..]
    //}

    pub fn sr(&self) -> u32 {
        self.sr
    }

    pub fn duration(&self) -> u32 {
        self.duration
    }
    pub fn channels(&self) -> u16 {
        self.channels
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn load_wav_file(path: impl AsRef<Path>) -> anyhow::Result<AudioSamples> {
    let pathbuf = path.as_ref().to_owned();
    log::info!("Load file {:?}", pathbuf);

    let mut reader = hound::WavReader::open(path)?;
    let spec = reader.spec();
    log::info!("Audio file loaded");
    let sr = spec.sample_rate;
    let channels = spec.channels;

    let samples = match spec.sample_format {
        hound::SampleFormat::Int => reader
            .samples::<i16>()
            .flatten()
            .map(|s| s as f32 / i16::MAX as f32)
            .collect::<Arc<[f32]>>(),
        hound::SampleFormat::Float => reader.samples::<f32>().flatten().collect::<Arc<[f32]>>(),
    };

    Ok(AudioSamples {
        path: pathbuf,
        sr,
        channels,
        audio: samples,
        duration: reader.duration() / channels as u32,
    })
}

fn load_mp3_file(path: impl AsRef<Path>) -> anyhow::Result<AudioSamples> {
    let path_buf = path.as_ref().to_path_buf();
    let file = BufReader::new(File::open(path)?);
    let source = rodio::Decoder::new(file)?;
    let sr = source.sample_rate();
    let channels = source.channels();
    let samples: Arc<[f32]> = source.collect();
    let duration = samples.len() as u32 / channels as u32;

    Ok(AudioSamples {
        path: path_buf,
        audio: samples,
        sr,
        channels,
        duration,
    })
}
