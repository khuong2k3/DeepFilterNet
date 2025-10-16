use std::{
    fs::File,
    io::BufReader,
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
