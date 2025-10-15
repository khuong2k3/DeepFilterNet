use std::{f32::consts::PI, fmt::{Display, Write}};


#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilterType {
    Lowpass,
}

impl Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterType::Lowpass => f.write_str("lowpass"),
        }
    }
}

pub trait Filter {
    fn process(&mut self, sample: f32, channel_idx: usize) -> f32;
}


#[derive(Debug)] // Derive Debug for easy printing/logging
pub struct LowPassFilter {
    alpha: f32,                     // Filter coefficient
    previous_outputs: Vec<f32>,     // Stores y[n-1] for each channel
    sample_rate: f32,               // Audio sample rate
    num_channels: usize,            // Number of audio channels
}

impl LowPassFilter {
    /// Creates a new LowPassFilter instance.
    ///
    /// Arguments:
    /// * `initial_cutoff_frequency`: The initial cutoff frequency in Hz.
    /// * `sample_rate`: The audio system's sample rate in Hz.
    /// * `num_channels`: The number of audio channels (e.g., 1 for mono, 2 for stereo).
    pub fn new(initial_cutoff_frequency: f32, sample_rate: f32, num_channels: usize) -> Self {
        let mut filter = LowPassFilter {
            alpha: 0.5, // Will be calculated in update_cutoff
            previous_outputs: vec![0.0; num_channels], // Initialize with silence
            sample_rate,
            num_channels,
        };
        filter.update_cutoff(initial_cutoff_frequency); // Set initial alpha
        filter
    }

    /// Updates the filter's cutoff frequency and recalculates the alpha coefficient.
    ///
    /// Arguments:
    /// * `new_fc`: The new cutoff frequency in Hz.
    pub fn update_cutoff(&mut self, new_fc: f32) {
        // Clamp cutoff frequency to a reasonable range to prevent numerical issues
        let fc = new_fc.clamp(1.0, self.sample_rate as f32 / 2.0 - 100.0) as f32; // Max fc is Nyquist - buffer
        let dt = 1.0 / self.sample_rate;
        let rc = 1.0 / (2.0 * PI * fc); // RC time constant
        // Alpha calculation for a first-order low-pass filter
        self.alpha = dt / (rc + dt);
        log::info!("Filter cutoff updated to {:.2} Hz, alpha: {:.6}", new_fc, self.alpha);
    }
}

impl Filter for LowPassFilter {

    /// Processes a single audio sample for a specific channel.
    /// Applies the first-order low-pass filter.
    ///
    /// Arguments:
    /// * `sample`: The current input sample (f32, typically normalized to [-1.0, 1.0]).
    /// * `channel_idx`: The index of the channel (0 for left, 1 for right, etc.).
    ///
    /// Returns:
    /// * The filtered output sample as an `f32`.
    fn process(&mut self, input_sample: f32, channel_idx: usize) -> f32 {
        if channel_idx >= self.num_channels {
            log::error!("Attempted to process sample for invalid channel_idx: {}", channel_idx);
            return input_sample; // Return original sample if channel index is out of bounds
        }

        // Apply the filter formula: y[n] = alpha * x[n] + (1 - alpha) * y[n-1]
        let filtered_sample = (self.alpha as f32 * input_sample) +
                              ((1.0 - self.alpha) as f32 * self.previous_outputs[channel_idx]);

        // Store the current output as the previous output for the next iteration.
        self.previous_outputs[channel_idx] = filtered_sample;

        filtered_sample
    }
    
}
