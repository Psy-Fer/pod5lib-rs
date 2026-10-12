use std::collections::HashMap;
use std::sync::Arc;

use crate::Result;
use crate::signal::SignalTable;

// ── UUID helpers ──────────────────────────────────────────────────────────────

/// Format 16 raw bytes into the canonical `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` UUID string.
///
/// POD5 stores read IDs as `FixedSizeBinary(16)` in standard RFC 4122 big-endian byte order,
/// matching `uuid.UUID(bytes=data)` in Python.
pub fn uuid_to_str(b: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15],
    )
}

// ── Pore ──────────────────────────────────────────────────────────────────────

/// Physical pore location on the flow cell.
#[derive(Debug, Clone, PartialEq)]
pub struct Pore {
    /// 1-indexed channel number.
    ///
    /// Read from `channel_32bit` when present (Reads table V7+), so channels above
    /// 65535 are preserved.
    pub channel: u32,
    /// 1-indexed well number.
    pub well: u8,
    /// Pore type identifier string (e.g. `"pore_type_not_set"`).
    pub pore_type: String,
}

// ── Calibration ───────────────────────────────────────────────────────────────

/// ADC → picoamp calibration parameters.
///
/// `pA = (adc + offset) * scale`
///
/// The stored `offset` is typically negative (the negated ADC zero-point),
/// matching the Python pod5 library formula `(signal + calibration.offset) * calibration.scale`.
#[derive(Debug, Clone, PartialEq)]
pub struct Calibration {
    /// ADC zero-point offset added before scaling; typically negative.
    pub offset: f32,
    /// Volts-per-ADC-count scale factor (`adc_range / digitisation`).
    pub scale: f32,
}

impl Calibration {
    /// Build from the `adc_range` and `digitisation` values stored in RunInfo.
    pub fn from_range(offset: f32, adc_range: f32, digitisation: f32) -> Self {
        Self {
            offset,
            scale: adc_range / digitisation,
        }
    }

    /// Convert a slice of raw ADC values to picoamps.
    pub fn to_pa(&self, adc: &[i16]) -> Vec<f32> {
        adc.iter()
            .map(|&s| (s as f32 + self.offset) * self.scale)
            .collect()
    }
}

// ── EndReason ─────────────────────────────────────────────────────────────────

/// Why a read was terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReasonKind {
    /// Reason not known or not recorded.
    Unknown,
    /// Multiplexer (mux) was switched to a different channel.
    MuxChange,
    /// Mux change triggered by an unblock command.
    UnblockMuxChange,
    /// Mux change triggered by the data-service unblock path.
    DataServiceUnblockMuxChange,
    /// Read ended because signal went positive (above threshold).
    SignalPositive,
    /// Read ended because signal went negative (below threshold).
    SignalNegative,
    /// Read terminated on explicit API request (e.g. `stop_receiving`).
    ApiRequest,
    /// Read terminated due to a device data error.
    DeviceDataError,
    /// Read terminated because the analysis configuration changed.
    AnalysisConfigChange,
    /// Sequencing was paused.
    Paused,
    /// Unrecognised end-reason string; the raw discriminant is preserved.
    Other(u8),
}

impl EndReasonKind {
    /// Parse the lowercase snake_case reason string stored in the POD5 dictionary column.
    pub fn from_reason_str(s: &str) -> Self {
        match s {
            "unknown" => Self::Unknown,
            "mux_change" => Self::MuxChange,
            "unblock_mux_change" => Self::UnblockMuxChange,
            "data_service_unblock_mux_change" => Self::DataServiceUnblockMuxChange,
            "signal_positive" => Self::SignalPositive,
            "signal_negative" => Self::SignalNegative,
            "api_request" => Self::ApiRequest,
            "device_data_error" => Self::DeviceDataError,
            "analysis_config_change" => Self::AnalysisConfigChange,
            "paused" => Self::Paused,
            _ => Self::Other(0),
        }
    }

    /// Return the canonical snake_case string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::MuxChange => "mux_change",
            Self::UnblockMuxChange => "unblock_mux_change",
            Self::DataServiceUnblockMuxChange => "data_service_unblock_mux_change",
            Self::SignalPositive => "signal_positive",
            Self::SignalNegative => "signal_negative",
            Self::ApiRequest => "api_request",
            Self::DeviceDataError => "device_data_error",
            Self::AnalysisConfigChange => "analysis_config_change",
            Self::Paused => "paused",
            Self::Other(_) => "other",
        }
    }
}

/// How and why a read ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndReason {
    /// Categorised termination reason.
    pub reason: EndReasonKind,
    /// Whether the read termination was externally forced (e.g. unblock).
    pub forced: bool,
}

// ── ScaleShift ────────────────────────────────────────────────────────────────

/// Optional (shift, scale) pair; `f32::NAN` when unavailable.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShiftScalePair {
    /// Shift component; `f32::NAN` if this pair was not recorded.
    pub shift: f32,
    /// Scale component; `f32::NAN` if this pair was not recorded.
    pub scale: f32,
}

impl ShiftScalePair {
    /// Sentinel value used when tracking or predicted scaling is absent.
    pub const UNAVAILABLE: Self = Self {
        shift: f32::NAN,
        scale: f32::NAN,
    };

    /// Returns `true` if both components are finite (i.e. the pair was recorded).
    pub fn is_available(&self) -> bool {
        self.shift.is_finite() && self.scale.is_finite()
    }
}

// ── RunInfo ───────────────────────────────────────────────────────────────────

/// Per-experiment run context (one entry per MinKNOW acquisition).
#[derive(Debug, Clone)]
pub struct RunInfo {
    /// Unique identifier for the MinKNOW acquisition (hex UUID string).
    pub acquisition_id: String,
    /// Milliseconds since UNIX epoch (UTC) when the acquisition started.
    pub acquisition_start_time_ms: i64,
    /// Maximum ADC output value for this device.
    pub adc_max: i16,
    /// Minimum ADC output value for this device.
    pub adc_min: i16,
    /// Arbitrary key/value context tags set by the experiment configuration.
    pub context_tags: HashMap<String, String>,
    /// User-supplied experiment name.
    pub experiment_name: String,
    /// Flow cell serial identifier.
    pub flow_cell_id: String,
    /// Flow cell product code (e.g. `"FLO-MIN114"`).
    pub flow_cell_product_code: String,
    /// MinKNOW sequencing protocol name.
    pub protocol_name: String,
    /// Unique identifier for the protocol run.
    pub protocol_run_id: String,
    /// Milliseconds since UNIX epoch (UTC) when the protocol started.
    pub protocol_start_time_ms: i64,
    /// User-supplied sample identifier.
    pub sample_id: String,
    /// ADC sampling rate in Hz.
    pub sample_rate: u16,
    /// Sequencing kit product code (e.g. `"SQK-LSK114"`).
    pub sequencing_kit: String,
    /// Physical sequencer position label (e.g. `"MN12345"`).
    pub sequencer_position: String,
    /// Sequencer position type string (e.g. `"MinION Mk1C"`).
    pub sequencer_position_type: String,
    /// Software version string recorded at acquisition time.
    pub software: String,
    /// Host system name.
    pub system_name: String,
    /// Host system type string.
    pub system_type: String,
    /// Arbitrary key/value tracking ID tags set by MinKNOW.
    pub tracking_id: HashMap<String, String>,
}

impl RunInfo {
    /// ADC digitisation: total number of ADC levels = `adc_max - adc_min + 1`.
    pub fn digitisation(&self) -> f32 {
        (self.adc_max as i32 - self.adc_min as i32 + 1) as f32
    }
}

// ── ReadRecord ────────────────────────────────────────────────────────────────

/// All metadata and signal for one nanopore read.
///
/// Matches the Python `pod5.ReadRecord` public API field for field.
#[derive(Debug, Clone)]
pub struct ReadRecord {
    /// 16-byte UUID in standard RFC 4122 big-endian byte order. Use [`uuid_to_str`] to display.
    pub read_id: [u8; 16],
    /// Sequential read counter within the acquisition (1-indexed).
    pub read_number: u32,
    /// Physical pore location where this read was acquired.
    pub pore: Pore,
    /// ADC calibration parameters for converting signal to picoamps.
    pub calibration: Calibration,
    /// How and why this read was terminated.
    pub end_reason: EndReason,
    /// Run-level metadata for the acquisition this read belongs to.
    pub run_info: Arc<RunInfo>,
    /// Level immediately before the read (pA).
    pub median_before: f32,
    /// Total number of ADC samples in this read.
    pub num_samples: u64,
    /// Absolute sample index in the acquisition where this read began.
    pub start_sample: u64,
    /// Real-time tracked scaling from the signal processing pipeline; [`ShiftScalePair::UNAVAILABLE`] if not recorded.
    pub tracked_scaling: ShiftScalePair,
    /// Predicted scaling from the signal processing pipeline; [`ShiftScalePair::UNAVAILABLE`] if not recorded.
    pub predicted_scaling: ShiftScalePair,
    /// Number of MinKNOW event detections within this read.
    pub num_minknow_events: u64,
    /// Number of reads that have occurred on this channel since the last mux change.
    pub num_reads_since_mux_change: u32,
    /// Seconds since the last mux change.
    pub time_since_mux_change: f32,
    /// Open-pore level for this read (pA); NaN if not recorded.
    pub open_pore_level: f32,
    /// Expected open-pore level for this read (pA); NaN if not recorded (Reads table V5+).
    pub expected_open_pore_level: f32,
    /// Selected read level for this read (pA); NaN if not recorded (Reads table V5+).
    pub selected_read_level: f32,

    // Private: signal lazy-load backing.
    pub(crate) signal_row_indices: Vec<u64>,
    pub(crate) signal_table: Arc<SignalTable>,
}

impl ReadRecord {
    /// Human-readable UUID string (e.g. `"a1b2c3d4-..."`).
    pub fn read_id_str(&self) -> String {
        uuid_to_str(&self.read_id)
    }

    /// Decompress and return all raw ADC samples for this read as `i16`.
    pub fn signal(&self) -> Result<Vec<i16>> {
        self.signal_table.samples_for_rows(&self.signal_row_indices)
    }

    /// Decompress and calibrate signal to picoamps (`f32`).
    pub fn signal_pa(&self) -> Result<Vec<f32>> {
        let adc = self.signal()?;
        Ok(self.calibration.to_pa(&adc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn dummy_run_info(adc_min: i16, adc_max: i16) -> RunInfo {
        RunInfo {
            acquisition_id: String::new(),
            acquisition_start_time_ms: 0,
            adc_max,
            adc_min,
            context_tags: HashMap::new(),
            experiment_name: String::new(),
            flow_cell_id: String::new(),
            flow_cell_product_code: String::new(),
            protocol_name: String::new(),
            protocol_run_id: String::new(),
            protocol_start_time_ms: 0,
            sample_id: String::new(),
            sample_rate: 4000,
            sequencing_kit: String::new(),
            sequencer_position: String::new(),
            sequencer_position_type: String::new(),
            software: String::new(),
            system_name: String::new(),
            system_type: String::new(),
            tracking_id: HashMap::new(),
        }
    }

    #[test]
    fn calibration_from_range() {
        let cal = Calibration::from_range(-5.0, 2000.0, 2048.0);
        assert!((cal.offset - (-5.0)).abs() < 1e-6);
        assert!((cal.scale - 2000.0_f32 / 2048.0).abs() < 1e-6);
    }

    #[test]
    fn end_reason_round_trip() {
        let pairs = [
            ("unknown", EndReasonKind::Unknown),
            ("mux_change", EndReasonKind::MuxChange),
            ("unblock_mux_change", EndReasonKind::UnblockMuxChange),
            (
                "data_service_unblock_mux_change",
                EndReasonKind::DataServiceUnblockMuxChange,
            ),
            ("signal_positive", EndReasonKind::SignalPositive),
            ("signal_negative", EndReasonKind::SignalNegative),
            ("api_request", EndReasonKind::ApiRequest),
            ("device_data_error", EndReasonKind::DeviceDataError),
            (
                "analysis_config_change",
                EndReasonKind::AnalysisConfigChange,
            ),
            ("paused", EndReasonKind::Paused),
        ];
        for (s, expected) in pairs {
            let k = EndReasonKind::from_reason_str(s);
            assert_eq!(k, expected);
            assert_eq!(k.as_str(), s);
        }
    }

    #[test]
    fn end_reason_unknown_variant() {
        let k = EndReasonKind::from_reason_str("something_new");
        assert!(matches!(k, EndReasonKind::Other(_)));
        assert_eq!(k.as_str(), "other");
    }

    #[test]
    fn shift_scale_availability() {
        assert!(
            ShiftScalePair {
                shift: 1.0,
                scale: 2.0
            }
            .is_available()
        );
        assert!(!ShiftScalePair::UNAVAILABLE.is_available());
        assert!(
            !ShiftScalePair {
                shift: f32::NAN,
                scale: 1.0
            }
            .is_available()
        );
        assert!(
            !ShiftScalePair {
                shift: 1.0,
                scale: f32::NAN
            }
            .is_available()
        );
    }

    #[test]
    fn run_info_digitisation() {
        let ri = dummy_run_info(-2048, 2047);
        assert!((ri.digitisation() - 4096.0).abs() < 1e-6);
    }

    #[test]
    fn run_info_digitisation_symmetric() {
        let ri = dummy_run_info(0, 511);
        assert!((ri.digitisation() - 512.0).abs() < 1e-6);
    }
}
