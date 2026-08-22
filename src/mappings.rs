use mirajazz::{
    device::DeviceQuery,
    types::{HidDeviceInfo, ImageFormat, ImageMirroring, ImageMode, ImageRotation},
};

// 18 for M18
// Must be unique between all the plugins, 2 characters long and match `DeviceNamespace` field in `manifest.json`
pub const DEVICE_NAMESPACE: &str = "18";

// Device layout: 3 rows of 5 LCD keys + 1 row with 3 bottom buttons (+ 2 unused slots)
// OpenDeck sees this as a 4x5 grid (20 slots, but only 18 are real buttons)
pub const ROW_COUNT: usize = 4;
pub const COL_COUNT: usize = 5;
pub const KEY_COUNT: usize = ROW_COUNT * COL_COUNT;
pub const ENCODER_COUNT: usize = 0;

#[derive(Debug, Clone)]
pub enum Kind {
    VsdInsideM18,
    MiraboxM18,
}

pub const VSDINSIDE_VID: u16 = 0x5548;
pub const MIRABOX_VID: u16 = 0x6603;
pub const VSDINSIDE_M18_PID: u16 = 0x1000;
pub const MIRABOX_M18_PID: u16 = 0x1009;
pub const MIRABOX_M18EN_PID: u16 = 0x1012;

// Map all queries to usage page 65440 and usage id 1
pub const VSDINSIDE_M18_QUERY: DeviceQuery =
    DeviceQuery::new(65440, 1, VSDINSIDE_VID, VSDINSIDE_M18_PID);
pub const MIRABOX_M18_QUERY: DeviceQuery = DeviceQuery::new(65440, 1, MIRABOX_VID, MIRABOX_M18_PID);
pub const MIRABOX_M18EN_QUERY: DeviceQuery =
    DeviceQuery::new(65440, 1, MIRABOX_VID, MIRABOX_M18EN_PID);

pub const QUERIES: [DeviceQuery; 3] = [VSDINSIDE_M18_QUERY, MIRABOX_M18_QUERY, MIRABOX_M18EN_QUERY];

/// Returns correct image format for device kind and key
pub fn get_image_format_for_key(kind: &Kind, _key: u8) -> ImageFormat {
    match kind {
        Kind::VsdInsideM18 | Kind::MiraboxM18 => ImageFormat {
            mode: ImageMode::JPEG,
            size: (64, 64),
            rotation: ImageRotation::Rot180,
            mirror: ImageMirroring::Both,
        },
    }
}

impl Kind {
    /// Matches devices VID+PID pairs to correct kinds
    pub fn from_vid_pid(vid: u16, pid: u16) -> Option<Self> {
        match (vid, pid) {
            (VSDINSIDE_VID, VSDINSIDE_M18_PID) => Some(Kind::VsdInsideM18),
            (MIRABOX_VID, MIRABOX_M18_PID) => Some(Kind::MiraboxM18),
            (MIRABOX_VID, MIRABOX_M18EN_PID) => Some(Kind::MiraboxM18),
            _ => None,
        }
    }

    /// Returns protocol version for device
    /// Protocol 1: 512-byte packets, 85x85 images
    /// Protocol 2: 1024-byte packets
    /// Protocol 3: 1024-byte packets, variable image sizes, supports press/release states
    /// M18 uses protocol 3 (supports both press and release states)
    pub fn protocol_version(&self) -> usize {
        match self {
            Self::VsdInsideM18 | Self::MiraboxM18 => 3,
        }
    }

    /// Returns human-readable device name
    pub fn human_name(&self) -> String {
        match self {
            Self::VsdInsideM18 => "VSD Inside M18",
            Self::MiraboxM18 => "Mirabox M18",
        }
        .to_string()
    }

    /// Because "v1" devices all share the same serial number, use custom suffix to be able to connect
    /// two devices with the different revisions at the same time
    pub fn id_suffix(&self) -> String {
        match self {
            Self::VsdInsideM18 => "VSD-M18",
            Self::MiraboxM18 => "MBOX-M18",
        }
        .to_string()
    }
}

#[derive(Debug, Clone)]
pub struct CandidateDevice {
    pub id: String,
    pub dev: HidDeviceInfo,
    pub kind: Kind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_supported_m18_variants_remain_mapped() {
        assert!(matches!(
            Kind::from_vid_pid(VSDINSIDE_VID, VSDINSIDE_M18_PID),
            Some(Kind::VsdInsideM18)
        ));
        assert!(matches!(
            Kind::from_vid_pid(MIRABOX_VID, MIRABOX_M18_PID),
            Some(Kind::MiraboxM18)
        ));
        assert!(matches!(
            Kind::from_vid_pid(MIRABOX_VID, MIRABOX_M18EN_PID),
            Some(Kind::MiraboxM18)
        ));
    }
}
