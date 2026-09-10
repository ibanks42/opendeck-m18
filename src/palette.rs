use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::Mutex;

use serde_json::{Value as SettingsValue, json};

pub const ACTION_UUID: &str = "com.github.ibanks42.opendeck-m18.set-led-colors";
pub const LED_COUNT: usize = 24;
pub type LedPalette = [[u8; 3]; LED_COUNT];

pub const DEFAULT_PALETTE: LedPalette = [[0x78, 0x00, 0x00]; LED_COUNT];

#[derive(Default)]
pub struct PaletteStore {
    settings: Option<SettingsValue>,
    palettes: HashMap<String, LedPalette>,
}

pub static PALETTES: LazyLock<Mutex<PaletteStore>> =
    LazyLock::new(|| Mutex::new(PaletteStore::default()));

impl PaletteStore {
    // A selection made before the initial response wins over persisted settings.
    pub fn load(&mut self, settings: SettingsValue) -> bool {
        if self.settings.is_some() {
            return false;
        }
        let pending = !self.palettes.is_empty();
        if let Some(saved) = settings
            .get("ledPalettes")
            .and_then(SettingsValue::as_object)
        {
            for (id, value) in saved {
                if let Some(palette) = parse_palette(value) {
                    self.palettes.entry(id.clone()).or_insert(palette);
                }
            }
        }
        self.settings = Some(settings);
        pending
    }

    pub fn select(&mut self, id: &str, palette: LedPalette) {
        self.palettes.insert(id.to_owned(), palette);
    }

    pub fn get(&self, id: &str) -> Option<LedPalette> {
        self.palettes.get(id).copied()
    }

    pub fn saved_palettes(&self) -> &HashMap<String, LedPalette> {
        &self.palettes
    }

    pub fn settings_to_save(&self) -> Option<SettingsValue> {
        let mut settings = self.settings.clone()?;
        if !settings.is_object() {
            settings = json!({});
        }
        let entries: serde_json::Map<String, SettingsValue> = self
            .palettes
            .iter()
            .map(|(id, palette)| (id.clone(), action_settings(palette)))
            .collect();
        settings["ledPalettes"] = SettingsValue::Object(entries);
        Some(settings)
    }
}

pub fn parse_palette(settings: &SettingsValue) -> Option<LedPalette> {
    let colors = settings.get("ledColors")?.as_array()?;
    if colors.len() != LED_COUNT {
        return None;
    }

    let mut palette = [[0; 3]; LED_COUNT];
    for (destination, color) in palette.iter_mut().zip(colors) {
        *destination = parse_color(color.as_str()?)?;
    }

    Some(palette)
}

pub fn palette_from_settings(settings: &SettingsValue) -> Option<LedPalette> {
    Some(if settings.get("ledColors").is_some() {
        parse_palette(settings)?
    } else {
        DEFAULT_PALETTE
    })
}

pub fn settings_need_default(settings: &SettingsValue) -> bool {
    settings.get("ledColors").is_none()
}

pub fn action_settings(palette: &LedPalette) -> SettingsValue {
    json!({
        "ledColors": palette_strings(palette),
    })
}

fn parse_color(color: &str) -> Option<[u8; 3]> {
    let hex = color.strip_prefix('#')?;
    if hex.len() != 6 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }

    Some([
        u8::from_str_radix(&hex[0..2], 16).ok()?,
        u8::from_str_radix(&hex[2..4], 16).ok()?,
        u8::from_str_radix(&hex[4..6], 16).ok()?,
    ])
}

fn palette_strings(palette: &LedPalette) -> Vec<String> {
    palette
        .iter()
        .map(|[red, green, blue]| format!("#{red:02x}{green:02x}{blue:02x}"))
        .collect()
}

