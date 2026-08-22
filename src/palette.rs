use openaction::SettingsValue;
use serde_json::json;

pub const ACTION_UUID: &str = "com.github.ibanks42.opendeck-m18.set-led-colors";
pub const LED_COUNT: usize = 24;
pub type LedPalette = [[u8; 3]; LED_COUNT];

pub const DEFAULT_PALETTE: LedPalette = [[0x78, 0x00, 0x00]; LED_COUNT];

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exactly_twenty_four_six_digit_rgb_colors() {
        let settings = json!({
            "ledColors": (0..LED_COUNT)
                .map(|index| format!("#{index:02x}80ff"))
                .collect::<Vec<_>>()
        });

        let palette = parse_palette(&settings).unwrap();

        assert_eq!(palette[0], [0x00, 0x80, 0xff]);
        assert_eq!(palette[23], [0x17, 0x80, 0xff]);
    }

    #[test]
    fn rejects_malformed_and_incorrectly_sized_palettes() {
        for settings in [
            json!({}),
            json!({ "ledColors": ["#ff0000"] }),
            json!({ "ledColors": vec!["ff0000"; LED_COUNT] }),
            json!({ "ledColors": vec!["#gg0000"; LED_COUNT] }),
            json!({ "ledColors": vec!["#ff000000"; LED_COUNT] }),
        ] {
            assert_eq!(parse_palette(&settings), None);
        }
    }

    #[test]
    fn untouched_action_uses_a_complete_visible_default() {
        let palette = palette_from_settings(&json!({})).unwrap();

        assert_eq!(palette, DEFAULT_PALETTE);
        assert!(palette.iter().all(|color| *color != [0, 0, 0]));
    }

    #[test]
    fn malformed_action_palette_is_not_replaced_with_a_default() {
        let settings = json!({ "ledColors": vec!["not-a-color"; LED_COUNT] });

        assert_eq!(palette_from_settings(&settings), None);
    }
}
