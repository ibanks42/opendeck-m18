use std::sync::Mutex;

use mirajazz::{error::MirajazzError, types::DeviceInput};

use crate::mappings::KEY_COUNT;

// Bottom button input codes (non-LCD buttons)
const BTN_LEFT: u8 = 0x25;
const BTN_MIDDLE: u8 = 0x30;
const BTN_RIGHT: u8 = 0x31;

// mirajazz's DeviceStateReader diffs the button vector we return against
// its own previous snapshot to compute ButtonDown/ButtonUp events, then
// overwrites that snapshot with whatever we return. So this needs to be
// the true current state of every button, not just the one that changed
// -- returning "only the current key is true, everything else false"
// tells mirajazz every other held button just released, even if it's
// still physically down (its next real release then gets silently
// swallowed, since mirajazz already thinks it's up).
//
// NOTE: shared across all connected devices of this type, since
// `process_input` is called as a bare `fn(u8, u8)` with no device
// identifier available to key per-device state on. Fine for the common
// single-device case; two simultaneously-connected M18-family devices
// would have their button states cross-contaminate here.
static BUTTON_STATE: Mutex<[bool; KEY_COUNT]> = Mutex::new([false; KEY_COUNT]);

pub fn process_input(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    log::info!("Processing input: key={}, state={}", input, state);

    match input {
        0..=15 => read_button_press(input, state),
        BTN_LEFT | BTN_MIDDLE | BTN_RIGHT => read_button_press(input, state),
        _ => Err(MirajazzError::BadData),
    }
}

/// Flips row order: row 0 ↔ row 2, row 1 stays.
/// Device is vertically flipped compared to OpenDeck.
fn flip_row(key: u8) -> u8 {
    let row = key / 5;
    let col = key % 5;
    (2 - row) * 5 + col
}

/// Converts opendeck key index to device key index (for sending images)
pub fn opendeck_to_device(key: u8) -> u8 {
    flip_row(key)
}

fn read_button_press(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    let mut button_state = BUTTON_STATE.lock().unwrap();

    // input == 0 carries no button change (e.g. a heartbeat report) --
    // just report the current state unchanged.
    if input != 0 {
        // Map input to OpenDeck button index (0-based)
        let pressed_index: usize = match input {
            // LCD buttons (1-15 from device, map to 0-14)
            1..=15 => (input - 1) as usize,
            // Bottom buttons (non-LCD, map to 15-17)
            BTN_LEFT => 15,
            BTN_MIDDLE => 16,
            BTN_RIGHT => 17,
            _ => return Err(MirajazzError::BadData),
        };

        button_state[pressed_index] = state != 0;
    }

    Ok(DeviceInput::ButtonStateChange(button_state.to_vec()))
}
