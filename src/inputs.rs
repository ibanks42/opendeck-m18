use std::sync::Arc;

use mirajazz::error::MirajazzError;
use tokio::sync::RwLock;

use crate::mappings::KEY_COUNT;

// Bottom button input codes (non-LCD buttons)
pub(crate) const BTN_LEFT: u8 = 0x25;
pub(crate) const BTN_MIDDLE: u8 = 0x30;
pub(crate) const BTN_RIGHT: u8 = 0x31;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ButtonEvent {
    Down(u8),
    Up(u8),
}

pub struct ButtonSession {
    states: [bool; KEY_COUNT],
    gate: Arc<RwLock<()>>,
}

impl ButtonSession {
    pub fn new() -> Self {
        Self {
            states: [false; KEY_COUNT],
            gate: Arc::new(RwLock::new(())),
        }
    }

    pub fn gate(&self) -> Arc<RwLock<()>> {
        self.gate.clone()
    }

    pub fn process_report(&mut self, report: &[u8]) -> Result<Option<ButtonEvent>, MirajazzError> {
        if !report.starts_with(&[65, 67, 75]) {
            return Ok(None);
        }

        let input = *report.get(9).ok_or(MirajazzError::BadData)?;
        let state = *report.get(10).ok_or(MirajazzError::BadData)?;

        log::info!("Processing input: key={}, state={}", input, state);

        if input == 0 {
            return Ok(None);
        }

        let position = match input {
            1..=15 => input - 1,
            BTN_LEFT => 15,
            BTN_MIDDLE => 16,
            BTN_RIGHT => 17,
            _ => return Err(MirajazzError::BadData),
        };
        let pressed = state != 0;
        let current = &mut self.states[position as usize];

        if *current == pressed {
            return Ok(None);
        }

        *current = pressed;
        Ok(Some(if pressed {
            ButtonEvent::Down(position)
        } else {
            ButtonEvent::Up(position)
        }))
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
