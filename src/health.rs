//! Tracks device liveness so connection losses can be explained in the log.
//!
//! The M18 resets itself (drops off USB and re-enumerates) after roughly 25–35 s
//! without a keepalive from the host. The plugin sends one every 10 s, so a loss
//! after a long silence points at the plugin or system having stalled (sleep,
//! starvation), while a loss during healthy communication points at the
//! physical connection: an unplug, cable, port, hub or power.

use std::{
    collections::{HashMap, VecDeque},
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

/// A keepalive this late is worth a warning; the device resets not long after.
pub const KEEPALIVE_LATE: Duration = Duration::from_secs(15);
/// Silence this long before a loss means the device's watchdog likely reset it.
const WATCHDOG_SILENCE: Duration = Duration::from_secs(20);
/// Losses during healthy communication within this window are counted.
const FLAKY_WINDOW: Duration = Duration::from_secs(60 * 60);
/// This many healthy-communication losses within the window triggers a warning.
const FLAKY_COUNT: usize = 3;

#[derive(Default)]
struct DeviceHealth {
    last_keepalive: Option<Instant>,
    healthy_losses: VecDeque<Instant>,
}

#[derive(Default)]
struct HealthRegistry {
    devices: HashMap<String, DeviceHealth>,
}

static HEALTH: LazyLock<Mutex<HealthRegistry>> = LazyLock::new(Default::default);

#[derive(Debug, PartialEq, Eq)]
pub enum LossCause {
    /// The device went quiet long enough for its own watchdog to reset it.
    Watchdog { silence: Duration },
    /// Communication was healthy right up to the loss.
    Connection {
        silence: Duration,
        recent_losses: usize,
    },
}

impl HealthRegistry {
    fn session_started(&mut self, id: &str, now: Instant) {
        self.devices
            .entry(id.to_owned())
            .or_default()
            .last_keepalive = Some(now);
    }

    /// Records a keepalive and returns how long it had been since the last one.
    fn keepalive_sent(&mut self, id: &str, now: Instant) -> Option<Duration> {
        let health = self.devices.entry(id.to_owned()).or_default();
        let gap = health
            .last_keepalive
            .map(|last| now.saturating_duration_since(last));
        health.last_keepalive = Some(now);
        gap
    }

    fn connection_lost(&mut self, id: &str, now: Instant) -> Option<LossCause> {
        let health = self.devices.get_mut(id)?;
        let silence = now.saturating_duration_since(health.last_keepalive.take()?);
        if silence >= WATCHDOG_SILENCE {
            return Some(LossCause::Watchdog { silence });
        }

        health.healthy_losses.push_back(now);
        while health
            .healthy_losses
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) > FLAKY_WINDOW)
        {
            health.healthy_losses.pop_front();
        }
        Some(LossCause::Connection {
            silence,
            recent_losses: health.healthy_losses.len(),
        })
    }
}

/// Call when a session has opened the device and starts talking to it.
pub fn session_started(id: &str) {
    if let Ok(mut health) = HEALTH.lock() {
        health.session_started(id, Instant::now());
    }
}

/// Call after each successful keepalive; warns when it went out late.
pub fn keepalive_sent(id: &str) {
    let gap = HEALTH
        .lock()
        .ok()
        .and_then(|mut health| health.keepalive_sent(id, Instant::now()));
    if let Some(gap) = gap
        && gap >= KEEPALIVE_LATE
    {
        log::warn!(
            "Keepalive to {} went out {:.1}s after the previous one; the device resets itself after roughly 25 s without one",
            id,
            gap.as_secs_f32()
        );
    }
}

/// Call once when a live session is lost; logs the most likely cause.
pub fn connection_lost(id: &str) {
    let cause = HEALTH
        .lock()
        .ok()
        .and_then(|mut health| health.connection_lost(id, Instant::now()));
    match cause {
        Some(LossCause::Watchdog { silence }) => log::warn!(
            "{} was lost {:.0}s after the last keepalive; the device most likely reset itself because the plugin or system stopped talking to it (sleep, suspend or a stall)",
            id,
            silence.as_secs_f32()
        ),
        Some(LossCause::Connection {
            silence,
            recent_losses,
        }) => {
            log::info!(
                "{} was lost while communication was healthy (last keepalive {:.0}s earlier): an unplug, or a cable, port, hub or power problem",
                id,
                silence.as_secs_f32()
            );
            if recent_losses >= FLAKY_COUNT {
                log::warn!(
                    "{} has dropped {} times in the last hour while communication was healthy; check its USB cable, port and any hub",
                    id,
                    recent_losses
                );
            }
        }
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_keepalive_reports_gap() {
        let mut registry = HealthRegistry::default();
        let start = Instant::now();
        registry.session_started("m18", start);
        assert_eq!(
            registry.keepalive_sent("m18", start + Duration::from_secs(18)),
            Some(Duration::from_secs(18))
        );
    }

    #[test]
    fn long_silence_is_attributed_to_watchdog() {
        let mut registry = HealthRegistry::default();
        let start = Instant::now();
        registry.session_started("m18", start);
        assert_eq!(
            registry.connection_lost("m18", start + Duration::from_secs(30)),
            Some(LossCause::Watchdog {
                silence: Duration::from_secs(30)
            })
        );
    }

    #[test]
    fn healthy_losses_are_counted_within_an_hour() {
        let mut registry = HealthRegistry::default();
        let mut now = Instant::now();
        for expected in 1..=3 {
            registry.session_started("m18", now);
            now += Duration::from_secs(5);
            assert_eq!(
                registry.connection_lost("m18", now),
                Some(LossCause::Connection {
                    silence: Duration::from_secs(5),
                    recent_losses: expected
                })
            );
            now += Duration::from_secs(60);
        }

        // An hour later the old losses no longer count.
        now += FLAKY_WINDOW;
        registry.session_started("m18", now);
        assert_eq!(
            registry.connection_lost("m18", now + Duration::from_secs(1)),
            Some(LossCause::Connection {
                silence: Duration::from_secs(1),
                recent_losses: 1
            })
        );
    }

    #[test]
    fn a_loss_is_only_reported_once_per_session() {
        let mut registry = HealthRegistry::default();
        let start = Instant::now();
        registry.session_started("m18", start);
        assert!(registry.connection_lost("m18", start).is_some());
        assert!(registry.connection_lost("m18", start).is_none());
    }
}
