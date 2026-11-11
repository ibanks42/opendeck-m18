use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use futures_lite::StreamExt;
use mirajazz::{
    device::{DeviceWatcher, list_devices},
    error::MirajazzError,
    types::{DeviceLifecycleEvent, HidDeviceInfo},
};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior, interval, sleep_until},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    SESSIONS, TRACKER,
    device::{device_task, disconnect_generation},
    mappings::{CandidateDevice, DEVICE_NAMESPACE, Kind, QUERIES},
};

fn get_device_id(dev: &HidDeviceInfo) -> Option<String> {
    let kind = Kind::from_vid_pid(dev.vendor_id, dev.product_id)?;

    match kind.protocol_version() {
        2 | 3 => Some(format!(
            "{}-{}",
            DEVICE_NAMESPACE,
            dev.serial_number.clone()?,
        )),
        1 => {
            // All the "v1" devices share the same serial. Hardcode it because Windows returns invalid serial for them
            // Also suffix v1 devices with the
            Some(format!(
                "{}-355499441494-{}",
                DEVICE_NAMESPACE,
                kind.id_suffix()
            ))
        }
        _ => None,
    }
}

fn device_info_to_candidate(dev: HidDeviceInfo) -> Option<CandidateDevice> {
    let id = get_device_id(&dev)?;
    let kind = Kind::from_vid_pid(dev.vendor_id, dev.product_id)?;

    Some(CandidateDevice { id, dev, kind })
}

/// Returns devices that matches known pid/vid pairs
async fn get_candidates() -> Result<Vec<CandidateDevice>, MirajazzError> {
    log::debug!("Looking for candidate devices");

    let started = Instant::now();
    let devices = list_devices(&QUERIES).await?;
    let elapsed = started.elapsed();
    // Enumeration touches every HID device on the system; a slow scan is a likely
    // source of system-wide input stutter, so make it visible in normal logs.
    if elapsed >= SLOW_SCAN {
        log::warn!("Device scan took {:?}", elapsed);
    } else {
        log::debug!("Device scan took {:?}", elapsed);
    }

    let mut candidates: Vec<CandidateDevice> = Vec::new();

    for dev in devices {
        if let Some(candidate) = device_info_to_candidate(dev.clone()) {
            candidates.push(candidate);
        } else {
            continue;
        }
    }

    Ok(candidates)
}

#[derive(Debug, Eq, PartialEq)]
enum SpawnOutcome {
    /// A new connection task was started.
    Started,
    /// A live session already exists for this id; nothing to do.
    Active,
    /// A previous session is still being torn down.
    Busy,
}

async fn spawn_candidate(
    tracker: &TaskTracker,
    candidate: CandidateDevice,
    generation: u64,
) -> SpawnOutcome {
    // Reserve the id before spawning so a hotplug notification and a recovery
    // attempt cannot start two connection generations at once.
    let token = {
        let mut sessions = SESSIONS.write().await;
        let Some(token) = sessions.reserve(candidate.id.clone(), generation) else {
            return if sessions.is_active(&candidate.id) {
                SpawnOutcome::Active
            } else {
                SpawnOutcome::Busy
            };
        };
        token
    };

    log::debug!("Spawning device task for {:?}", candidate);
    tracker.spawn(device_task(candidate, token));
    SpawnOutcome::Started
}

/// Delays between recovery attempts after a device session fails while the
/// device may still be plugged in. The first delay also gives the OS time to
/// deliver a disconnect event if the device was actually unplugged.
const RECOVERY_BACKOFF: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];
/// A device that stays healthy this long gets a fresh set of recovery attempts.
const RECOVERY_RESET_AFTER: Duration = Duration::from_secs(60);
/// Retry delay while the previous session is still cleaning up.
const RECOVERY_BUSY_RETRY: Duration = Duration::from_millis(500);
/// A fallback poll this close to a re-subscribe attempt is skipped.
const POLL_SKIP_WINDOW: Duration = Duration::from_secs(2);
/// Scans slower than this are logged as warnings.
const SLOW_SCAN: Duration = Duration::from_millis(50);
/// Periodic rescan interval, used only while OS hotplug notifications are down.
const FALLBACK_RESCAN: Duration = Duration::from_secs(15);

static RECOVERY_REQUESTS: std::sync::Mutex<Option<mpsc::UnboundedSender<String>>> =
    std::sync::Mutex::new(None);

/// Asks the watcher to check whether a device whose session just failed is still
/// present, and reconnect it if so. A device that was unplugged is left alone
/// until the OS reports it connected again.
pub fn request_recovery(id: &str) {
    let sender = RECOVERY_REQUESTS
        .lock()
        .ok()
        .and_then(|sender| sender.clone());
    if let Some(sender) = sender {
        sender.send(id.to_owned()).ok();
    }
}

struct RecoveryState {
    attempts: usize,
    last_failure: Instant,
    due: Option<Instant>,
}

#[derive(Default)]
struct RecoveryScheduler {
    entries: HashMap<String, RecoveryState>,
}

impl RecoveryScheduler {
    /// Records a failed session and schedules the next recovery attempt.
    /// Returns the attempt number and delay, or `None` once retries are exhausted.
    fn request(&mut self, id: &str, now: Instant) -> Option<(usize, Duration)> {
        let state = self.entries.entry(id.to_owned()).or_insert(RecoveryState {
            attempts: 0,
            last_failure: now,
            due: None,
        });
        if now.saturating_duration_since(state.last_failure) >= RECOVERY_RESET_AFTER {
            state.attempts = 0;
        }
        state.last_failure = now;

        let Some(delay) = RECOVERY_BACKOFF.get(state.attempts).copied() else {
            state.due = None;
            return None;
        };
        state.attempts += 1;
        state.due = Some(now + delay);
        Some((state.attempts, delay))
    }

    /// Retries soon without spending an attempt.
    fn retry_soon(&mut self, id: &str, now: Instant) {
        if let Some(state) = self.entries.get_mut(id) {
            state.due = Some(now + RECOVERY_BUSY_RETRY);
        }
    }

    /// Forgets a device, e.g. because the OS reported it unplugged or reconnected.
    /// Returns true if a recovery attempt was still pending.
    fn clear(&mut self, id: &str) -> bool {
        self.entries
            .remove(id)
            .is_some_and(|state| state.due.is_some())
    }

    fn next_due(&self) -> Option<Instant> {
        self.entries.values().filter_map(|state| state.due).min()
    }

    fn take_due(&mut self, now: Instant) -> Vec<String> {
        self.entries
            .iter_mut()
            .filter(|(_, state)| state.due.is_some_and(|due| due <= now))
            .map(|(id, state)| {
                state.due = None;
                id.clone()
            })
            .collect()
    }

    fn attempts(&self, id: &str) -> usize {
        self.entries.get(id).map_or(0, |state| state.attempts)
    }
}

async fn run_recovery<K: Eq + Hash>(
    tracker: &TaskTracker,
    generations: &GenerationTracker<K>,
    recovery: &mut RecoveryScheduler,
    due: Vec<String>,
) {
    let candidates = match get_candidates().await {
        Ok(candidates) => candidates,
        Err(err) => {
            log::error!("Device scan for recovery failed: {}", err);
            for id in due {
                schedule_recovery(recovery, &id);
            }
            return;
        }
    };

    for id in due {
        let Some(generation) = generations.current(&id) else {
            log::info!("{} was unplugged; waiting for it to be connected again", id);
            recovery.clear(&id);
            continue;
        };
        let Some(candidate) = candidates.iter().find(|candidate| candidate.id == id) else {
            log::info!(
                "{} is no longer present; waiting for it to be connected again",
                id
            );
            recovery.clear(&id);
            continue;
        };

        match spawn_candidate(tracker, candidate.clone(), generation).await {
            SpawnOutcome::Started => log::info!(
                "{} is still present after an error; reconnecting (attempt {}/{})",
                id,
                recovery.attempts(&id),
                RECOVERY_BACKOFF.len()
            ),
            SpawnOutcome::Active => {}
            SpawnOutcome::Busy => recovery.retry_soon(&id, Instant::now()),
        }
    }
}

fn schedule_recovery(recovery: &mut RecoveryScheduler, id: &str) {
    match recovery.request(id, Instant::now()) {
        Some((attempt, delay)) => log::info!(
            "Checking {} again in {:?} (recovery attempt {}/{})",
            id,
            delay,
            attempt,
            RECOVERY_BACKOFF.len()
        ),
        None => log::error!(
            "{} kept failing after {} reconnect attempts; unplug and replug it to retry",
            id,
            RECOVERY_BACKOFF.len()
        ),
    }
}

struct GenerationTracker<K> {
    by_device: HashMap<K, VecDeque<(String, u64)>>,
    current_by_id: HashMap<String, u64>,
    next: u64,
}

impl<K> Default for GenerationTracker<K> {
    fn default() -> Self {
        Self {
            by_device: HashMap::new(),
            current_by_id: HashMap::new(),
            next: 0,
        }
    }
}

impl<K: Eq + Hash> GenerationTracker<K> {
    fn allocate(&mut self) -> u64 {
        self.next = self.next.wrapping_add(1);
        self.next
    }

    fn track_existing(&mut self, device: K, id: String) -> u64 {
        let generation = match self.current_by_id.get(&id) {
            Some(generation) => *generation,
            None => {
                let generation = self.allocate();
                self.current_by_id.insert(id.clone(), generation);
                generation
            }
        };
        self.by_device
            .entry(device)
            .or_default()
            .push_back((id, generation));
        generation
    }

    fn track_connected(&mut self, device: K, id: String) -> u64 {
        let generation = self.allocate();
        self.current_by_id.insert(id.clone(), generation);
        self.by_device
            .entry(device)
            .or_default()
            .push_back((id, generation));
        generation
    }

    fn current(&self, id: &str) -> Option<u64> {
        self.current_by_id.get(id).copied()
    }

    fn take_disconnected(&mut self, device: &K, id: &str) -> Option<u64> {
        let tracked = self.by_device.get_mut(device).and_then(VecDeque::pop_front);
        let generation = match tracked {
            Some((tracked_id, generation)) => {
                if tracked_id != id {
                    log::error!(
                        "Disconnect event resolved to {} but its connection generation belongs to {}",
                        id,
                        tracked_id
                    );
                    return None;
                }
                generation
            }
            None => return None,
        };
        if self.by_device.get(device).is_some_and(VecDeque::is_empty) {
            self.by_device.remove(device);
        }

        if self.current(id) == Some(generation) {
            self.current_by_id.remove(id);
        }
        Some(generation)
    }
}

async fn handle_disconnected<K, I, Resolve, DeviceKey, Disconnect, DisconnectFuture>(
    generations: &mut GenerationTracker<K>,
    info: I,
    resolve_id: Resolve,
    device_key: DeviceKey,
    disconnect: Disconnect,
) where
    K: Eq + Hash,
    Resolve: FnOnce(&I) -> Option<String>,
    DeviceKey: FnOnce(&I) -> K,
    Disconnect: FnOnce(String, u64) -> DisconnectFuture,
    DisconnectFuture: Future<Output = bool>,
{
    let Some(id) = resolve_id(&info) else {
        log::error!("Unable to map disconnected device to an OpenDeck device id");
        return;
    };
    let device = device_key(&info);
    let Some(generation) = generations.take_disconnected(&device, &id) else {
        log::debug!("Ignoring stale disconnect event for {}", id);
        return;
    };

    if disconnect(id.clone(), generation).await {
        log::info!("Disconnected device {}", id);
    } else {
        log::debug!("Ignoring stale disconnect event for {}", id);
    }
}

/// Messages from the task that owns the OS hotplug subscription.
enum HotplugMessage {
    Subscribed,
    Event(DeviceLifecycleEvent),
    /// The subscription failed to start or its stream ended.
    Ended(Option<MirajazzError>),
}

/// Owns one OS hotplug subscription and forwards its events. A
/// [`DeviceWatcher`] can only be subscribed once, so each attempt gets a new one.
async fn run_hotplug(sender: mpsc::UnboundedSender<HotplugMessage>) {
    let mut watcher = DeviceWatcher::new();
    let mut stream = match watcher.watch(&QUERIES).await {
        Ok(stream) => stream,
        Err(err) => {
            sender.send(HotplugMessage::Ended(Some(err))).ok();
            return;
        }
    };
    if sender.send(HotplugMessage::Subscribed).is_err() {
        return;
    }

    while let Some(event) = stream.next().await {
        if sender.send(HotplugMessage::Event(event)).is_err() {
            return;
        }
    }
    sender.send(HotplugMessage::Ended(None)).ok();
}

/// Delays between attempts to re-subscribe to OS hotplug events.
const HOTPLUG_RESUBSCRIBE_BACKOFF: [Duration; 6] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(30),
];

fn hotplug_backoff(failures: usize) -> Duration {
    HOTPLUG_RESUBSCRIBE_BACKOFF[failures.min(HOTPLUG_RESUBSCRIBE_BACKOFF.len() - 1)]
}

/// Starts sessions for every matching device that is present but not connected.
/// Used when hotplug events may have been missed.
async fn sync_present_devices<K: Eq + Hash>(
    tracker: &TaskTracker,
    generations: &mut GenerationTracker<K>,
    device_key: impl Fn(&HidDeviceInfo) -> K,
    reason: &str,
) {
    match get_candidates().await {
        Ok(candidates) => {
            for candidate in candidates {
                let generation = generations.current(&candidate.id).unwrap_or_else(|| {
                    generations.track_existing(device_key(&candidate.dev), candidate.id.clone())
                });
                spawn_candidate(tracker, candidate, generation).await;
            }
        }
        Err(err) => log::error!("Device scan ({}) failed: {}", reason, err),
    }
}

pub async fn watcher_task(token: Arc<CancellationToken>) -> Result<(), MirajazzError> {
    let tracker = TRACKER.lock().await.clone();
    let mut generations = GenerationTracker::default();
    let mut recovery = RecoveryScheduler::default();

    let (recovery_sender, mut recovery_requests) = mpsc::unbounded_channel();
    if let Ok(mut sender) = RECOVERY_REQUESTS.lock() {
        *sender = Some(recovery_sender);
    }

    for candidate in get_candidates().await? {
        let generation = generations.track_existing(candidate.dev.id.clone(), candidate.id.clone());
        spawn_candidate(&tracker, candidate, generation).await;
    }

    // Keep one sender here so the channel never closes between subscriptions.
    let (hotplug_sender, mut hotplug_events) = mpsc::unbounded_channel();
    let mut hotplug_task = tokio::spawn(run_hotplug(hotplug_sender.clone()));
    let mut hotplug_active = false;
    let mut hotplug_failures = 0usize;
    let mut resubscribe_at: Option<Instant> = None;
    // Polling only runs while hotplug is down and re-subscribing keeps failing.
    let mut rescan = interval(FALLBACK_RESCAN);
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let result = loop {
        let next_recovery = recovery.next_due();
        let polling = !hotplug_active && hotplug_failures > 0;

        tokio::select! {
            Some(message) = hotplug_events.recv() => match message {
                HotplugMessage::Subscribed => {
                    if hotplug_failures > 0 {
                        log::info!("Re-subscribed to device hotplug events");
                        // Catch anything that changed while we were not listening.
                        sync_present_devices(&tracker, &mut generations, |dev| dev.id.clone(), "after re-subscribe").await;
                    } else {
                        log::debug!("Watcher is ready");
                    }
                    hotplug_active = true;
                    hotplug_failures = 0;
                    resubscribe_at = None;
                }
                HotplugMessage::Event(DeviceLifecycleEvent::Connected(info)) => {
                    log::info!("Device connected: {:?}", info);
                    if let Some(candidate) = device_info_to_candidate(info) {
                        recovery.clear(&candidate.id);
                        let generation = generations
                            .track_connected(candidate.dev.id.clone(), candidate.id.clone());
                        spawn_candidate(&tracker, candidate, generation).await;
                    }
                }
                HotplugMessage::Event(DeviceLifecycleEvent::Disconnected(info)) => {
                    log::info!("Device disconnected: {:?}", info);
                    // Fully unplugged: stop recovering and wait for the OS to
                    // report it connected again.
                    if let Some(id) = get_device_id(&info)
                        && recovery.clear(&id)
                    {
                        log::info!(
                            "{} was unplugged; cancelled reconnect check and waiting for it to be connected again",
                            id
                        );
                    }
                    handle_disconnected(
                        &mut generations,
                        info,
                        get_device_id,
                        |info| info.id.clone(),
                        |id, generation| async move {
                            let removed = disconnect_generation(&id, generation).await;
                            if removed {
                                crate::health::connection_lost(&id);
                            }
                            removed
                        },
                    )
                    .await;
                }
                HotplugMessage::Ended(err) => {
                    if hotplug_active || hotplug_failures == 0 {
                        // Just went down: start the poll timer once, so retries
                        // shorter than the poll interval cannot keep postponing it.
                        rescan.reset();
                    }
                    hotplug_active = false;
                    let delay = hotplug_backoff(hotplug_failures);
                    hotplug_failures += 1;
                    match err {
                        Some(err) => log::warn!(
                            "Could not subscribe to device hotplug events ({}); retrying in {:?}, polling every {:?} meanwhile",
                            err, delay, FALLBACK_RESCAN
                        ),
                        None => log::warn!(
                            "Device hotplug events stopped; re-subscribing in {:?}, polling every {:?} meanwhile",
                            delay, FALLBACK_RESCAN
                        ),
                    }
                    resubscribe_at = Some(Instant::now() + delay);
                }
            },
            _ = sleep_until(resubscribe_at.unwrap_or_else(Instant::now)), if resubscribe_at.is_some() => {
                resubscribe_at = None;
                hotplug_task = tokio::spawn(run_hotplug(hotplug_sender.clone()));
            }
            Some(id) = recovery_requests.recv() => {
                schedule_recovery(&mut recovery, &id);
            }
            _ = sleep_until(next_recovery.unwrap_or_else(Instant::now)), if next_recovery.is_some() => {
                let due = recovery.take_due(Instant::now());
                if !due.is_empty() {
                    run_recovery(&tracker, &generations, &mut recovery, due).await;
                }
            }
            _ = rescan.tick(), if polling => {
                // Without hotplug notifications there is no other way to notice
                // devices appearing, so poll until re-subscribing succeeds. Skip a
                // poll that lands right before a re-subscribe attempt; a
                // successful one runs its own catch-up scan.
                let resubscribing_soon = resubscribe_at
                    .is_some_and(|at| at.saturating_duration_since(Instant::now()) < POLL_SKIP_WINDOW);
                if !resubscribing_soon {
                    sync_present_devices(&tracker, &mut generations, |dev| dev.id.clone(), "fallback poll").await;
                }
            }
            _ = token.cancelled() => {
                log::debug!("Watcher is shutting down");

                break Ok(());
            }
        }
    };

    hotplug_task.abort();
    if let Ok(mut sender) = RECOVERY_REQUESTS.lock() {
        *sender = None;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_backs_off_then_gives_up() {
        let mut recovery = RecoveryScheduler::default();
        let start = Instant::now();
        let mut now = start;

        for (index, expected) in RECOVERY_BACKOFF.iter().enumerate() {
            let (attempt, delay) = recovery.request("m18", now).expect("attempt scheduled");
            assert_eq!(attempt, index + 1);
            assert_eq!(delay, *expected);
            assert_eq!(recovery.next_due(), Some(now + delay));
            now += delay;
            assert_eq!(recovery.take_due(now), vec!["m18".to_string()]);
        }

        assert_eq!(recovery.request("m18", now), None);
        assert_eq!(recovery.next_due(), None);
    }

    #[test]
    fn recovery_resets_after_healthy_period() {
        let mut recovery = RecoveryScheduler::default();
        let now = Instant::now();
        recovery.request("m18", now);
        recovery.request("m18", now);

        let later = now + RECOVERY_RESET_AFTER;
        assert_eq!(
            recovery.request("m18", later),
            Some((1, RECOVERY_BACKOFF[0]))
        );
    }

    #[test]
    fn nothing_is_due_until_its_deadline() {
        let mut recovery = RecoveryScheduler::default();
        let now = Instant::now();
        recovery.request("m18", now);

        assert!(recovery.take_due(now).is_empty());
        assert_eq!(recovery.take_due(now + RECOVERY_BACKOFF[0]).len(), 1);
        // Taking an entry clears its deadline so it is not retried twice.
        assert_eq!(recovery.next_due(), None);
    }

    #[test]
    fn busy_retry_does_not_spend_an_attempt() {
        let mut recovery = RecoveryScheduler::default();
        let now = Instant::now();
        recovery.request("m18", now);
        recovery.take_due(now + RECOVERY_BACKOFF[0]);

        recovery.retry_soon("m18", now);
        assert_eq!(recovery.attempts("m18"), 1);
        assert_eq!(recovery.next_due(), Some(now + RECOVERY_BUSY_RETRY));
    }

    #[test]
    fn unplug_clears_pending_recovery() {
        let mut recovery = RecoveryScheduler::default();
        let now = Instant::now();
        recovery.request("m18", now);

        assert!(recovery.clear("m18"));
        assert!(!recovery.clear("m18"));
        assert_eq!(recovery.next_due(), None);
        assert_eq!(recovery.attempts("m18"), 0);
        // A later failure starts from the first backoff step again.
        assert_eq!(recovery.request("m18", now), Some((1, RECOVERY_BACKOFF[0])));
    }

    #[test]
    fn hotplug_resubscribe_backoff_caps_at_30s() {
        let delays: Vec<u64> = (0..8).map(|n| hotplug_backoff(n).as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
    }
}
