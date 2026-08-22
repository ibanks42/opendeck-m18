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
use tokio::time::{MissedTickBehavior, interval};
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
    log::info!("Looking for candidate devices");

    let mut candidates: Vec<CandidateDevice> = Vec::new();

    for dev in list_devices(&QUERIES).await? {
        if let Some(candidate) = device_info_to_candidate(dev.clone()) {
            candidates.push(candidate);
        } else {
            continue;
        }
    }

    Ok(candidates)
}

async fn spawn_candidate(tracker: &TaskTracker, candidate: CandidateDevice, generation: u64) {
    // Reserve the id before spawning so a hotplug notification and a periodic
    // scan cannot start two connection generations at once.
    let token = {
        let mut sessions = SESSIONS.write().await;
        let Some(token) = sessions.reserve(candidate.id.clone(), generation) else {
            return;
        };
        token
    };

    log::info!("Spawning device task for {:?}", candidate);
    tracker.spawn(device_task(candidate, token));
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

pub async fn watcher_task(token: Arc<CancellationToken>) -> Result<(), MirajazzError> {
    let tracker = TRACKER.lock().await.clone();
    let mut generations = GenerationTracker::default();

    for candidate in get_candidates().await? {
        let generation = generations.track_existing(candidate.dev.id.clone(), candidate.id.clone());
        spawn_candidate(&tracker, candidate, generation).await;
    }

    let mut watcher = DeviceWatcher::new();
    let mut watcher_stream = watcher.watch(&QUERIES).await?;
    let mut rescan = interval(Duration::from_secs(2));
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);
    rescan.tick().await;
    let mut watcher_active = true;

    log::info!("Watcher is ready");

    loop {
        tokio::select! {
            event = watcher_stream.next(), if watcher_active => {
                match event {
                    Some(DeviceLifecycleEvent::Connected(info)) => {
                        log::info!("Device connected: {:?}", info);
                        if let Some(candidate) = device_info_to_candidate(info) {
                            let generation = generations
                                .track_connected(candidate.dev.id.clone(), candidate.id.clone());
                            spawn_candidate(&tracker, candidate, generation).await;
                        }
                    }
                    Some(DeviceLifecycleEvent::Disconnected(info)) => {
                        log::info!("Device disconnected: {:?}", info);
                        handle_disconnected(
                            &mut generations,
                            info,
                            get_device_id,
                            |info| info.id.clone(),
                            |id, generation| async move {
                                disconnect_generation(&id, generation).await
                            },
                        )
                        .await;
                    }
                    None => {
                        log::error!("Device watcher stream ended; periodic rescanning will continue");
                        watcher_active = false;
                    }
                }
            }
            _ = rescan.tick() => {
                match get_candidates().await {
                    Ok(candidates) => {
                        for candidate in candidates {
                            let generation = match generations.current(&candidate.id) {
                                Some(generation) => generation,
                                None if watcher_active => {
                                    // A disconnect was already observed. Wait for the
                                    // corresponding watcher connect event so this HID
                                    // instance gets a fresh generation.
                                    continue;
                                }
                                None => {
                                    generations.track_existing(
                                        candidate.dev.id.clone(),
                                        candidate.id.clone(),
                                    )
                                }
                            };
                            spawn_candidate(&tracker, candidate, generation).await;
                        }
                    }
                    Err(err) => log::error!("Periodic device scan failed: {}", err),
                }
            }
            _ = token.cancelled() => {
                log::info!("Watcher is shutting down");

                break Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn unmappable_disconnect_is_skipped_and_later_events_continue() {
        let mut generations = GenerationTracker::<u8>::default();
        let generation = generations.track_existing(1_u8, "device".to_string());
        let resolutions = Arc::new(AtomicUsize::new(0));
        let invalid_resolutions = resolutions.clone();

        handle_disconnected(
            &mut generations,
            99_u8,
            move |_| {
                invalid_resolutions.fetch_add(1, Ordering::Relaxed);
                None
            },
            |info| *info,
            |_, _| async { panic!("an unmappable event must not start cleanup") },
        )
        .await;

        let cleaned = Arc::new(Mutex::new(Vec::new()));
        let cleaned_event = cleaned.clone();
        let valid_resolutions = resolutions.clone();
        handle_disconnected(
            &mut generations,
            1_u8,
            move |_| {
                valid_resolutions.fetch_add(1, Ordering::Relaxed);
                Some("device".to_string())
            },
            |info| *info,
            move |id, generation| async move {
                cleaned_event.lock().await.push((id, generation));
                true
            },
        )
        .await;

        assert_eq!(resolutions.load(Ordering::Relaxed), 2);
        assert_eq!(
            *cleaned.lock().await,
            vec![("device".to_string(), generation)]
        );
    }

    #[tokio::test]
    async fn stale_disconnect_retains_its_original_generation() {
        let mut generations = GenerationTracker::default();
        let old_generation = generations.track_existing(1_u8, "device".to_string());
        let replacement_generation = generations.track_connected(1_u8, "device".to_string());
        let cleaned_generation = Arc::new(Mutex::new(None));
        let cleaned_event = cleaned_generation.clone();

        handle_disconnected(
            &mut generations,
            1_u8,
            |_| Some("device".to_string()),
            |info| *info,
            move |_, generation| async move {
                *cleaned_event.lock().await = Some(generation);
                false
            },
        )
        .await;

        assert_eq!(*cleaned_generation.lock().await, Some(old_generation));
        assert_eq!(generations.current("device"), Some(replacement_generation));
    }
}
