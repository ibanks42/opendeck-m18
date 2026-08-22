use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_lite::StreamExt;
use mirajazz::{
    device::{DeviceWatcher, list_devices},
    error::MirajazzError,
    types::{DeviceLifecycleEvent, HidDeviceInfo},
};
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    TOKENS, TRACKER, TaskRegistration,
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
        _ => unreachable!(),
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
        let mut tokens = TOKENS.write().await;
        if tokens.contains_key(&candidate.id) {
            return;
        }

        let token = Arc::new(CancellationToken::new());
        tokens.insert(
            candidate.id.clone(),
            TaskRegistration {
                token: token.clone(),
                generation: Some(generation),
            },
        );
        token
    };

    log::info!("Spawning device task for {:?}", candidate);
    tracker.spawn(device_task(candidate, token));
}

async fn handle_disconnected(info: HidDeviceInfo, generation: u64) {
    let id = get_device_id(&info)
        .expect("Unable to get device id, check mappings in Kind::from_vid_pid");

    if disconnect_generation(&id, generation).await {
        log::info!("Disconnected device {}", id);
    } else {
        log::debug!("Ignoring stale disconnect event for {}", id);
    }
}

fn allocate_generation(next_generation: &mut u64) -> u64 {
    *next_generation = next_generation.wrapping_add(1);
    *next_generation
}

pub async fn watcher_task(token: Arc<CancellationToken>) -> Result<(), MirajazzError> {
    let tracker = TRACKER.lock().await.clone();
    let mut generations = HashMap::new();
    let mut next_generation = 0;

    for candidate in get_candidates().await? {
        let generation = match generations.get(&candidate.id) {
            Some(generation) => *generation,
            None => {
                let generation = allocate_generation(&mut next_generation);
                generations.insert(candidate.id.clone(), generation);
                generation
            }
        };
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
                            let generation = allocate_generation(&mut next_generation);
                            generations.insert(candidate.id.clone(), generation);
                            spawn_candidate(&tracker, candidate, generation).await;
                        }
                    }
                    Some(DeviceLifecycleEvent::Disconnected(info)) => {
                        log::info!("Device disconnected: {:?}", info);
                        let id = get_device_id(&info)
                            .expect("Unable to get device id, check mappings in Kind::from_vid_pid");
                        if let Some(generation) = generations.remove(&id) {
                            handle_disconnected(info, generation).await;
                        } else {
                            log::debug!("Ignoring stale disconnect event for {}", id);
                        }
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
                            let generation = match generations.get(&candidate.id) {
                                Some(generation) => *generation,
                                None if watcher_active => {
                                    // A disconnect was already observed. Wait for the
                                    // corresponding watcher connect event so this HID
                                    // instance gets a fresh generation.
                                    continue;
                                }
                                None => {
                                    let generation = allocate_generation(&mut next_generation);
                                    generations.insert(candidate.id.clone(), generation);
                                    generation
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
