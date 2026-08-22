use device::{DeviceCommand, DeviceOutput, command_for_set_image};
use mirajazz::device::Device;
use openaction::*;
use palette::{
    ACTION_UUID, DEFAULT_PALETTE, action_settings, palette_from_settings, settings_need_default,
};
use std::{
    collections::HashMap,
    process::exit,
    sync::{Arc, LazyLock},
};
use tokio::sync::{Mutex, RwLock};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use watcher::watcher_task;

#[cfg(not(target_os = "windows"))]
use tokio::signal::unix::{SignalKind, signal};

mod device;
mod inputs;
mod mappings;
mod palette;
mod watcher;

pub static DEVICES: LazyLock<RwLock<HashMap<String, Arc<Device>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
pub struct TaskRegistration {
    pub token: Arc<CancellationToken>,
    pub generation: Option<u64>,
}

pub static TOKENS: LazyLock<RwLock<HashMap<String, TaskRegistration>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
pub static OUTPUTS: LazyLock<RwLock<HashMap<String, Arc<DeviceOutput>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
pub static TRACKER: LazyLock<Mutex<TaskTracker>> = LazyLock::new(|| Mutex::new(TaskTracker::new()));

async fn send_device_command(device_id: &str, command: DeviceCommand) {
    let output = OUTPUTS.read().await.get(device_id).cloned();

    if let Some(output) = output {
        if output.send(command).await.is_err() {
            log::error!("Output worker for device {} is unavailable", output.id);
            output.token.cancel();
        }
    } else {
        log::error!("Received event for unknown device: {}", device_id);
    }
}

struct GlobalEventHandler {}
impl openaction::GlobalEventHandler for GlobalEventHandler {
    async fn plugin_ready(
        &self,
        _outbound: &mut openaction::OutboundEventManager,
    ) -> EventHandlerResult {
        let tracker = TRACKER.lock().await.clone();

        let token = Arc::new(CancellationToken::new());
        tracker.spawn(watcher_task(token.clone()));

        TOKENS.write().await.insert(
            "_watcher_task".to_string(),
            TaskRegistration {
                token,
                generation: None,
            },
        );

        log::info!("Plugin initialized");

        Ok(())
    }

    async fn set_image(
        &self,
        event: SetImageEvent,
        _outbound: &mut OutboundEventManager,
    ) -> EventHandlerResult {
        log::debug!("Asked to set image: {:#?}", event);

        // Skip knobs images
        if event.controller == Some("Encoder".to_string()) {
            log::debug!("Looks like a knob, no need to set image");
            return Ok(());
        }

        let output = OUTPUTS.read().await.get(&event.device).cloned();

        if let Some(output) = output {
            match command_for_set_image(event) {
                Ok(Some(command)) => {
                    if output.send(command).await.is_err() {
                        log::error!("Output worker for device {} is unavailable", output.id);
                        output.token.cancel();
                    }
                }
                Ok(None) => {}
                Err(err) => log::error!("Unable to prepare image: {}", err),
            }
        } else {
            log::error!("Received event for unknown device: {}", event.device);
        }

        Ok(())
    }

    async fn set_brightness(
        &self,
        event: SetBrightnessEvent,
        _outbound: &mut OutboundEventManager,
    ) -> EventHandlerResult {
        log::debug!("Asked to set brightness: {:#?}", event);

        let output = OUTPUTS.read().await.get(&event.device).cloned();

        if let Some(output) = output {
            if output
                .send(DeviceCommand::SetBrightness(event.brightness))
                .await
                .is_err()
            {
                log::error!("Output worker for device {} is unavailable", output.id);
                output.token.cancel();
            }
        } else {
            log::error!("Received event for unknown device: {}", event.device);
        }

        Ok(())
    }
}

struct ActionEventHandler {}
impl openaction::ActionEventHandler for ActionEventHandler {
    async fn key_down(
        &self,
        event: KeyEvent,
        _outbound: &mut OutboundEventManager,
    ) -> EventHandlerResult {
        if event.action != ACTION_UUID {
            return Ok(());
        }

        let Some(palette) = palette_from_settings(&event.payload.settings) else {
            log::warn!(
                "Ignoring malformed LED palette for action {}",
                event.context
            );
            return Ok(());
        };

        send_device_command(&event.device, DeviceCommand::SetLedColors(palette)).await;

        Ok(())
    }

    async fn will_appear(
        &self,
        event: AppearEvent,
        outbound: &mut OutboundEventManager,
    ) -> EventHandlerResult {
        if event.action == ACTION_UUID && settings_need_default(&event.payload.settings) {
            outbound
                .set_settings(event.context, action_settings(&DEFAULT_PALETTE))
                .await?;
        }

        Ok(())
    }
}

async fn shutdown() {
    let tokens = TOKENS.write().await;

    for registration in tokens.values() {
        registration.token.cancel();
    }
}

async fn connect() {
    if let Err(error) = init_plugin(GlobalEventHandler {}, ActionEventHandler {}).await {
        log::error!("Failed to initialize plugin: {}", error);

        exit(1);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn sigterm() -> Result<(), Box<dyn std::error::Error>> {
    let mut sig = signal(SignalKind::terminate())?;

    sig.recv().await;

    Ok(())
}

#[cfg(target_os = "windows")]
async fn sigterm() -> Result<(), Box<dyn std::error::Error>> {
    // Future that would never resolve, so select only acts on OpenDeck connection loss
    // TODO: Proper windows termination handling
    std::future::pending::<()>().await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simplelog::TermLogger::init(
        simplelog::LevelFilter::Info,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Never,
    )
    .unwrap();

    tokio::select! {
        _ = connect() => {},
        _ = sigterm() => {},
    }

    log::info!("Shutting down");

    shutdown().await;

    let tracker = TRACKER.lock().await.clone();

    log::info!("Waiting for tasks to finish");

    tracker.close();
    tracker.wait().await;

    log::info!("Tasks are finished, exiting now");

    Ok(())
}
