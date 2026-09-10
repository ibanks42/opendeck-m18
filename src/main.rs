use device::{DeviceCommand, DeviceOutput, command_for_set_image};
use mirajazz::device::Device;
use openaction::global_events::{SetBrightnessEvent, SetImageEvent, set_global_event_handler};
use openaction::*;
use palette::{
    ACTION_UUID, DEFAULT_PALETTE, action_settings, palette_from_settings, settings_need_default,
};
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicBool, Ordering},
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
mod session;
mod watcher;

pub static SESSIONS: LazyLock<RwLock<session::SessionRegistry<Arc<Device>, Arc<DeviceOutput>>>> =
    LazyLock::new(|| RwLock::new(session::SessionRegistry::default()));
pub static TRACKER: LazyLock<Mutex<TaskTracker>> = LazyLock::new(|| Mutex::new(TaskTracker::new()));

// OpenAction's send helpers return Ok before their outbound manager is initialized.
// Track the connection lifetime so device workers still fail when it is unavailable.
static CONNECTION_READY: AtomicBool = AtomicBool::new(false);

fn require_opendeck_connection() -> Result<(), String> {
    if CONNECTION_READY.load(Ordering::Acquire) {
        Ok(())
    } else {
        Err("OpenDeck outbound connection is unavailable".to_string())
    }
}

struct ConnectionGuard;

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        CONNECTION_READY.store(false, Ordering::Release);
    }
}

async fn send_device_command(device_id: &str, command: DeviceCommand) {
    let output = SESSIONS.read().await.output(device_id).cloned();

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
#[async_trait]
impl global_events::GlobalEventHandler for GlobalEventHandler {
    async fn plugin_ready(&self) -> OpenActionResult<()> {
        CONNECTION_READY.store(true, Ordering::Release);
        get_global_settings().await?;
        let tracker = TRACKER.lock().await.clone();

        let token = Arc::new(CancellationToken::new());
        tracker.spawn(watcher_task(token.clone()));

        SESSIONS
            .write()
            .await
            .insert_task("_watcher_task".to_string(), token);

        log::info!("Plugin initialized");

        Ok(())
    }

    async fn did_receive_global_settings(
        &self,
        event: global_events::DidReceiveGlobalSettingsEvent,
    ) -> OpenActionResult<()> {
        let mut store = palette::PALETTES.lock().await;
        let pending = store.load(event.payload.settings);
        for (id, colors) in store.saved_palettes() {
            let output = SESSIONS.read().await.output(id).cloned();
            if let Some(output) = output
                && output
                    .send(DeviceCommand::SetLedColors(*colors))
                    .await
                    .is_err()
            {
                output.token.cancel();
            }
        }
        if pending && let Some(settings) = store.settings_to_save() {
            set_global_settings(settings).await?;
        }
        Ok(())
    }

    async fn device_plugin_set_image(&self, event: SetImageEvent) -> OpenActionResult<()> {
        log::debug!("Asked to set image: {:#?}", event);

        // Skip knobs images
        if event.controller == Some("Encoder".to_string()) {
            log::debug!("Looks like a knob, no need to set image");
            return Ok(());
        }

        let output = SESSIONS.read().await.output(&event.device).cloned();

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

    async fn device_plugin_set_brightness(
        &self,
        event: SetBrightnessEvent,
    ) -> OpenActionResult<()> {
        log::debug!("Asked to set brightness: {:#?}", event);

        let output = SESSIONS.read().await.output(&event.device).cloned();

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

struct LedColorsAction;

async fn apply_palette(instance: &Instance, settings: &serde_json::Value) {
    log::debug!(
        "Applying LED palette for action {} to device {}",
        instance.instance_id,
        instance.device_id
    );

    let Some(palette) = palette_from_settings(settings) else {
        log::warn!(
            "Ignoring malformed LED palette for action {}",
            instance.instance_id
        );
        return;
    };

    let mut store = palette::PALETTES.lock().await;
    store.select(&instance.device_id, palette);
    send_device_command(&instance.device_id, DeviceCommand::SetLedColors(palette)).await;
    if let Some(settings) = store.settings_to_save()
        && let Err(error) = set_global_settings(settings).await
    {
        log::error!("Unable to save LED selection through OpenDeck: {}", error);
    }
}

#[async_trait]
impl Action for LedColorsAction {
    const UUID: ActionUuid = ACTION_UUID;
    type Settings = serde_json::Value;

    async fn key_down(
        &self,
        instance: &Instance,
        settings: &Self::Settings,
    ) -> OpenActionResult<()> {
        apply_palette(instance, settings).await;

        Ok(())
    }

    async fn will_appear(
        &self,
        instance: &Instance,
        settings: &Self::Settings,
    ) -> OpenActionResult<()> {
        log::debug!("will_appear called");
        if settings_need_default(settings) {
            instance
                .set_settings(&action_settings(&DEFAULT_PALETTE))
                .await?;
        }

        Ok(())
    }
}

/// Verbosity comes from `OPENDECK_M18_LOG`, or from a `debug` marker file in the
/// plugin directory. OpenDeck runs a plugin with its own directory as the working
/// directory, and already uses a `debug` file there to open devtools for webview
/// plugins, so the marker enables debug logs without relaunching OpenDeck from a
/// terminal.
fn log_level() -> simplelog::LevelFilter {
    if let Ok(level) = std::env::var("OPENDECK_M18_LOG")
        && let Ok(level) = level.parse()
    {
        return level;
    }

    let beside_exe = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("debug")));
    if std::path::Path::new("debug").exists() || beside_exe.is_some_and(|path| path.exists()) {
        return simplelog::LevelFilter::Debug;
    }

    simplelog::LevelFilter::Info
}

async fn shutdown() {
    SESSIONS.read().await.cancel_all();
}

async fn connect(args: Vec<String>) -> OpenActionResult<()> {
    let _connection = ConnectionGuard;
    let result = run(args).await;
    if let Err(error) = &result {
        log::error!("Failed to initialize plugin: {}", error);
    }
    result
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
    let level = log_level();

    simplelog::TermLogger::init(
        level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Never,
    )
    .unwrap();

    // Logged unconditionally so a run always states which build is executing and
    // how it resolved its verbosity.
    log::info!(
        "opendeck-m18 {} starting: level={}, OPENDECK_M18_LOG={:?}, cwd={:?}, exe={:?}",
        env!("CARGO_PKG_VERSION"),
        level,
        std::env::var("OPENDECK_M18_LOG").ok(),
        std::env::current_dir().ok(),
        std::env::current_exe().ok()
    );

    set_global_event_handler(&GlobalEventHandler {});
    register_action(LedColorsAction).await;

    let result: Result<(), Box<dyn std::error::Error>> = tokio::select! {
        result = connect(std::env::args().collect()) => result.map_err(Into::into),
        result = sigterm() => result,
    };

    log::info!("Shutting down");

    shutdown().await;

    let tracker = TRACKER.lock().await.clone();

    log::debug!("Waiting for tasks to finish");

    tracker.close();
    tracker.wait().await;

    log::info!("Tasks are finished, exiting now");

    result
}
