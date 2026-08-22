use std::{future::Future, sync::Arc, time::Duration};

use data_url::DataUrl;
use image::{DynamicImage, load_from_memory_with_format};
use mirajazz::{device::Device, error::MirajazzError, state::DeviceStateUpdate};
use openaction::{OUTBOUND_EVENT_MANAGER, SetImageEvent};
use tokio::{
    sync::{RwLock, mpsc},
    time::{Instant, MissedTickBehavior, interval, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::{
    SESSIONS,
    inputs::opendeck_to_device,
    mappings::{
        COL_COUNT, CandidateDevice, ENCODER_COUNT, KEY_COUNT, Kind, ROW_COUNT,
        get_image_format_for_key,
    },
    palette::LedPalette,
    session::{Removal, SessionMatch, SessionRegistry},
};

pub enum DeviceCommand {
    SetImage { position: u8, image: DynamicImage },
    ClearImage(u8),
    ClearAll,
    SetBrightness(u8),
    SetLedColors(LedPalette),
}

pub struct DeviceOutput {
    pub id: String,
    pub token: Arc<CancellationToken>,
    sender: mpsc::Sender<DeviceCommand>,
}

impl DeviceOutput {
    pub async fn send(&self, command: DeviceCommand) -> Result<(), ()> {
        self.sender.send(command).await.map_err(|_| ())
    }
}

/// Initializes a device and listens for events
pub async fn device_task(candidate: CandidateDevice, token: Arc<CancellationToken>) {
    log::info!("Running device task for {:?}", candidate);

    let device = async {
        let device = connect(&candidate).await?;

        // Initialization is deliberately not cancellation-selectable. If the USB
        // handle fails during one of these writes, the write future must resolve
        // before the device is discarded.
        device.set_brightness(50).await?;
        device.clear_all_button_images().await?;
        device.flush().await?;

        Ok(device)
    }
    .await;

    let device: Device = match device {
        Ok(device) => device,
        Err(err) => {
            handle_error(&candidate.id, &token, err).await;
            log::error!(
                "Had error during device init, finishing device task: {:?}",
                candidate
            );
            return;
        }
    };

    let device = Arc::new(device);
    let (sender, receiver) = mpsc::channel(128);
    let output = Arc::new(DeviceOutput {
        id: candidate.id.clone(),
        token: token.clone(),
        sender,
    });

    match publish_device_if_current(&candidate, &device, &output, &token).await {
        PublicationOutcome::Published => {}
        PublicationOutcome::Stale => {
            log::debug!("Discarding cancelled connection for {}", candidate.id);
            device.shutdown().await.ok();
            return;
        }
        PublicationOutcome::RegistrationFailed(error) => {
            log::error!("Unable to register device {}: {}", candidate.id, error);
            device.shutdown().await.ok();
            return;
        }
    }

    let mut output_task = tokio::spawn(device_output_task(
        candidate.id.clone(),
        candidate.kind.clone(),
        device.clone(),
        receiver,
        token.clone(),
    ));
    let input_task = tokio::spawn(device_events_task(
        candidate.clone(),
        device.clone(),
        token.clone(),
    ));

    let output_finished = tokio::select! {
        result = &mut output_task => Some(result),
        _ = token.cancelled() => None,
    };

    let output_result = match output_finished {
        Some(result) => result,
        None => output_task.await,
    };

    match output_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            handle_error(&candidate.id, &token, err).await;
        }
        Err(err) => {
            log::error!("Output worker for {} panicked: {}", candidate.id, err);
            disconnect_session(&candidate.id, &token).await;
        }
    }

    disconnect_session(&candidate.id, &token).await;

    log::info!("Shutting down owned device {:?}", candidate);
    // This task always owns this exact handle. The output worker has fully
    // stopped, so shutdown cannot overlap with another write on it.
    device.shutdown().await.ok();

    // A pending HID read must not be cancelled and then reused. Detaching lets a
    // physical disconnect complete it normally; it owns no output path.
    drop(input_task);

    log::info!("Device task finished for {:?}", candidate);
}

#[derive(Debug, Eq, PartialEq)]
enum PublicationOutcome {
    Published,
    Stale,
    RegistrationFailed(String),
}

async fn publish_with<D, O, Register, RegisterFuture, Deregister, DeregisterFuture>(
    sessions: &RwLock<SessionRegistry<D, O>>,
    id: &str,
    device: D,
    output: O,
    token: &Arc<CancellationToken>,
    register: Register,
    deregister: Deregister,
) -> PublicationOutcome
where
    Register: FnOnce() -> RegisterFuture,
    RegisterFuture: Future<Output = Result<(), String>>,
    Deregister: FnOnce(String) -> DeregisterFuture,
    DeregisterFuture: Future<Output = Result<(), String>>,
{
    if sessions
        .write()
        .await
        .begin_registration(id, token, device, output)
        .is_err()
    {
        sessions
            .write()
            .await
            .begin_removal(id, SessionMatch::Token(token));
        return PublicationOutcome::Stale;
    }

    if let Err(error) = register().await {
        sessions.write().await.discard_registration(id, token);
        return PublicationOutcome::RegistrationFailed(error);
    }

    if sessions.write().await.finish_registration(id, token) {
        return PublicationOutcome::Published;
    }

    if let Err(error) = deregister(id.to_owned()).await {
        log::error!(
            "Unable to roll back stale OpenDeck registration for {}: {}",
            id,
            error
        );
    }
    sessions.write().await.discard_registration(id, token);
    PublicationOutcome::Stale
}

async fn register_opendeck_device(candidate: &CandidateDevice) -> Result<(), String> {
    let mut manager = OUTBOUND_EVENT_MANAGER.lock().await;
    let outbound = manager
        .as_mut()
        .ok_or_else(|| "OpenDeck outbound connection is unavailable".to_string())?;

    outbound
        .register_device(
            candidate.id.clone(),
            candidate.kind.human_name(),
            ROW_COUNT as u8,
            COL_COUNT as u8,
            ENCODER_COUNT as u8,
            0,
        )
        .await
        .map_err(|error| error.to_string())
}

async fn publish_device_if_current(
    candidate: &CandidateDevice,
    device: &Arc<Device>,
    output: &Arc<DeviceOutput>,
    token: &Arc<CancellationToken>,
) -> PublicationOutcome {
    log::info!("Registering device {}", candidate.id);
    publish_with(
        &SESSIONS,
        &candidate.id,
        device.clone(),
        output.clone(),
        token,
        || register_opendeck_device(candidate),
        deregister_opendeck_device,
    )
    .await
}

async fn deregister_opendeck_device(id: String) -> Result<(), String> {
    let mut manager = OUTBOUND_EVENT_MANAGER.lock().await;
    let outbound = manager
        .as_mut()
        .ok_or_else(|| "OpenDeck outbound connection is unavailable".to_string())?;
    outbound
        .deregister_device(id)
        .await
        .map_err(|error| error.to_string())
}

async fn disconnect_matching_with<D, O, F, Fut>(
    sessions: &RwLock<SessionRegistry<D, O>>,
    id: &str,
    expected: SessionMatch<'_>,
    deregister: F,
) -> bool
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let Some(removal) = sessions.write().await.begin_removal(id, expected) else {
        log::debug!("Ignoring stale disconnect from replaced device {}", id);
        return false;
    };
    let Removal::Ready(removed) = removal else {
        return true;
    };

    if removed.device.is_some() {
        log::info!("Deregistering device {}", id);
        if let Err(error) = deregister(id.to_owned()).await {
            log::error!("Unable to deregister device {}: {}", id, error);
        }
    }

    if removed.cleanup_pending {
        sessions.write().await.finish_cleanup(id, &removed.token);
    }
    drop(removed.output);
    true
}

async fn disconnect_matching(id: &str, expected: SessionMatch<'_>) -> bool {
    disconnect_matching_with(&SESSIONS, id, expected, deregister_opendeck_device).await
}

async fn disconnect_session(id: &str, expected_token: &Arc<CancellationToken>) -> bool {
    disconnect_matching(id, SessionMatch::Token(expected_token)).await
}

pub async fn disconnect_generation(id: &str, generation: u64) -> bool {
    disconnect_matching(id, SessionMatch::Generation(generation)).await
}

fn is_nonfatal_error(err: &MirajazzError) -> bool {
    matches!(err, MirajazzError::ImageError(_) | MirajazzError::BadData)
}

fn schedule_flush(flush_deadline: &mut Option<Instant>) {
    flush_deadline.get_or_insert_with(|| Instant::now() + Duration::from_millis(50));
}

/// Handles a device error. Image conversion errors are nonfatal; HID and
/// protocol errors discard this connection generation so the watcher can reopen it.
pub async fn handle_error(
    id: &str,
    expected_token: &Arc<CancellationToken>,
    err: MirajazzError,
) -> bool {
    log::error!("Device {} error: {}", id, err);

    if is_nonfatal_error(&err) {
        return true;
    }

    disconnect_session(id, expected_token).await;
    false
}

pub async fn connect(candidate: &CandidateDevice) -> Result<Device, MirajazzError> {
    let result = Device::connect(
        &candidate.dev,
        candidate.kind.protocol_version(),
        KEY_COUNT,
        ENCODER_COUNT,
    )
    .await;

    match result {
        Ok(device) => Ok(device),
        Err(e) => {
            log::error!("Error while connecting to device: {e}");
            Err(e)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InputEvent {
    KeyDown(String, u8),
    KeyUp(String, u8),
    EncoderDown(String, u8),
    EncoderUp(String, u8),
    EncoderChange(String, u8, i16),
}

impl InputEvent {
    fn from_update(id: &str, update: DeviceStateUpdate) -> Self {
        match update {
            DeviceStateUpdate::ButtonDown(key) => Self::KeyDown(id.to_owned(), key),
            DeviceStateUpdate::ButtonUp(key) => Self::KeyUp(id.to_owned(), key),
            DeviceStateUpdate::EncoderDown(encoder) => Self::EncoderDown(id.to_owned(), encoder),
            DeviceStateUpdate::EncoderUp(encoder) => Self::EncoderUp(id.to_owned(), encoder),
            DeviceStateUpdate::EncoderTwist(encoder, value) => {
                Self::EncoderChange(id.to_owned(), encoder, value as i16)
            }
        }
    }
}

async fn send_opendeck_input(event: InputEvent) -> Result<(), String> {
    let mut manager = OUTBOUND_EVENT_MANAGER.lock().await;
    let outbound = manager
        .as_mut()
        .ok_or_else(|| "OpenDeck outbound connection is unavailable".to_string())?;

    let result = match event {
        InputEvent::KeyDown(id, key) => outbound.key_down(id, key).await,
        InputEvent::KeyUp(id, key) => outbound.key_up(id, key).await,
        InputEvent::EncoderDown(id, encoder) => outbound.encoder_down(id, encoder).await,
        InputEvent::EncoderUp(id, encoder) => outbound.encoder_up(id, encoder).await,
        InputEvent::EncoderChange(id, encoder, value) => {
            outbound.encoder_change(id, encoder, value).await
        }
    };

    result.map_err(|error| error.to_string())
}

async fn deliver_input_with<D, O, S, SendFuture, R, DeregisterFuture>(
    sessions: &RwLock<SessionRegistry<D, O>>,
    id: &str,
    token: &Arc<CancellationToken>,
    event: InputEvent,
    send: S,
    deregister: R,
) -> bool
where
    S: FnOnce(InputEvent) -> SendFuture,
    SendFuture: Future<Output = Result<(), String>>,
    R: FnOnce(String) -> DeregisterFuture,
    DeregisterFuture: Future<Output = Result<(), String>>,
{
    if let Err(error) = send(event).await {
        log::error!("Unable to deliver input event for device {}: {}", id, error);
        disconnect_matching_with(sessions, id, SessionMatch::Token(token), deregister).await;
        return false;
    }

    true
}

/// Handles events from device to OpenDeck
async fn device_events_task(
    candidate: CandidateDevice,
    device: Arc<Device>,
    token: Arc<CancellationToken>,
) -> Result<(), MirajazzError> {
    log::info!("Connecting to {} for incoming events", candidate.id);
    let reader = device.get_reader(crate::inputs::process_input);

    log::info!("Connected to {} for incoming events", candidate.id);
    log::info!("Reader is ready for {}", candidate.id);

    loop {
        log::info!("Reading updates...");

        let updates = match reader.read(None).await {
            Ok(updates) => updates,
            Err(e) => {
                if !handle_error(&candidate.id, &token, e).await {
                    break;
                }
                continue;
            }
        };

        if token.is_cancelled() {
            break;
        }

        for update in updates {
            if token.is_cancelled() {
                return Ok(());
            }

            log::info!("New update: {:#?}", update);
            let event = InputEvent::from_update(&candidate.id, update);

            if !deliver_input_with(
                &SESSIONS,
                &candidate.id,
                &token,
                event,
                send_opendeck_input,
                deregister_opendeck_device,
            )
            .await
            {
                return Ok(());
            }
        }
    }

    Ok(())
}

enum OutputAction {
    Command(Option<DeviceCommand>),
    Flush,
    KeepAlive,
    Cancel,
}

enum OutputStep {
    Continue,
    ScheduleFlush,
    ClearFlush,
    Stop,
}

trait OutputDevice: Send + Sync {
    async fn set_button_image(
        &self,
        key: u8,
        format: mirajazz::types::ImageFormat,
        image: DynamicImage,
    ) -> Result<(), MirajazzError>;
    async fn clear_button_image(&self, key: u8) -> Result<(), MirajazzError>;
    async fn clear_all_button_images(&self) -> Result<(), MirajazzError>;
    async fn set_brightness(&self, brightness: u8) -> Result<(), MirajazzError>;
    async fn set_led_colors(&self, colors: &[[u8; 3]]) -> Result<(), MirajazzError>;
    async fn flush(&self) -> Result<(), MirajazzError>;
    async fn keep_alive(&self) -> Result<(), MirajazzError>;
}

impl OutputDevice for Device {
    async fn set_button_image(
        &self,
        key: u8,
        format: mirajazz::types::ImageFormat,
        image: DynamicImage,
    ) -> Result<(), MirajazzError> {
        Device::set_button_image(self, key, format, image).await
    }

    async fn clear_button_image(&self, key: u8) -> Result<(), MirajazzError> {
        Device::clear_button_image(self, key).await
    }

    async fn clear_all_button_images(&self) -> Result<(), MirajazzError> {
        Device::clear_all_button_images(self).await
    }

    async fn set_brightness(&self, brightness: u8) -> Result<(), MirajazzError> {
        Device::set_brightness(self, brightness).await
    }

    async fn set_led_colors(&self, colors: &[[u8; 3]]) -> Result<(), MirajazzError> {
        Device::set_led_colors(self, colors).await
    }

    async fn flush(&self) -> Result<(), MirajazzError> {
        Device::flush(self).await
    }

    async fn keep_alive(&self) -> Result<(), MirajazzError> {
        Device::keep_alive(self).await
    }
}

/// Owns all active-session HID writes for one device. The select only chooses
/// the next action; each HID future is awaited afterward, where cancellation
/// cannot drop it halfway through an overlapped Windows write.
async fn device_output_task<D: OutputDevice + 'static>(
    id: String,
    kind: Kind,
    device: Arc<D>,
    mut receiver: mpsc::Receiver<DeviceCommand>,
    token: Arc<CancellationToken>,
) -> Result<(), MirajazzError> {
    let mut keepalive = interval(Duration::from_secs(10));
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Skip);
    keepalive.tick().await;

    let mut flush_deadline: Option<Instant> = None;

    loop {
        let flush_at =
            flush_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(24 * 60 * 60));

        let action = tokio::select! {
            biased;
            _ = token.cancelled() => OutputAction::Cancel,
            _ = sleep_until(flush_at), if flush_deadline.is_some() => OutputAction::Flush,
            command = receiver.recv() => OutputAction::Command(command),
            _ = keepalive.tick() => OutputAction::KeepAlive,
        };

        let result = match action {
            OutputAction::Command(Some(DeviceCommand::SetImage { position, image })) => {
                log::info!("Setting image for button {}", position);
                device
                    .set_button_image(
                        opendeck_to_device(position),
                        get_image_format_for_key(&kind, position),
                        image,
                    )
                    .await
                    .map(|_| OutputStep::ScheduleFlush)
            }
            OutputAction::Command(Some(DeviceCommand::ClearImage(position))) => device
                .clear_button_image(opendeck_to_device(position))
                .await
                .map(|_| OutputStep::ScheduleFlush),
            OutputAction::Command(Some(DeviceCommand::ClearAll)) => {
                match device.clear_all_button_images().await {
                    Ok(()) => device.flush().await.map(|_| OutputStep::ClearFlush),
                    Err(err) => Err(err),
                }
            }
            OutputAction::Command(Some(DeviceCommand::SetBrightness(brightness))) => device
                .set_brightness(brightness)
                .await
                .map(|_| OutputStep::Continue),
            OutputAction::Command(Some(DeviceCommand::SetLedColors(colors))) => device
                .set_led_colors(&colors)
                .await
                .map(|_| OutputStep::Continue),
            OutputAction::Command(None) | OutputAction::Cancel => Ok(OutputStep::Stop),
            OutputAction::Flush => {
                log::debug!("Flushing pending updates for {}", id);
                device.flush().await.map(|_| OutputStep::ClearFlush)
            }
            OutputAction::KeepAlive => {
                log::info!("Sending keepalive to {}", id);
                device.keep_alive().await.map(|_| OutputStep::Continue)
            }
        };

        match result {
            Ok(OutputStep::Continue) => {}
            Ok(OutputStep::ScheduleFlush) => {
                // Anchor the batch to its first update. A continuous image stream
                // can no longer postpone flushing forever.
                schedule_flush(&mut flush_deadline);
            }
            Ok(OutputStep::ClearFlush) => flush_deadline = None,
            Ok(OutputStep::Stop) => return Ok(()),
            Err(err) if is_nonfatal_error(&err) => {
                log::error!("Device {} nonfatal output error: {}", id, err);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
}

/// Handles different combinations of "set image" event, including clearing the specific buttons and whole device
pub fn command_for_set_image(evt: SetImageEvent) -> Result<Option<DeviceCommand>, MirajazzError> {
    match (evt.position, evt.image) {
        (Some(position), Some(image)) => {
            let url = DataUrl::process(image.as_str()).unwrap();
            let (body, _fragment) = url.decode_to_vec().unwrap();

            if url.mime_type().subtype != "jpeg" {
                log::error!("Incorrect mime type: {}", url.mime_type());
                return Ok(None);
            }

            let image = load_from_memory_with_format(body.as_slice(), image::ImageFormat::Jpeg)?;
            Ok(Some(DeviceCommand::SetImage { position, image }))
        }
        (Some(position), None) => Ok(Some(DeviceCommand::ClearImage(position))),
        (None, None) => Ok(Some(DeviceCommand::ClearAll)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::{Notify, RwLock};

    use super::*;

    #[derive(Default)]
    struct RecordingDevice {
        operations: StdMutex<Vec<&'static str>>,
        changed: Notify,
    }

    impl RecordingDevice {
        async fn operation(&self, start: &'static str, end: &'static str) {
            self.operations.lock().unwrap().push(start);
            self.changed.notify_waiters();
            tokio::task::yield_now().await;
            self.operations.lock().unwrap().push(end);
            self.changed.notify_waiters();
        }

        async fn wait_for(&self, operation: &str) {
            loop {
                let changed = self.changed.notified();
                if self.operations.lock().unwrap().contains(&operation) {
                    return;
                }
                changed.await;
            }
        }
    }

    impl OutputDevice for RecordingDevice {
        async fn set_button_image(
            &self,
            _key: u8,
            _format: mirajazz::types::ImageFormat,
            _image: DynamicImage,
        ) -> Result<(), MirajazzError> {
            self.operation("image:start", "image:end").await;
            Ok(())
        }

        async fn clear_button_image(&self, _key: u8) -> Result<(), MirajazzError> {
            self.operation("clear:start", "clear:end").await;
            Ok(())
        }

        async fn clear_all_button_images(&self) -> Result<(), MirajazzError> {
            self.operation("clear-all:start", "clear-all:end").await;
            Ok(())
        }

        async fn set_brightness(&self, _brightness: u8) -> Result<(), MirajazzError> {
            self.operation("brightness:start", "brightness:end").await;
            Ok(())
        }

        async fn set_led_colors(&self, _colors: &[[u8; 3]]) -> Result<(), MirajazzError> {
            self.operation("led:start", "led:end").await;
            Ok(())
        }

        async fn flush(&self) -> Result<(), MirajazzError> {
            self.operation("flush:start", "flush:end").await;
            Ok(())
        }

        async fn keep_alive(&self) -> Result<(), MirajazzError> {
            self.operation("keepalive:start", "keepalive:end").await;
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn led_and_existing_output_operations_do_not_interleave() {
        let device = Arc::new(RecordingDevice::default());
        let (sender, receiver) = mpsc::channel(8);
        let token = Arc::new(CancellationToken::new());
        let worker = tokio::spawn(device_output_task(
            "test-device".to_owned(),
            Kind::MiraboxM18,
            device.clone(),
            receiver,
            token.clone(),
        ));

        sender
            .send(DeviceCommand::SetImage {
                position: 0,
                image: DynamicImage::new_rgb8(1, 1),
            })
            .await
            .unwrap();
        sender.send(DeviceCommand::ClearImage(1)).await.unwrap();
        sender
            .send(DeviceCommand::SetLedColors([[1, 2, 3]; 24]))
            .await
            .unwrap();
        sender.send(DeviceCommand::SetBrightness(75)).await.unwrap();
        sender.send(DeviceCommand::ClearAll).await.unwrap();
        device.wait_for("flush:end").await;

        tokio::time::advance(Duration::from_secs(10)).await;
        device.wait_for("keepalive:end").await;

        token.cancel();
        drop(sender);

        worker.await.unwrap().unwrap();

        assert_eq!(
            *device.operations.lock().unwrap(),
            [
                "image:start",
                "image:end",
                "clear:start",
                "clear:end",
                "led:start",
                "led:end",
                "brightness:start",
                "brightness:end",
                "clear-all:start",
                "clear-all:end",
                "flush:start",
                "flush:end",
                "keepalive:start",
                "keepalive:end",
            ]
        );
    }

    #[tokio::test]
    async fn registration_failure_rolls_back_the_session() {
        let sessions = RwLock::new(SessionRegistry::<&str, &str>::default());
        let token = sessions
            .write()
            .await
            .reserve("device".to_string(), 1)
            .unwrap();

        let outcome = publish_with(
            &sessions,
            "device",
            "device handle",
            "output handle",
            &token,
            || async { Err("connection closed".to_string()) },
            |_| async { Ok(()) },
        )
        .await;

        assert_eq!(
            outcome,
            PublicationOutcome::RegistrationFailed("connection closed".to_string())
        );
        assert!(token.is_cancelled());
        let sessions = sessions.read().await;
        assert!(!sessions.is_current("device", &token));
        assert!(sessions.output("device").is_none());
    }

    #[tokio::test]
    async fn disconnect_during_registration_rolls_back_before_replacement() {
        let sessions = RwLock::new(SessionRegistry::<&str, &str>::default());
        let token = sessions
            .write()
            .await
            .reserve("device".to_string(), 1)
            .unwrap();
        let deregistrations = Arc::new(AtomicUsize::new(0));
        let deregistrations_for_cleanup = deregistrations.clone();

        let outcome = publish_with(
            &sessions,
            "device",
            "device handle",
            "output handle",
            &token,
            || async {
                assert_eq!(
                    sessions.read().await.output("device"),
                    Some(&"output handle")
                );
                assert!(matches!(
                    sessions
                        .write()
                        .await
                        .begin_removal("device", SessionMatch::Generation(1)),
                    Some(Removal::RegistrationPending)
                ));
                assert!(
                    sessions
                        .write()
                        .await
                        .reserve("device".to_string(), 2)
                        .is_none()
                );
                Ok(())
            },
            move |_| async move {
                deregistrations_for_cleanup.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )
        .await;

        assert_eq!(outcome, PublicationOutcome::Stale);
        assert!(token.is_cancelled());
        assert_eq!(deregistrations.load(Ordering::Relaxed), 1);
        assert!(
            sessions
                .write()
                .await
                .reserve("device".to_string(), 2)
                .is_some()
        );
    }

    #[tokio::test]
    async fn deregistration_runs_without_holding_the_session_lock() {
        let sessions = RwLock::new(SessionRegistry::<&str, &str>::default());
        let token = sessions
            .write()
            .await
            .reserve("device".to_string(), 1)
            .unwrap();
        assert_eq!(
            publish_with(
                &sessions,
                "device",
                "device handle",
                "output handle",
                &token,
                || async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await,
            PublicationOutcome::Published
        );

        assert!(
            disconnect_matching_with(
                &sessions,
                "device",
                SessionMatch::Token(&token),
                |_| async {
                    assert!(sessions.read().await.output("device").is_none());
                    Ok(())
                },
            )
            .await
        );
        assert!(!sessions.read().await.is_current("device", &token));
    }

    #[tokio::test]
    async fn every_input_delivery_failure_removes_the_published_session() {
        let cases = [
            (
                DeviceStateUpdate::ButtonDown(1),
                InputEvent::KeyDown("device".to_string(), 1),
            ),
            (
                DeviceStateUpdate::ButtonUp(2),
                InputEvent::KeyUp("device".to_string(), 2),
            ),
            (
                DeviceStateUpdate::EncoderDown(3),
                InputEvent::EncoderDown("device".to_string(), 3),
            ),
            (
                DeviceStateUpdate::EncoderUp(4),
                InputEvent::EncoderUp("device".to_string(), 4),
            ),
            (
                DeviceStateUpdate::EncoderTwist(5, -2),
                InputEvent::EncoderChange("device".to_string(), 5, -2),
            ),
        ];

        for (update, expected_event) in cases {
            let sessions = RwLock::new(SessionRegistry::<&str, &str>::default());
            let token = sessions
                .write()
                .await
                .reserve("device".to_string(), 1)
                .unwrap();
            assert_eq!(
                publish_with(
                    &sessions,
                    "device",
                    "device handle",
                    "output handle",
                    &token,
                    || async { Ok(()) },
                    |_| async { Ok(()) },
                )
                .await,
                PublicationOutcome::Published
            );

            let deregistrations = Arc::new(AtomicUsize::new(0));
            let deregistrations_for_cleanup = deregistrations.clone();
            let delivered = deliver_input_with(
                &sessions,
                "device",
                &token,
                InputEvent::from_update("device", update),
                move |event| async move {
                    assert_eq!(event, expected_event);
                    Err("connection closed".to_string())
                },
                move |id| async move {
                    assert_eq!(id, "device");
                    deregistrations_for_cleanup.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .await;

            assert!(!delivered);
            assert!(token.is_cancelled());
            assert_eq!(deregistrations.load(Ordering::Relaxed), 1);
            let sessions = sessions.read().await;
            assert!(!sessions.is_current("device", &token));
            assert!(sessions.output("device").is_none());
        }
    }

    #[tokio::test]
    async fn stale_generation_cleanup_cannot_remove_a_replacement() {
        let sessions = RwLock::new(SessionRegistry::<&str, &str>::default());
        let old_token = sessions
            .write()
            .await
            .reserve("device".to_string(), 1)
            .unwrap();
        assert_eq!(
            publish_with(
                &sessions,
                "device",
                "old device",
                "old output",
                &old_token,
                || async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await,
            PublicationOutcome::Published
        );
        assert!(
            disconnect_matching_with(
                &sessions,
                "device",
                SessionMatch::Generation(1),
                |_| async { Ok(()) },
            )
            .await
        );

        let replacement_token = sessions
            .write()
            .await
            .reserve("device".to_string(), 2)
            .unwrap();
        assert_eq!(
            publish_with(
                &sessions,
                "device",
                "replacement device",
                "replacement output",
                &replacement_token,
                || async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await,
            PublicationOutcome::Published
        );
        let deregistrations = Arc::new(AtomicUsize::new(0));
        let deregistrations_for_cleanup = deregistrations.clone();

        assert!(
            !disconnect_matching_with(
                &sessions,
                "device",
                SessionMatch::Generation(1),
                move |_| async move {
                    deregistrations_for_cleanup.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .await
        );
        assert_eq!(deregistrations.load(Ordering::Relaxed), 0);
        assert!(!replacement_token.is_cancelled());
        let sessions = sessions.read().await;
        assert!(sessions.is_current("device", &replacement_token));
        assert_eq!(sessions.output("device"), Some(&"replacement output"));
    }
}
