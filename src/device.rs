use std::{sync::Arc, time::Duration};

use data_url::DataUrl;
use image::{DynamicImage, load_from_memory_with_format};
use mirajazz::{device::Device, error::MirajazzError, state::DeviceStateUpdate};
use openaction::{OUTBOUND_EVENT_MANAGER, SetImageEvent};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior, interval, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::{
    DEVICES, OUTPUTS, TOKENS,
    inputs::opendeck_to_device,
    mappings::{
        COL_COUNT, CandidateDevice, ENCODER_COUNT, KEY_COUNT, Kind, ROW_COUNT,
        get_image_format_for_key,
    },
    palette::LedPalette,
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

enum SessionMatch<'a> {
    Token(&'a Arc<CancellationToken>),
    Generation(u64),
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

    if !publish_device_if_current(&candidate, &device, &output, &token).await {
        log::debug!("Discarding cancelled connection for {}", candidate.id);
        device.shutdown().await.ok();
        return;
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

async fn publish_device_if_current(
    candidate: &CandidateDevice,
    device: &Arc<Device>,
    output: &Arc<DeviceOutput>,
    token: &Arc<CancellationToken>,
) -> bool {
    // Keep the registration read lock until OpenDeck has accepted the device.
    // A disconnect needs the write lock, so it cannot slip between validation
    // and publication and leave a ghost registration behind.
    let tokens = TOKENS.read().await;
    let token_is_current = tokens
        .get(&candidate.id)
        .is_some_and(|registered| Arc::ptr_eq(&registered.token, token));

    if token.is_cancelled() || !token_is_current {
        return false;
    }

    DEVICES
        .write()
        .await
        .insert(candidate.id.clone(), device.clone());
    OUTPUTS
        .write()
        .await
        .insert(candidate.id.clone(), output.clone());

    log::info!("Registering device {}", candidate.id);
    if let Some(outbound) = OUTBOUND_EVENT_MANAGER.lock().await.as_mut() {
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
            .unwrap();
    }

    true
}

async fn disconnect_matching(id: &str, expected: SessionMatch<'_>) -> bool {
    // Hold the registration write lock through local cleanup and the outbound
    // deregistration. A replacement cannot publish itself until this generation
    // is completely gone.
    let mut tokens = TOKENS.write().await;
    let Some(registered) = tokens.get(id) else {
        return false;
    };

    let matches = match expected {
        SessionMatch::Token(token) => Arc::ptr_eq(&registered.token, token),
        SessionMatch::Generation(generation) => registered.generation == Some(generation),
    };

    if !matches {
        log::debug!("Ignoring stale disconnect from replaced device {}", id);
        return false;
    }

    let token = registered.token.clone();
    token.cancel();

    let removed_output = {
        let mut outputs = OUTPUTS.write().await;
        if outputs
            .get(id)
            .is_some_and(|output| Arc::ptr_eq(&output.token, &token))
        {
            outputs.remove(id)
        } else {
            None
        }
    };

    let removed_device = if removed_output.is_some() {
        DEVICES.write().await.remove(id)
    } else {
        None
    };

    if removed_device.is_some() {
        log::info!("Deregistering device {}", id);
        if let Some(outbound) = OUTBOUND_EVENT_MANAGER.lock().await.as_mut() {
            outbound.deregister_device(id.to_owned()).await.ok();
        }
    }

    tokens.remove(id);
    true
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

        for update in updates {
            log::info!("New update: {:#?}", update);
            let id = candidate.id.clone();

            if let Some(outbound) = OUTBOUND_EVENT_MANAGER.lock().await.as_mut() {
                match update {
                    DeviceStateUpdate::ButtonDown(key) => outbound.key_down(id, key).await.unwrap(),
                    DeviceStateUpdate::ButtonUp(key) => outbound.key_up(id, key).await.unwrap(),
                    DeviceStateUpdate::EncoderDown(encoder) => {
                        outbound.encoder_down(id, encoder).await.unwrap();
                    }
                    DeviceStateUpdate::EncoderUp(encoder) => {
                        outbound.encoder_up(id, encoder).await.unwrap();
                    }
                    DeviceStateUpdate::EncoderTwist(encoder, val) => {
                        outbound
                            .encoder_change(id, encoder, val as i16)
                            .await
                            .unwrap();
                    }
                }
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

    use super::*;
    use tokio::sync::Notify;

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
}
