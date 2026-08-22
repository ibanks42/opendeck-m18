# Changelog

All notable changes to this project will be documented in this file.

## [0.1.4] - 2026-08-22

### Added

- RGB LED color control action
- Per-session button event reporting

### Fixed

- Device lifecycle error handling

## [0.1.3] - 2026-08-21

### Fixed

- Hardened M18 HID reconnect handling to improve device recovery after disconnects

## [0.1.2] - 2026-01-25

### Added

- Keepalive mechanism to prevent device restart during idle periods

## [0.1.1] - 2025-01-13

### Added

- Support for Mirabox M18 device (6603:1009 and 6603:1012)
- Distinct device identification for VSD Inside M18 vs Mirabox M18
- Support for Mirabox M18 EN variant

### Changed

- Clarified that 5548:1000 is VSD Inside M18 (not Mirabox)
- Updated UDEV rules to include both device VID/PIDs

### Fixed

- Corrected Mirabox M18 VID/PID values based on official SDK (was incorrectly using 0416:1000)

## [0.1.0] - 2025-01-12

### Initial Release

- Forked from [opendeck-akp153](https://github.com/4ndv/opendeck-akp153) v0.9.4
- Adapted for VSD Inside M18 device (5548:1000)
- 15 LCD keys (5 columns x 3 rows) + 3 bottom buttons (non-LCD)
- Protocol version 3 (1024-byte packets, supports press/release states)
- 64x64 JPEG images with 180° rotation and both-axis mirroring
- Vertical row flip for image output (device rows are inverted vs OpenDeck)
- Bottom buttons mapped to keys 16-18 in OpenDeck
- Updated DeviceNamespace from "99" to "18"
- Updated plugin identifiers for M18-specific plugin
