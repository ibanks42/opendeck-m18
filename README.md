![Plugin Icon](assets/icon.png)

# OpenDeck VSD Inside M18 / Mirabox M18 Plugin

An unofficial plugin for VSD Inside M18 / Mirabox M18 devices.

**This is a fork of [opendeck-akp153](https://github.com/4ndv/opendeck-akp153) by Andrey Viktorov (st.lynx), adapted for the M18 device.**

## OpenDeck version

Requires OpenDeck 2.5.0 or newer

## Supported devices

- VSD Inside M18 (5548:1000)
- Mirabox M18 (6603:1009)
- Mirabox M18 EN (6603:1012)

## Device Layout

The M18 has:
- 15 LCD keys (5 columns x 3 rows)
- 3 bottom buttons (non-LCD, mapped to keys 16-18 in OpenDeck)

**Note:** OpenDeck displays a 4x5 grid (20 slots), but the M18 only has 18 physical buttons. The last 2 slots in the bottom row are unused and do not correspond to any button on the device.

## RGB LED control

Add the **Set LED Colors** action to a keypad button to configure the M18's 24 RGB LEDs. Its Property Inspector follows the physical LED layout and can set each LED independently or apply one color to all LEDs.

## Platform support

- Linux: Primary development platform
- Mac: Best effort, may need testing
- Windows: Not tested, contributions welcome

## Installation

1. Download an archive from [releases](https://github.com/ibanks42/opendeck-m18/releases)
2. In OpenDeck: Plugins -> Install from file
3. Linux: Download [udev rules](./40-opendeck-m18.rules) and install them by copying into `/etc/udev/rules.d/` and running `sudo udevadm control --reload-rules`
4. Unplug and plug again the device, restart OpenDeck

## Building

### Prerequisites

You'll need:

- A Linux OS of some sort
- Rust 1.87 and up with `x86_64-unknown-linux-gnu` and `x86_64-pc-windows-gnu` targets installed
- Docker
- [just](https://just.systems)

### Building a release package

```sh
$ just package
```

## Acknowledgments

This plugin is a fork of [opendeck-akp153](https://github.com/4ndv/opendeck-akp153) by Andrey Viktorov.

It is also heavily based on work by contributors of [elgato-streamdeck](https://github.com/streamduck-org/elgato-streamdeck) crate.

## License

GPL-3.0 (same as the original plugin)
