# pyevolviot

Async Python client for the EvolvIOT Home Assistant API.

`pyevolviot` contains the EvolvIOT cloud and local device communication code used by
the Home Assistant EvolvIOT integration. It provides an `aiohttp`-based client for
validating credentials, exchanging OAuth/device authorization tokens, reading device
state, sending cloud commands, and sending signed local device commands.

## Installation

```bash
python -m pip install pyevolviot
```

## Requirements

- Python 3.12 or newer
- `aiohttp`
- `cryptography`

## Usage

```python
from aiohttp import ClientSession

from pyevolviot import EvolvIOTApi


async def main() -> None:
    async with ClientSession() as session:
        api = EvolvIOTApi(
            session,
            "https://api.evolviot.com",
            access_token="ACCESS_TOKEN",
        )

        devices = await api.async_get_devices()
        states = await api.async_get_states()

        print(devices)
        print(states)
```

## API Overview

The main entry point is `pyevolviot.EvolvIOTApi`.

Cloud methods:

- `async_validate()`
- `async_health()`
- `async_get_devices()`
- `async_get_states()`
- `async_get_state(entity_id)`
- `async_command(entity_id, payload)`
- `async_exchange_authorization_code(authorization_code, client_id, client_secret)`
- `async_start_device_authorization()`
- `async_exchange_device_code(device_code)`

Local device methods:

- `async_local_command(...)`
- `async_local_status(...)`

## Development

Create a virtual environment and install the package in editable mode:

```bash
python -m venv .venv
python -m pip install -e . pytest pytest-asyncio ruff build twine
```

Run checks:

```bash
python -m pytest
python -m ruff check src tests
python -m ruff format --check src tests
python -m build
python -m twine check dist/*
```

## License

Apache-2.0
