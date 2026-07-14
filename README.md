# pyevolviot

Async Python client for the EvolvIOT Home Assistant API.

`pyevolviot` contains the EvolvIOT cloud and local device communication code used by
the Home Assistant EvolvIOT integration. It provides an `aiohttp`-based client for
validating credentials, exchanging OAuth/device authorization tokens, reading typed
device/entity/state models, receiving raw WebSocket state pushes, sending cloud
commands, and sending signed local device commands.

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

        data = await api.async_get_data()

        for entity in data.entities.values():
            print(entity.entity_id, data.states.get(entity.entity_id))
```

## WebSocket Usage

Home Assistant should keep OAuth/device-code pairing on HTTP, then use the raw
`/homeassistant-ws` connection for runtime updates:

```python
from pyevolviot import EvolvIOTStateChangedEvent


async def handle_event(event) -> None:
    if isinstance(event, EvolvIOTStateChangedEvent):
        print(event.state.entity_id, event.state.state)


websocket = await api.async_connect_websocket()
websocket.async_add_listener(handle_event)
await websocket.async_command("switch.evolviot_switch", "turn_on")
```

`EvolvIOTWebSocket.async_run_forever()` can be used by callers that want the
library to reconnect with backoff after unexpected socket closure.

## API Overview

The main entry point is `pyevolviot.EvolvIOTApi`.

Cloud methods:

- `async_validate()`
- `async_validate_data()`
- `async_health()`
- `async_get_devices()`
- `async_get_data()`
- `async_get_states()`
- `async_get_typed_states()`
- `async_get_state(entity_id)`
- `async_get_typed_state(entity_id)`
- `async_command(entity_id, payload)`
- `async_send_command(entity_id, payload)`
- `async_connect_websocket()`
- `async_exchange_authorization_code(authorization_code, client_id, client_secret)`
- `async_start_device_authorization()`
- `async_exchange_device_code(device_code)`

Typed models:

- `EvolvIOTData`
- `EvolvIOTDevice`
- `EvolvIOTEntity`
- `EvolvIOTState`
- `EvolvIOTCommandResult`
- `EvolvIOTWebSocket`

Local device methods:

- `async_local_command(...)`
- `async_local_status(...)`
- `async_local_command_for_entity(...)`

Local command metadata, status key normalization, camelCase/snake_case fallbacks,
and switch-like on/off value coercion are handled by the typed models so Home
Assistant integrations can map model data directly to entities.

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
