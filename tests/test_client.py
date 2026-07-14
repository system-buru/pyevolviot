"""Test the EvolvIOT API client."""

import asyncio
import json
from collections.abc import Callable
from typing import Any, Self, cast
from unittest.mock import AsyncMock, Mock, patch

from aiohttp import ClientError, ClientResponseError, ClientSession, WSMsgType
import pytest

from pyevolviot.client import (
    EvolvIOTApi,
    EvolvIOTAuthError,
    EvolvIOTData,
    EvolvIOTLocalCommand,
    EvolvIOTReadyEvent,
    EvolvIOTStateChangedEvent,
    EvolvIOTWebSocket,
    EvolvIOTConnectionError,
    EvolvIOTDeviceAuthorizationDenied,
    EvolvIOTDeviceAuthorizationExpired,
    EvolvIOTDeviceAuthorizationPending,
    EvolvIOTWebSocketError,
    _local_status_headers,
    _sanitize_device_id_for_mdns,
    normalize_api_base_url,
    websocket_url_from_api_base_url,
)


class MockResponse:
    """Mock aiohttp response."""

    def __init__(
        self,
        *,
        status: int = 200,
        payload: Any | None = None,
        text: str = "",
    ) -> None:
        """Initialize the response."""
        self.status = status
        self.payload = payload if payload is not None else {}
        self._text = text

    async def __aenter__(self) -> Self:
        """Enter context manager."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Exit context manager."""

    def raise_for_status(self) -> None:
        """Raise for HTTP errors."""
        if self.status >= 400:
            raise ClientResponseError(Mock(), (), status=self.status)

    async def json(self, *, content_type: str | None = None) -> Any:
        """Return JSON payload."""
        return self.payload

    async def text(self) -> str:
        """Return text payload."""
        return self._text


class MockWSMessage:
    """Mock aiohttp WebSocket message."""

    def __init__(self, payload: Any, message_type: WSMsgType = WSMsgType.TEXT) -> None:
        """Initialize the message."""
        self.type = message_type
        self.data = json.dumps(payload) if message_type == WSMsgType.TEXT else ""


class MockWebSocket:
    """Mock aiohttp WebSocket."""

    def __init__(self) -> None:
        """Initialize the WebSocket."""
        self.closed = False
        self.messages: asyncio.Queue[MockWSMessage] = asyncio.Queue()
        self.sent_json: list[dict[str, Any]] = []

    def __aiter__(self) -> Self:
        """Return the async iterator."""
        return self

    async def __anext__(self) -> MockWSMessage:
        """Return the next WebSocket message."""
        message = await self.messages.get()
        if message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
            self.closed = True
            raise StopAsyncIteration
        return message

    def feed_json(self, payload: dict[str, Any]) -> None:
        """Feed a JSON message to the client."""
        self.messages.put_nowait(MockWSMessage(payload))

    def feed_close(self) -> None:
        """Feed a close message to the client."""
        self.messages.put_nowait(MockWSMessage({}, WSMsgType.CLOSE))

    async def send_json(self, payload: dict[str, Any]) -> None:
        """Record an outbound JSON message."""
        self.sent_json.append(payload)

    async def close(self) -> None:
        """Close the WebSocket."""
        if not self.closed:
            self.closed = True
            self.feed_close()


class MockSession:
    """Mock aiohttp session."""

    def __init__(
        self,
        *responses: MockResponse,
        websockets: list[MockWebSocket | Exception] | None = None,
    ) -> None:
        """Initialize the session."""
        self.responses = list(responses)
        self.websocket_results = list(websockets or [])
        self.get_calls: list[tuple[str, dict[str, Any]]] = []
        self.post_calls: list[tuple[str, dict[str, Any]]] = []
        self.request_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.ws_connect_calls: list[tuple[str, dict[str, Any]]] = []

    def _response(self) -> MockResponse:
        """Return the next response."""
        return self.responses.pop(0)

    def get(self, url: str, **kwargs: Any) -> MockResponse:
        """Mock GET."""
        self.get_calls.append((url, kwargs))
        return self._response()

    def post(self, url: str, **kwargs: Any) -> MockResponse:
        """Mock POST."""
        self.post_calls.append((url, kwargs))
        return self._response()

    def request(self, method: str, url: str, **kwargs: Any) -> MockResponse:
        """Mock request."""
        self.request_calls.append((method, url, kwargs))
        return self._response()

    async def ws_connect(self, url: str, **kwargs: Any) -> MockWebSocket:
        """Mock WebSocket connection."""
        self.ws_connect_calls.append((url, kwargs))
        result = self.websocket_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class ErrorSession(MockSession):
    """Mock session raising client errors."""

    def get(self, url: str, **kwargs: Any) -> MockResponse:
        """Mock GET failure."""
        raise ClientError

    def post(self, url: str, **kwargs: Any) -> MockResponse:
        """Mock POST failure."""
        raise ClientError

    def request(self, method: str, url: str, **kwargs: Any) -> MockResponse:
        """Mock request failure."""
        raise ClientError

    async def ws_connect(self, url: str, **kwargs: Any) -> MockWebSocket:
        """Mock WebSocket connection failure."""
        raise ClientError


async def _async_wait_for(condition: Callable[[], bool]) -> None:
    """Wait until a test condition becomes true."""
    for _ in range(10):
        if condition():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition was not met")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "https://api.evolviot.com/api/homeassistant"),
        ("https://example.com", "https://example.com/api/homeassistant"),
        ("https://example.com/api", "https://example.com/api/homeassistant"),
        (
            "https://example.com/api/homeassistant",
            "https://example.com/api/homeassistant",
        ),
    ],
)
def test_normalize_api_base_url(value: str | None, expected: str) -> None:
    """Test API base URL normalization."""
    assert normalize_api_base_url(value) == expected


@pytest.mark.parametrize(
    ("api_base_url", "token", "expected"),
    [
        (
            "https://example.com/api/homeassistant",
            "token value",
            "wss://example.com/homeassistant-ws?access_token=token+value",
        ),
        (
            "http://localhost:3000/api/homeassistant",
            "token",
            "ws://localhost:3000/homeassistant-ws?access_token=token",
        ),
    ],
)
def test_websocket_url_from_api_base_url(
    api_base_url: str,
    token: str,
    expected: str,
) -> None:
    """Test WebSocket URL generation."""
    assert websocket_url_from_api_base_url(api_base_url, token) == expected


def test_local_helpers() -> None:
    """Test local helper values."""
    assert _sanitize_device_id_for_mdns("Device_01!") == "device-01-"

    with (
        patch("pyevolviot.client.time.time", return_value=1000),
        patch(
            "pyevolviot.client.os.urandom",
            return_value=b"1" * 12,
        ),
    ):
        headers = _local_status_headers("uid", "device", "secret")

    assert headers["X-Evolv-Timestamp"] == "1000"
    assert headers["X-Evolv-Nonce"]
    assert headers["X-Evolv-Signature"]


def test_typed_data_models_normalize_payloads() -> None:
    """Test typed models normalize API payload variants."""
    data = EvolvIOTData.from_payload(
        {
            "userId": "user-123",
            "entities": [
                {
                    "entityId": "switch.evolviot_switch",
                    "uniqueId": "SWITCH123/power",
                    "name": "Living Room Switch",
                    "control": {"key": "Main Power"},
                    "device": {
                        "id": "SWITCH123",
                        "name": "Living Room",
                        "localControl": {
                            "deviceSecret": "secret",
                            "endpoint": "control",
                        },
                    },
                }
            ],
            "states": [
                {
                    "entityId": "switch.evolviot_switch",
                    "rawValue": "true",
                }
            ],
        }
    )

    entity = data.entities["switch.evolviot_switch"]
    state = data.states["switch.evolviot_switch"]

    assert data.user_id == "user-123"
    assert entity.domain == "switch"
    assert entity.unique_id == "SWITCH123/power"
    assert state.state == "on"
    assert state.is_on
    assert entity.local_command("user-123", {"command": "turn_on"}) == (
        EvolvIOTLocalCommand(
            uid="user-123",
            device_id="SWITCH123",
            endpoint="Main Power",
            device_secret="secret",
            switch_name="Main Power",
            value=1,
        )
    )
    assert entity.local_value_from_status({"mainpower": 0}) == 0
    assert entity.state_from_local_status({"mainpower": 0}, base_state=state).state == (
        "off"
    )


async def test_health_and_validate() -> None:
    """Test health and validation requests."""
    session = MockSession(
        MockResponse(text="ok"),
        MockResponse(payload={"entities": []}),
    )
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com", "token")

    assert await api.async_validate() == {"entities": []}
    assert session.get_calls[0] == ("https://api.evolviot.com/health", {"ssl": True})
    assert session.request_calls[0][0:2] == (
        "GET",
        "https://example.com/api/homeassistant/devices",
    )


async def test_get_typed_data() -> None:
    """Test fetching typed account data over HTTP."""
    session = MockSession(
        MockResponse(
            payload={
                "user_id": "user-123",
                "entities": [
                    {
                        "entity_id": "switch.evolviot_switch",
                        "domain": "switch",
                        "name": "Switch",
                    }
                ],
            }
        ),
        MockResponse(
            payload={
                "states": [
                    {
                        "entity_id": "switch.evolviot_switch",
                        "state": "off",
                    }
                ]
            }
        ),
    )
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com", "token")

    data = await api.async_get_data()

    assert data.user_id == "user-123"
    assert data.entities["switch.evolviot_switch"].name == "Switch"
    assert data.states["switch.evolviot_switch"].state == "off"


async def test_health_connection_error() -> None:
    """Test health connection errors."""
    api = EvolvIOTApi(cast(ClientSession, ErrorSession()), "https://example.com")

    with pytest.raises(EvolvIOTConnectionError):
        await api.async_health()


async def test_authorization_code_exchange() -> None:
    """Test OAuth authorization code exchange."""
    session = MockSession(MockResponse(payload={"access_token": "token"}))
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com")

    assert await api.async_exchange_authorization_code("code", "id", "secret") == {
        "access_token": "token"
    }
    assert session.post_calls[0][1]["data"] == {
        "grant_type": "authorization_code",
        "code": "code",
        "client_id": "id",
        "client_secret": "secret",
    }


@pytest.mark.parametrize(
    ("response", "exception"),
    [
        (MockResponse(status=401), EvolvIOTAuthError),
        (MockResponse(payload={}), EvolvIOTAuthError),
    ],
)
async def test_authorization_code_exchange_errors(
    response: MockResponse,
    exception: type[Exception],
) -> None:
    """Test OAuth authorization code exchange errors."""
    api = EvolvIOTApi(cast(ClientSession, MockSession(response)), "https://example.com")

    with pytest.raises(exception):
        await api.async_exchange_authorization_code("code", "id", "secret")


async def test_start_device_authorization() -> None:
    """Test device authorization start."""
    api = EvolvIOTApi(
        cast(ClientSession, MockSession(MockResponse(payload={"device_code": "abc"}))),
        "https://example.com",
    )

    assert await api.async_start_device_authorization() == {"device_code": "abc"}


@pytest.mark.parametrize(
    ("error", "exception"),
    [
        ("authorization_pending", EvolvIOTDeviceAuthorizationPending),
        ("slow_down", EvolvIOTDeviceAuthorizationPending),
        ("access_denied", EvolvIOTDeviceAuthorizationDenied),
        ("expired_token", EvolvIOTDeviceAuthorizationExpired),
        ("invalid_grant", EvolvIOTDeviceAuthorizationExpired),
        ("other", EvolvIOTAuthError),
    ],
)
async def test_device_code_exchange_errors(
    error: str,
    exception: type[Exception],
) -> None:
    """Test device code exchange errors."""
    api = EvolvIOTApi(
        cast(
            ClientSession,
            MockSession(MockResponse(status=400, payload={"error": error})),
        ),
        "https://example.com",
    )

    with pytest.raises(exception):
        await api.async_exchange_device_code("device-code")


async def test_device_code_exchange_success() -> None:
    """Test device code exchange success."""
    api = EvolvIOTApi(
        cast(
            ClientSession, MockSession(MockResponse(payload={"access_token": "token"}))
        ),
        "https://example.com",
    )

    assert await api.async_exchange_device_code("device-code") == {
        "access_token": "token"
    }


async def test_entity_requests_quote_entity_id() -> None:
    """Test entity ID quoting for state and command requests."""
    session = MockSession(MockResponse(payload={}), MockResponse(payload={}))
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com", "token")

    await api.async_get_state("switch.device/one")
    await api.async_command("switch.device/one", {"command": "turn_on"})

    assert session.request_calls[0][1].endswith("/devices/switch.device%2Fone/state")
    assert session.request_calls[1][1].endswith("/devices/switch.device%2Fone/command")


async def test_get_typed_state_accepts_response_shapes() -> None:
    """Test typed single state parsing handles raw and nested responses."""
    session = MockSession(
        MockResponse(
            payload={
                "entity_id": "switch.raw",
                "state": "on",
            }
        ),
        MockResponse(
            payload={
                "state": {
                    "entity_id": "switch.nested",
                    "raw_value": 0,
                }
            }
        ),
    )
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com", "token")

    raw_state = await api.async_get_typed_state("switch.raw")
    nested_state = await api.async_get_typed_state("switch.nested")

    assert raw_state.state == "on"
    assert nested_state.state == "off"


async def test_request_refreshes_token() -> None:
    """Test token refresh and retry."""
    token_callback = AsyncMock()
    session = MockSession(
        MockResponse(status=401),
        MockResponse(payload={"access_token": "new", "refresh_token": "new-refresh"}),
        MockResponse(payload={"entities": []}),
    )
    api = EvolvIOTApi(
        cast(ClientSession, session),
        "https://example.com",
        "old",
        refresh_token="refresh",
        client_id="client",
        client_secret="secret",
        token_update_callback=token_callback,
    )

    assert await api.async_get_devices() == {"entities": []}
    assert api.access_token == "new"
    token_callback.assert_awaited_once_with(
        {"access_token": "new", "refresh_token": "new-refresh"}
    )


async def test_request_auth_error_without_refresh() -> None:
    """Test request auth error without refresh token."""
    api = EvolvIOTApi(
        cast(ClientSession, MockSession(MockResponse(status=401))),
        "https://example.com",
        "old",
    )

    with pytest.raises(EvolvIOTAuthError):
        await api.async_get_devices()


async def test_request_connection_error() -> None:
    """Test request connection error."""
    api = EvolvIOTApi(cast(ClientSession, ErrorSession()), "https://example.com")

    with pytest.raises(EvolvIOTConnectionError):
        await api.async_get_devices()


async def test_local_command_and_status() -> None:
    """Test local command and status calls."""
    session = MockSession(
        MockResponse(text="ok"),
        MockResponse(payload={"power": 1}),
    )
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com")

    with patch(
        "pyevolviot.client.os.urandom",
        return_value=b"1" * 16,
    ):
        await api.async_local_command(
            uid="uid",
            device_id="Device_01",
            endpoint="/control",
            device_secret="secret",
            switch_name="power",
            value=1,
        )

    assert session.post_calls[0][0] == "http://evolviot-device-01.local/control"
    assert set(session.post_calls[0][1]["json"]) == {"data", "hmac"}

    assert await api.async_local_status(
        uid="uid",
        device_id="Device_01",
        device_secret="secret",
    ) == {"power": 1}
    assert session.get_calls[0][0] == "http://evolviot-device-01.local/status"


async def test_websocket_ready_and_state_changed() -> None:
    """Test WebSocket ready and push state handling."""
    ws = MockWebSocket()
    ws.feed_json(
        {
            "type": "ready",
            "user_id": "user-123",
            "entities": [
                {
                    "entity_id": "switch.evolviot_switch",
                    "domain": "switch",
                    "name": "Switch",
                }
            ],
            "states": [
                {
                    "entity_id": "switch.evolviot_switch",
                    "state": "off",
                }
            ],
        }
    )
    session = MockSession(websockets=[ws])
    api = EvolvIOTApi(cast(ClientSession, session), "https://example.com", "token")
    websocket = EvolvIOTWebSocket(api)
    events = []
    websocket.async_add_listener(events.append)

    data = await websocket.async_connect()

    assert data.user_id == "user-123"
    assert data.states["switch.evolviot_switch"].state == "off"
    assert isinstance(events[0], EvolvIOTReadyEvent)
    assert session.ws_connect_calls[0] == (
        "wss://example.com/homeassistant-ws?access_token=token",
        {"ssl": True},
    )

    ws.feed_json(
        {
            "type": "state_changed",
            "state": {
                "entity_id": "switch.evolviot_switch",
                "state": "on",
                "raw_value": 1,
            },
        }
    )
    await _async_wait_for(
        lambda: websocket.data.states["switch.evolviot_switch"].state == "on"
    )

    assert isinstance(events[-1], EvolvIOTStateChangedEvent)
    await websocket.async_close()


async def test_websocket_command() -> None:
    """Test sending commands over the WebSocket."""
    ws = MockWebSocket()
    ws.feed_json({"type": "ready", "entities": [], "states": []})
    api = EvolvIOTApi(
        cast(ClientSession, MockSession(websockets=[ws])),
        "https://example.com",
        "token",
    )
    websocket = await api.async_connect_websocket()
    events = []
    websocket.async_add_listener(events.append)

    command_task = asyncio.create_task(
        websocket.async_command("switch.evolviot_switch", "turn_on")
    )
    await _async_wait_for(lambda: len(ws.sent_json) == 1)
    request = ws.sent_json[0]

    assert request["type"] == "device.command"
    assert request["entity_id"] == "switch.evolviot_switch"
    assert request["command"] == "turn_on"

    ws.feed_json(
        {
            "id": request["id"],
            "type": "command_result",
            "entity_id": "switch.evolviot_switch",
            "command": {"accepted": True, "acked": True, "payload": "100"},
            "state": {
                "entity_id": "switch.evolviot_switch",
                "state": "on",
            },
        }
    )

    result = await command_task

    assert result.accepted
    assert result.acked
    assert result.state.state == "on"
    assert websocket.data.states["switch.evolviot_switch"].state == "on"
    assert isinstance(events[0], EvolvIOTStateChangedEvent)
    await websocket.async_close()


async def test_websocket_error_response() -> None:
    """Test WebSocket error responses."""
    ws = MockWebSocket()
    ws.feed_json({"type": "ready", "entities": [], "states": []})
    websocket = await EvolvIOTApi(
        cast(ClientSession, MockSession(websockets=[ws])),
        "https://example.com",
        "token",
    ).async_connect_websocket()

    command_task = asyncio.create_task(
        websocket.async_command("switch.evolviot_switch", "turn_on")
    )
    await _async_wait_for(lambda: len(ws.sent_json) == 1)
    ws.feed_json(
        {
            "id": ws.sent_json[0]["id"],
            "type": "error",
            "error": "entity_not_found",
            "message": "Entity not found",
        }
    )

    with pytest.raises(EvolvIOTWebSocketError):
        await command_task

    await websocket.async_close()


async def test_websocket_close_before_ready() -> None:
    """Test WebSocket close before ready fails setup."""
    ws = MockWebSocket()
    ws.feed_close()
    api = EvolvIOTApi(
        cast(ClientSession, MockSession(websockets=[ws])),
        "https://example.com",
        "token",
    )

    with pytest.raises(EvolvIOTConnectionError):
        await api.async_connect_websocket()


async def test_websocket_refreshes_token_on_auth_error() -> None:
    """Test WebSocket auth failures refresh the access token."""
    ws = MockWebSocket()
    ws.feed_json({"type": "ready", "entities": [], "states": []})
    session = MockSession(
        MockResponse(payload={"access_token": "new", "refresh_token": "refresh"}),
        websockets=[ClientResponseError(Mock(), (), status=401), ws],
    )
    api = EvolvIOTApi(
        cast(ClientSession, session),
        "https://example.com",
        "old",
        refresh_token="refresh",
    )

    websocket = await api.async_connect_websocket()

    assert api.access_token == "new"
    assert session.ws_connect_calls[0][0].endswith("access_token=old")
    assert session.ws_connect_calls[1][0].endswith("access_token=new")
    await websocket.async_close()


@pytest.fixture(name="client_error_session")
def client_error_session_fixture() -> ErrorSession:
    """Return a session that raises client errors."""
    return ErrorSession()


async def test_local_command_connection_error(
    client_error_session: ErrorSession,
) -> None:
    """Test local command connection errors."""
    api = EvolvIOTApi(cast(ClientSession, client_error_session), "https://example.com")

    with pytest.raises(EvolvIOTConnectionError):
        await api.async_local_command(
            uid="uid",
            device_id="device",
            endpoint="control",
            device_secret="secret",
            switch_name="power",
            value=1,
        )


async def test_local_status_connection_error(
    client_error_session: ErrorSession,
) -> None:
    """Test local status connection errors."""
    api = EvolvIOTApi(cast(ClientSession, client_error_session), "https://example.com")

    with pytest.raises(EvolvIOTConnectionError):
        await api.async_local_status(
            uid="uid",
            device_id="device",
            device_secret="secret",
        )
