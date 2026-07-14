"""API client for the EvolvIOT Home Assistant cloud endpoints."""

import asyncio
import base64
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
import hashlib
import hmac
from itertools import count
import json
import logging
import os
import re
import time
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from aiohttp import (
    ClientError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    ClientWebSocketResponse,
    WSMsgType,
)
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

DEFAULT_API_BASE_URL = "https://api.evolviot.com/api/homeassistant"
DEFAULT_HEALTH_URL = "https://api.evolviot.com/health"
DEFAULT_LOCAL_COMMAND_TIMEOUT = 3
DEFAULT_WS_REQUEST_TIMEOUT = 10
HOME_ASSISTANT_WS_PATH = "/homeassistant-ws"
LOCAL_MDNS_DOMAIN = "evolviot"

TokenUpdateCallback = Callable[[dict[str, Any]], Awaitable[None]]
EventCallback = Callable[["EvolvIOTEvent"], Awaitable[None] | None]

_LOGGER = logging.getLogger(__name__)


class EvolvIOTApiError(Exception):
    """Base API error."""


class EvolvIOTAuthError(EvolvIOTApiError):
    """Authentication failed."""


class EvolvIOTConnectionError(EvolvIOTApiError):
    """Connection failed."""


class EvolvIOTDeviceAuthorizationPending(EvolvIOTApiError):
    """Device authorization is still pending."""


class EvolvIOTDeviceAuthorizationDenied(EvolvIOTAuthError):
    """Device authorization was denied."""


class EvolvIOTDeviceAuthorizationExpired(EvolvIOTAuthError):
    """Device authorization expired."""


class EvolvIOTWebSocketError(EvolvIOTApiError):
    """WebSocket protocol error."""

    def __init__(self, error: str, message: str = "") -> None:
        """Initialize the error."""
        super().__init__(message or error)
        self.error = error
        self.message = message or error


@dataclass(slots=True, frozen=True)
class EvolvIOTLocalControl:
    """Local control metadata for an EvolvIOT entity or device."""

    enabled: bool
    device_secret: str
    switch_name: str
    status_key: str
    endpoint: str
    raw: dict[str, Any]

    @classmethod
    def from_payload(
        cls, payload: Mapping[str, Any] | None
    ) -> "EvolvIOTLocalControl | None":
        """Build local control metadata from an API payload."""
        if not payload:
            return None

        raw = dict(payload)
        enabled = bool(_payload_get(raw, "enabled", default=True))
        return cls(
            enabled=enabled,
            device_secret=str(
                _payload_get(raw, "device_secret", "deviceSecret", default="")
            ).strip(),
            switch_name=str(
                _payload_get(raw, "switch_name", "switchName", default="")
            ).strip(),
            status_key=str(
                _payload_get(raw, "status_key", "statusKey", default="")
            ).strip(),
            endpoint=str(_payload_get(raw, "endpoint", default="")).strip(),
            raw=raw,
        )


@dataclass(slots=True, frozen=True)
class EvolvIOTDevice:
    """Device metadata for an EvolvIOT entity."""

    id: str
    name: str
    manufacturer: str
    model: str
    local_control: EvolvIOTLocalControl | None
    raw: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "EvolvIOTDevice":
        """Build device metadata from an API payload."""
        raw = dict(payload or {})
        return cls(
            id=str(
                _payload_get(raw, "id", "device_id", "deviceId", default="")
            ).strip(),
            name=str(_payload_get(raw, "name", default="")).strip(),
            manufacturer=str(
                _payload_get(raw, "manufacturer", default="EvolvIOT")
            ).strip(),
            model=str(_payload_get(raw, "model", default="")).strip(),
            local_control=EvolvIOTLocalControl.from_payload(
                _as_mapping(
                    _payload_get(raw, "local_control", "localControl", default={})
                )
            ),
            raw=raw,
        )


@dataclass(slots=True, frozen=True)
class EvolvIOTControl:
    """Control metadata for an EvolvIOT entity."""

    key: str
    name: str
    appliance_name: str
    raw: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "EvolvIOTControl":
        """Build control metadata from an API payload."""
        raw = dict(payload or {})
        return cls(
            key=str(_payload_get(raw, "key", default="")).strip(),
            name=str(_payload_get(raw, "name", default="")).strip(),
            appliance_name=str(
                _payload_get(raw, "appliance_name", "applianceName", default="")
            ).strip(),
            raw=raw,
        )


@dataclass(slots=True, frozen=True)
class EvolvIOTLocalCommand:
    """Local encrypted command metadata resolved for an entity."""

    uid: str
    device_id: str
    endpoint: str
    device_secret: str
    switch_name: str
    value: float | bool | str


@dataclass(slots=True, frozen=True)
class EvolvIOTState:
    """State for one EvolvIOT entity."""

    entity_id: str
    state: str | None
    available: bool
    raw_value: Any
    attributes: dict[str, Any]
    raw: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "EvolvIOTState | None":
        """Build an entity state from an API payload."""
        if not payload:
            return None

        raw = dict(payload)
        entity_id = str(_payload_get(raw, "entity_id", "entityId", default="")).strip()
        if not entity_id:
            return None

        raw_value = _payload_get(raw, "raw_value", "rawValue", "value")
        state = _payload_get(raw, "state")
        state_value = str(state).strip().lower() if state is not None else None
        if not state_value and raw_value is not None:
            state_value = _state_from_value(raw_value)

        available = bool(_payload_get(raw, "available", default=True))
        attributes = _as_dict(_payload_get(raw, "attributes", default={}))
        return cls(
            entity_id=entity_id,
            state=state_value,
            available=available,
            raw_value=raw_value,
            attributes=attributes,
            raw=raw,
        )

    @classmethod
    def from_local_value(
        cls,
        entity_id: str,
        value: Any,
        *,
        base_state: "EvolvIOTState | None" = None,
    ) -> "EvolvIOTState":
        """Build a normalized state from a local device value."""
        raw = dict(base_state.raw if base_state is not None else {})
        raw["entity_id"] = entity_id
        raw["available"] = True
        raw["raw_value"] = value
        raw["state"] = _state_from_value(value)
        return cls(
            entity_id=entity_id,
            state=raw["state"],
            available=True,
            raw_value=value,
            attributes=dict(base_state.attributes if base_state is not None else {}),
            raw=raw,
        )

    @property
    def is_on(self) -> bool | None:
        """Return whether the state represents an on switch value."""
        if self.state is None:
            return None
        return self.state == "on"


@dataclass(slots=True, frozen=True)
class EvolvIOTEntity:
    """EvolvIOT entity metadata."""

    entity_id: str
    unique_id: str
    domain: str
    name: str
    device: EvolvIOTDevice
    control: EvolvIOTControl
    local_control: EvolvIOTLocalControl | None
    raw: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "EvolvIOTEntity | None":
        """Build an entity from an API payload."""
        if not payload:
            return None

        raw = dict(payload)
        entity_id = str(_payload_get(raw, "entity_id", "entityId", default="")).strip()
        if not entity_id:
            return None

        domain = str(_payload_get(raw, "domain", default="")).strip()
        if not domain and "." in entity_id:
            domain = entity_id.split(".", 1)[0]

        return cls(
            entity_id=entity_id,
            unique_id=str(
                _payload_get(raw, "unique_id", "uniqueId", default=entity_id)
            ).strip(),
            domain=domain,
            name=str(_payload_get(raw, "name", default=entity_id)).strip(),
            device=EvolvIOTDevice.from_payload(_as_mapping(raw.get("device"))),
            control=EvolvIOTControl.from_payload(_as_mapping(raw.get("control"))),
            local_control=EvolvIOTLocalControl.from_payload(
                _as_mapping(
                    _payload_get(raw, "local_control", "localControl", default={})
                )
            ),
            raw=raw,
        )

    @property
    def effective_local_control(self) -> EvolvIOTLocalControl | None:
        """Return entity or device local control metadata."""
        return self.device.local_control or self.local_control

    def local_command(
        self,
        uid: str,
        payload: Mapping[str, Any],
    ) -> EvolvIOTLocalCommand | None:
        """Return local command metadata for a Home Assistant command payload."""
        value = _local_command_value(payload)
        if value is None:
            return None

        local_control = self.effective_local_control
        if local_control is None or not local_control.enabled:
            return None

        device_id = self.device.id.strip()
        device_secret = local_control.device_secret.strip()
        switch_name = (
            local_control.switch_name or local_control.status_key or self.control.key
        ).strip()
        endpoint = local_control.endpoint.strip()
        if not endpoint or endpoint == "control":
            endpoint = switch_name

        uid = uid.strip()
        if not all((uid, device_id, device_secret, switch_name, endpoint)):
            return None

        return EvolvIOTLocalCommand(
            uid=uid,
            device_id=device_id,
            endpoint=endpoint,
            device_secret=device_secret,
            switch_name=switch_name,
            value=value,
        )

    def local_value_from_status(self, local_data: Mapping[str, Any]) -> Any | None:
        """Return this entity's value from a local device status payload."""
        local_control = self.effective_local_control
        candidates = [
            local_control.status_key if local_control is not None else "",
            local_control.switch_name if local_control is not None else "",
            self.control.key,
            self.control.name,
            self.control.appliance_name,
        ]

        for candidate in candidates:
            key = str(candidate or "").strip()
            if key and key in local_data:
                return local_data[key]

        normalized_local_data = {
            _normalize_status_key(key): value for key, value in local_data.items()
        }
        for candidate in candidates:
            normalized_key = _normalize_status_key(candidate)
            if normalized_key and normalized_key in normalized_local_data:
                return normalized_local_data[normalized_key]
        return None

    def state_from_local_status(
        self,
        local_data: Mapping[str, Any],
        *,
        base_state: EvolvIOTState | None = None,
    ) -> EvolvIOTState | None:
        """Return a normalized entity state from local device status data."""
        value = self.local_value_from_status(local_data)
        if value is None:
            return None
        return EvolvIOTState.from_local_value(
            self.entity_id,
            value,
            base_state=base_state,
        )


@dataclass(slots=True, frozen=True)
class EvolvIOTData:
    """Entities and states for one EvolvIOT account."""

    user_id: str
    entities: dict[str, EvolvIOTEntity]
    states: dict[str, EvolvIOTState]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> "EvolvIOTData":
        """Build account data from a ready/devices/states payload."""
        raw = dict(payload or {})
        entities = {}
        for item in _as_list(raw.get("entities")):
            entity = EvolvIOTEntity.from_payload(_as_mapping(item))
            if entity is not None:
                entities[entity.entity_id] = entity

        states = {}
        for item in _as_list(raw.get("states")):
            state = EvolvIOTState.from_payload(_as_mapping(item))
            if state is not None:
                states[state.entity_id] = state

        return cls(
            user_id=str(_payload_get(raw, "user_id", "userId", default="")).strip(),
            entities=entities,
            states=states,
        )

    def with_entities(
        self,
        entities: Mapping[str, EvolvIOTEntity],
    ) -> "EvolvIOTData":
        """Return a copy with updated entities."""
        return replace(self, entities=dict(entities))

    def with_states(self, states: Mapping[str, EvolvIOTState]) -> "EvolvIOTData":
        """Return a copy with updated states."""
        return replace(self, states=dict(states))

    def with_state(self, state: EvolvIOTState) -> "EvolvIOTData":
        """Return a copy with one updated state."""
        states = dict(self.states)
        states[state.entity_id] = state
        return replace(self, states=states)


@dataclass(slots=True, frozen=True)
class EvolvIOTCommandResult:
    """Result of a cloud or WebSocket command."""

    entity_id: str
    unique_id: str
    accepted: bool
    acked: bool
    payload: Any
    state: EvolvIOTState | None
    raw: dict[str, Any]

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> "EvolvIOTCommandResult":
        """Build a command result from an API payload."""
        raw = dict(payload or {})
        command = _as_dict(raw.get("command"))
        return cls(
            entity_id=str(
                _payload_get(raw, "entity_id", "entityId", default="")
            ).strip(),
            unique_id=str(
                _payload_get(raw, "unique_id", "uniqueId", default="")
            ).strip(),
            accepted=bool(command.get("accepted", raw.get("accepted", False))),
            acked=bool(command.get("acked", raw.get("acked", False))),
            payload=command.get("payload", raw.get("payload")),
            state=EvolvIOTState.from_payload(_as_mapping(raw.get("state"))),
            raw=raw,
        )


@dataclass(slots=True, frozen=True)
class EvolvIOTReadyEvent:
    """Initial WebSocket ready event."""

    data: EvolvIOTData


@dataclass(slots=True, frozen=True)
class EvolvIOTStateChangedEvent:
    """WebSocket state-changed event."""

    state: EvolvIOTState


EvolvIOTEvent = EvolvIOTReadyEvent | EvolvIOTStateChangedEvent


def normalize_api_base_url(value: str | None) -> str:
    """Normalize user supplied API URL to the Home Assistant route root."""
    base_url = (value or DEFAULT_API_BASE_URL).strip().rstrip("/")
    if base_url.endswith("/api/homeassistant"):
        return base_url
    if base_url.endswith("/api"):
        return f"{base_url}/homeassistant"
    return f"{base_url}/api/homeassistant"


def websocket_url_from_api_base_url(api_base_url: str, access_token: str) -> str:
    """Return the raw Home Assistant WebSocket URL for an API base URL."""
    parsed = urlsplit(normalize_api_base_url(api_base_url))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    query = urlencode({"access_token": access_token})
    return urlunsplit((scheme, parsed.netloc, HOME_ASSISTANT_WS_PATH, query, ""))


def _payload_get(
    payload: Mapping[str, Any],
    *keys: str,
    default: Any = None,
) -> Any:
    """Return the first present payload key."""
    for key in keys:
        if key in payload:
            return payload[key]
    return default


def _as_mapping(value: Any) -> Mapping[str, Any]:
    """Return a mapping for dict-like payloads."""
    return value if isinstance(value, Mapping) else {}


def _as_dict(value: Any) -> dict[str, Any]:
    """Return a dict for dict-like payloads."""
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    """Return a list for list payloads."""
    return list(value) if isinstance(value, list) else []


def _normalize_status_key(value: Any) -> str:
    """Normalize status keys for tolerant local payload matching."""
    return "".join(
        character for character in str(value or "").lower() if character.isalnum()
    )


def _state_from_value(value: Any) -> str:
    """Normalize a raw switch-like value to Home Assistant state text."""
    try:
        return "on" if float(value) > 0 else "off"
    except (TypeError, ValueError):
        return "on" if str(value).strip().lower() in {"on", "true", "1"} else "off"


def _local_command_value(payload: Mapping[str, Any]) -> float | bool | str | None:
    """Map a Home Assistant command payload to an EvolvIOT local value."""
    command = payload.get("command")
    if command == "turn_on":
        return 1
    if command == "turn_off":
        return 0
    return None


def _sanitize_device_id_for_mdns(device_id: str) -> str:
    """Return the ESP mDNS-safe device id."""
    return re.sub(r"[^a-z0-9-]", "-", device_id.lower())


def _derive_local_keys(
    device_secret: str,
    uid: str,
    device_id: str,
) -> tuple[bytes, bytes]:
    """Derive AES and HMAC keys matching the EvolvIOT app."""
    key_material = f"{device_secret}:{uid}:{device_id}"
    aes_key = hashlib.sha256(f"{key_material}:AES".encode()).digest()
    hmac_key = hashlib.sha256(f"{key_material}:HMAC".encode()).digest()
    return aes_key, hmac_key


def _encrypt_local_payload(
    payload: dict[str, Any],
    device_secret: str,
    uid: str,
    device_id: str,
) -> str:
    """Encrypt local control payload with AES-256-CBC."""
    aes_key, _ = _derive_local_keys(device_secret, uid, device_id)
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    padding_size = 16 - (len(payload_bytes) % 16)
    padded_payload = payload_bytes + bytes([padding_size]) * padding_size

    iv = os.urandom(16)
    encryptor = Cipher(algorithms.AES(aes_key), modes.CBC(iv)).encryptor()
    ciphertext = encryptor.update(padded_payload) + encryptor.finalize()
    return base64.b64encode(iv + ciphertext).decode("ascii")


def _sign_local_payload(
    encrypted_data: str,
    device_secret: str,
    uid: str,
    device_id: str,
) -> str:
    """Sign encrypted local control payload with HMAC-SHA256."""
    _, hmac_key = _derive_local_keys(device_secret, uid, device_id)
    signature = hmac.new(
        hmac_key,
        encrypted_data.encode(),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(signature).decode("ascii")


def _local_status_headers(
    uid: str, device_id: str, device_secret: str
) -> dict[str, str]:
    """Build signed local status headers matching the EvolvIOT app."""
    timestamp = str(int(time.time()))
    nonce = base64.urlsafe_b64encode(os.urandom(12)).decode("ascii").rstrip("=")
    canonical = f"GET\n/status\n{timestamp}\n{nonce}\n{device_id}"
    _, hmac_key = _derive_local_keys(device_secret, uid, device_id)
    signature = hmac.new(
        hmac_key,
        canonical.encode(),
        hashlib.sha256,
    ).digest()
    return {
        "X-Evolv-Timestamp": timestamp,
        "X-Evolv-Nonce": nonce,
        "X-Evolv-Signature": base64.b64encode(signature).decode("ascii"),
    }


class EvolvIOTApi:
    """Small async client for `/api/homeassistant`."""

    def __init__(
        self,
        session: ClientSession,
        api_base_url: str,
        access_token: str = "",
        *,
        refresh_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        health_url: str = DEFAULT_HEALTH_URL,
        verify_ssl: bool = True,
        token_update_callback: TokenUpdateCallback | None = None,
    ) -> None:
        """Initialize the API client."""
        self._session = session
        self.api_base_url = normalize_api_base_url(api_base_url)
        self.health_url = health_url.strip().rstrip("/") or DEFAULT_HEALTH_URL
        self.access_token = access_token.strip()
        self.refresh_token = (refresh_token or "").strip()
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip()
        self.verify_ssl = verify_ssl
        self._token_update_callback = token_update_callback

    async def async_validate(self) -> dict[str, Any]:
        """Validate cloud reachability and the supplied bearer token."""
        await self.async_health()
        return await self.async_get_devices()

    async def async_validate_data(self) -> EvolvIOTData:
        """Validate cloud reachability and return typed account data."""
        await self.async_health()
        return await self.async_get_data()

    async def async_health(self) -> None:
        """Check backend health."""
        try:
            async with self._session.get(
                self.health_url,
                ssl=self.verify_ssl,
            ) as response:
                response.raise_for_status()
                await response.text()
        except ClientResponseError as err:
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_exchange_authorization_code(
        self,
        authorization_code: str,
        client_id: str,
        client_secret: str,
    ) -> dict[str, Any]:
        """Exchange an OAuth authorization code for tokens."""
        try:
            async with self._session.post(
                f"{self.api_base_url}/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                ssl=self.verify_ssl,
            ) as response:
                if response.status in (401, 403):
                    raise EvolvIOTAuthError("Invalid EvolvIOT OAuth credentials")
                response.raise_for_status()
                data = await response.json(content_type=None)
                if not isinstance(data, dict) or not data.get("access_token"):
                    raise EvolvIOTAuthError(
                        "Token response did not include access token"
                    )
                return data
        except EvolvIOTApiError:
            raise
        except ClientResponseError as err:
            raise EvolvIOTAuthError("Invalid OAuth authorization code") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_start_device_authorization(self) -> dict[str, Any]:
        """Start app-based Home Assistant pairing."""
        try:
            async with self._session.post(
                f"{self.api_base_url}/device/authorize",
                ssl=self.verify_ssl,
            ) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except ClientResponseError as err:
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_exchange_device_code(self, device_code: str) -> dict[str, Any]:
        """Exchange an approved device code for access and refresh tokens."""
        try:
            async with self._session.post(
                f"{self.api_base_url}/oauth/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                },
                ssl=self.verify_ssl,
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    error = str((data or {}).get("error") or "")
                    if error in {"authorization_pending", "slow_down"}:
                        raise EvolvIOTDeviceAuthorizationPending(
                            "Device authorization is pending"
                        )
                    if error == "access_denied":
                        raise EvolvIOTDeviceAuthorizationDenied(
                            "Device authorization was denied"
                        )
                    if error in {"expired_token", "invalid_grant"}:
                        raise EvolvIOTDeviceAuthorizationExpired(
                            "Device authorization expired"
                        )
                    raise EvolvIOTAuthError(error or "Device authorization failed")

                if not isinstance(data, dict) or not data.get("access_token"):
                    raise EvolvIOTAuthError(
                        "Token response did not include access token"
                    )
                return data
        except EvolvIOTApiError:
            raise
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_get_devices(self) -> dict[str, Any]:
        """Fetch entities available to the authenticated user."""
        return await self._request("get", "/devices")

    async def async_get_data(self) -> EvolvIOTData:
        """Fetch entities and states as typed data."""
        devices_payload = await self.async_get_devices()
        states_payload = await self.async_get_states()
        return EvolvIOTData.from_payload(
            {
                **devices_payload,
                "states": states_payload.get("states", []),
            }
        )

    async def async_get_states(self) -> dict[str, Any]:
        """Fetch states for all entities."""
        return await self._request("get", "/devices/states")

    async def async_get_typed_states(self) -> dict[str, EvolvIOTState]:
        """Fetch all entity states as typed models."""
        payload = await self.async_get_states()
        data = EvolvIOTData.from_payload({"states": payload.get("states", [])})
        return data.states

    async def async_get_state(self, entity_id: str) -> dict[str, Any]:
        """Fetch one entity state."""
        safe_entity_id = quote(entity_id, safe="")
        return await self._request("get", f"/devices/{safe_entity_id}/state")

    async def async_get_typed_state(self, entity_id: str) -> EvolvIOTState | None:
        """Fetch one entity state as a typed model."""
        payload = await self.async_get_state(entity_id)
        state_payload = payload.get("state")
        if isinstance(state_payload, Mapping):
            return EvolvIOTState.from_payload(state_payload)
        return EvolvIOTState.from_payload(payload)

    async def async_command(
        self, entity_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Send a command to one entity."""
        safe_entity_id = quote(entity_id, safe="")
        return await self._request(
            "post", f"/devices/{safe_entity_id}/command", json=payload
        )

    async def async_send_command(
        self,
        entity_id: str,
        payload: Mapping[str, Any] | str,
    ) -> EvolvIOTCommandResult:
        """Send a command and return a typed command result."""
        command_payload = (
            {"command": payload} if isinstance(payload, str) else dict(payload)
        )
        return EvolvIOTCommandResult.from_payload(
            await self.async_command(entity_id, command_payload)
        )

    async def async_local_command_for_entity(
        self,
        uid: str,
        entity: EvolvIOTEntity,
        payload: Mapping[str, Any],
    ) -> bool:
        """Send a local command when entity metadata supports local control."""
        local_command = entity.local_command(uid, payload)
        if local_command is None:
            return False

        await self.async_local_command(
            uid=local_command.uid,
            device_id=local_command.device_id,
            endpoint=local_command.endpoint,
            device_secret=local_command.device_secret,
            switch_name=local_command.switch_name,
            value=local_command.value,
        )
        return True

    async def async_connect_websocket(
        self,
        *,
        request_timeout: float = DEFAULT_WS_REQUEST_TIMEOUT,
    ) -> "EvolvIOTWebSocket":
        """Connect to the Home Assistant raw WebSocket endpoint."""
        websocket = EvolvIOTWebSocket(self, request_timeout=request_timeout)
        await websocket.async_connect()
        return websocket

    async def async_local_command(
        self,
        *,
        uid: str,
        device_id: str,
        endpoint: str,
        device_secret: str,
        switch_name: str,
        value: float | bool | str,
    ) -> None:
        """Send an encrypted command directly to an ESP over local HTTP."""
        local_payload = {
            "switchName": switch_name,
            "value": value,
        }
        encrypted_data = _encrypt_local_payload(
            local_payload,
            device_secret,
            uid,
            device_id,
        )
        signature = _sign_local_payload(
            encrypted_data,
            device_secret,
            uid,
            device_id,
        )
        safe_endpoint = quote(endpoint.strip("/"), safe="")
        safe_device_id = _sanitize_device_id_for_mdns(device_id)
        url = f"http://{LOCAL_MDNS_DOMAIN}-{safe_device_id}.local/{safe_endpoint}"

        try:
            _LOGGER.debug("Sending EvolvIOT local command to %s", url)
            async with self._session.post(
                url,
                json={"data": encrypted_data, "hmac": signature},
                timeout=ClientTimeout(total=DEFAULT_LOCAL_COMMAND_TIMEOUT),
            ) as response:
                response.raise_for_status()
                await response.text()
        except ClientResponseError as err:
            raise EvolvIOTApiError(
                f"EvolvIOT local API returned HTTP {err.status}"
            ) from err
        except (TimeoutError, ClientError) as err:
            raise EvolvIOTConnectionError(
                "Could not connect to EvolvIOT device locally"
            ) from err

    async def async_local_status(
        self,
        *,
        uid: str,
        device_id: str,
        device_secret: str,
    ) -> dict[str, Any]:
        """Check device status directly over local HTTP."""
        safe_device_id = _sanitize_device_id_for_mdns(device_id)
        url = f"http://{LOCAL_MDNS_DOMAIN}-{safe_device_id}.local/status"
        headers = _local_status_headers(uid, device_id, device_secret)

        try:
            _LOGGER.debug("Checking EvolvIOT local status at %s", url)
            async with self._session.get(
                url,
                headers=headers,
                timeout=ClientTimeout(total=DEFAULT_LOCAL_COMMAND_TIMEOUT),
            ) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except ClientResponseError as err:
            raise EvolvIOTApiError(
                f"EvolvIOT local API returned HTTP {err.status}"
            ) from err
        except (TimeoutError, ClientError) as err:
            raise EvolvIOTConnectionError(
                "Could not connect to EvolvIOT device locally"
            ) from err

    async def _request(
        self,
        method: str,
        path: str,
        *,
        auth: bool = True,
        retry_auth: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Run an HTTP request and return JSON."""
        headers = dict(kwargs.pop("headers", {}))
        if auth:
            headers["Authorization"] = f"Bearer {self.access_token}"

        url = f"{self.api_base_url}{path}"
        try:
            async with self._session.request(
                method.upper(),
                url,
                headers=headers,
                ssl=self.verify_ssl,
                **kwargs,
            ) as response:
                if response.status in (401, 403):
                    if auth and retry_auth and await self._async_refresh_token():
                        return await self._request(
                            method,
                            path,
                            auth=auth,
                            retry_auth=False,
                            **kwargs,
                        )
                    raise EvolvIOTAuthError("Invalid EvolvIOT credentials")

                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except EvolvIOTApiError:
            raise
        except ClientResponseError as err:
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def _async_ws_connect(
        self,
        *,
        retry_auth: bool = True,
    ) -> ClientWebSocketResponse:
        """Open the Home Assistant raw WebSocket connection."""
        try:
            return await self._session.ws_connect(
                websocket_url_from_api_base_url(self.api_base_url, self.access_token),
                ssl=self.verify_ssl,
            )
        except ClientResponseError as err:
            if (
                err.status in (401, 403)
                and retry_auth
                and await self._async_refresh_token()
            ):
                return await self._async_ws_connect(retry_auth=False)
            if err.status in (401, 403):
                raise EvolvIOTAuthError("Invalid EvolvIOT credentials") from err
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def _async_refresh_token(self) -> bool:
        """Refresh the access token when OAuth credentials are available."""
        if not self.refresh_token:
            return False

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        if self.client_id:
            data["client_id"] = self.client_id
        if self.client_secret:
            data["client_secret"] = self.client_secret

        try:
            async with self._session.post(
                f"{self.api_base_url}/oauth/token",
                data=data,
                ssl=self.verify_ssl,
            ) as response:
                if response.status >= 400:
                    return False
                token_data = await response.json(content_type=None)
        except ClientError:
            return False

        access_token = str(token_data.get("access_token") or "").strip()
        if not access_token:
            return False

        self.access_token = access_token
        self.refresh_token = str(
            token_data.get("refresh_token") or self.refresh_token
        ).strip()

        if self._token_update_callback is not None:
            await self._token_update_callback(
                {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                }
            )

        return True


class EvolvIOTWebSocket:
    """Raw WebSocket client for EvolvIOT Home Assistant events."""

    def __init__(
        self,
        api: EvolvIOTApi,
        *,
        request_timeout: float = DEFAULT_WS_REQUEST_TIMEOUT,
    ) -> None:
        """Initialize the WebSocket client."""
        self._api = api
        self._request_timeout = request_timeout
        self._ws: ClientWebSocketResponse | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._ready_waiter: asyncio.Future[EvolvIOTData] | None = None
        self._closed_waiter: asyncio.Future[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._callbacks: list[EventCallback] = []
        self._request_counter = count(1)
        self._closing = False
        self.data = EvolvIOTData(user_id="", entities={}, states={})

    @property
    def connected(self) -> bool:
        """Return whether the WebSocket is connected."""
        return self._ws is not None and not self._ws.closed

    def async_add_listener(self, callback: EventCallback) -> Callable[[], None]:
        """Add an event listener and return an unsubscribe callback."""
        self._callbacks.append(callback)

        def unsubscribe() -> None:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

        return unsubscribe

    async def async_connect(self) -> EvolvIOTData:
        """Connect and wait for the initial ready payload."""
        if self.connected:
            return self.data

        loop = asyncio.get_running_loop()
        self._closing = False
        self._ready_waiter = loop.create_future()
        self._closed_waiter = loop.create_future()
        self._ws = await self._api._async_ws_connect()
        self._reader_task = asyncio.create_task(self._async_reader())

        try:
            return await asyncio.wait_for(
                self._ready_waiter,
                timeout=self._request_timeout,
            )
        except TimeoutError as err:
            await self.async_close()
            raise EvolvIOTConnectionError(
                "Timed out waiting for EvolvIOT WebSocket ready payload"
            ) from err

    async def async_run_forever(
        self,
        *,
        reconnect: bool = True,
        backoff: tuple[float, ...] = (1, 2, 5, 10, 30),
    ) -> None:
        """Keep the WebSocket connected until closed."""
        attempt = 0
        while not self._closing:
            try:
                await self.async_connect()
                attempt = 0
                await self.async_wait_closed()
            except EvolvIOTAuthError:
                raise
            except EvolvIOTApiError:
                if self._closing or not reconnect:
                    raise

            if self._closing or not reconnect:
                return

            await asyncio.sleep(backoff[min(attempt, len(backoff) - 1)])
            attempt += 1

    async def async_wait_closed(self) -> None:
        """Wait until the active WebSocket closes."""
        if self._closed_waiter is not None:
            await self._closed_waiter

    async def async_refresh(self) -> EvolvIOTData:
        """Request fresh entities and states."""
        await self._async_request("refresh")
        return self.data

    async def async_list_devices(self) -> dict[str, EvolvIOTEntity]:
        """Request the latest entity metadata."""
        payload = await self._async_request("devices")
        data = EvolvIOTData.from_payload(payload)
        self.data = self.data.with_entities(data.entities)
        return self.data.entities

    async def async_list_states(self) -> dict[str, EvolvIOTState]:
        """Request the latest entity states."""
        payload = await self._async_request("devices.states")
        data = EvolvIOTData.from_payload({"states": payload.get("states", [])})
        self.data = self.data.with_states(data.states)
        return self.data.states

    async def async_get_state(self, entity_id: str) -> EvolvIOTState | None:
        """Request one entity state."""
        payload = await self._async_request(
            "device.state",
            {"entity_id": entity_id},
        )
        state = EvolvIOTState.from_payload(_as_mapping(payload.get("state")))
        if state is not None:
            self.data = self.data.with_state(state)
        return state

    async def async_command(
        self,
        entity_id: str,
        payload: Mapping[str, Any] | str,
    ) -> EvolvIOTCommandResult:
        """Send a command over the WebSocket."""
        message: dict[str, Any] = {
            "entity_id": entity_id,
        }
        if isinstance(payload, str):
            message["command"] = payload
        else:
            payload_data = dict(payload)
            if "command" in payload_data:
                message["command"] = payload_data["command"]
            else:
                message["payload"] = payload_data

        response = await self._async_request("device.command", message)
        result = EvolvIOTCommandResult.from_payload(response)
        if result.state is not None:
            self.data = self.data.with_state(result.state)
            await self._async_emit(EvolvIOTStateChangedEvent(result.state))
        return result

    async def async_close(self) -> None:
        """Close the WebSocket connection."""
        self._closing = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        self._fail_pending(EvolvIOTConnectionError("EvolvIOT WebSocket closed"))

    async def _async_request(
        self,
        message_type: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send a request and wait for the matching response."""
        if not self.connected or self._ws is None:
            raise EvolvIOTConnectionError("EvolvIOT WebSocket is not connected")

        request_id = f"req-{next(self._request_counter)}"
        message = {"id": request_id, "type": message_type}
        if payload:
            message.update(payload)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = future
        try:
            await self._ws.send_json(message)
            return await asyncio.wait_for(future, timeout=self._request_timeout)
        except TimeoutError as err:
            raise EvolvIOTConnectionError(
                f"Timed out waiting for EvolvIOT WebSocket response to {message_type}"
            ) from err
        finally:
            self._pending.pop(request_id, None)

    async def _async_reader(self) -> None:
        """Read and dispatch WebSocket messages."""
        assert self._ws is not None
        try:
            async for message in self._ws:
                if message.type == WSMsgType.TEXT:
                    await self._async_handle_text(message.data)
                elif message.type in (WSMsgType.CLOSED, WSMsgType.CLOSE):
                    break
                elif message.type == WSMsgType.ERROR:
                    raise EvolvIOTConnectionError("EvolvIOT WebSocket failed")
        except asyncio.CancelledError:
            raise
        except EvolvIOTApiError as err:
            self._fail_pending(err)
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_exception(err)
        except (TypeError, json.JSONDecodeError) as err:
            error = EvolvIOTWebSocketError("invalid_json", str(err))
            self._fail_pending(error)
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_exception(error)
        finally:
            closed_error = EvolvIOTConnectionError("EvolvIOT WebSocket closed")
            self._fail_pending(closed_error)
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_exception(closed_error)
            self._ws = None
            if self._closed_waiter is not None and not self._closed_waiter.done():
                self._closed_waiter.set_result(None)

    async def _async_handle_text(self, data: str) -> None:
        """Handle one text WebSocket message."""
        payload = json.loads(data)
        if not isinstance(payload, dict):
            raise EvolvIOTWebSocketError("invalid_message", "Expected object payload")

        message_id = str(payload.get("id") or "")
        message_type = str(payload.get("type") or "")

        if message_type == "error":
            error = EvolvIOTWebSocketError(
                str(payload.get("error") or "request_failed"),
                str(payload.get("message") or ""),
            )
            if message_id in self._pending and not self._pending[message_id].done():
                self._pending[message_id].set_exception(error)
                return
            raise error

        if message_type == "ready":
            self.data = EvolvIOTData.from_payload(payload)
            event = EvolvIOTReadyEvent(self.data)
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_result(self.data)
            await self._async_emit(event)
        elif message_type == "state_changed":
            state = EvolvIOTState.from_payload(_as_mapping(payload.get("state")))
            if state is not None:
                self.data = self.data.with_state(state)
                await self._async_emit(EvolvIOTStateChangedEvent(state))
        elif message_type == "command_result":
            result = EvolvIOTCommandResult.from_payload(payload)
            if result.state is not None:
                self.data = self.data.with_state(result.state)

        if message_id in self._pending and not self._pending[message_id].done():
            self._pending[message_id].set_result(payload)

    async def _async_emit(self, event: EvolvIOTEvent) -> None:
        """Emit an event to listeners."""
        for callback in list(self._callbacks):
            result = callback(event)
            if result is not None:
                await result

    def _fail_pending(self, err: Exception) -> None:
        """Fail all pending WebSocket requests."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(err)
