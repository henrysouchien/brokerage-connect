from __future__ import annotations

import asyncio
import math
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional, Set

from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class IBKRExecuteRequest(BaseModel):
  user_id: int
  tool_name: str
  tool_input: Dict[str, Any] = Field(default_factory=dict)
  request_id: str
  no_replay: bool = False


class IBKRToolResultRequest(BaseModel):
  request_id: str
  nonce: str
  delivery_id: Optional[str] = None
  ack: bool = False
  result: Optional[Any] = None
  error: Optional[Dict[str, Any]] = None


class ClientState(NamedTuple):
  jwt_session_id: str
  queue: "asyncio.Queue[Dict[str, Any]]"
  connected_at: float
  client_id: str
  authorized_accounts: Set[str]


class InflightState(NamedTuple):
  user_id: int
  future: "asyncio.Future[Dict[str, Any]]"
  tool_name: str
  no_replay: bool


def _tool_timeout() -> int:
  return int(os.getenv("IBKR_RELAY_TOOL_TIMEOUT", "60"))


def _reconnect_grace_seconds() -> int:
  return int(os.getenv("IBKR_RELAY_RECONNECT_GRACE_S", str(_tool_timeout() + 30)))


@dataclass
class _DetachedState:
  client_id: str
  deadline: float


class IBKRRelay:
  """User-keyed relay state machine for local IBKR/TWS execution.

  This mirrors the delivery/nonce/ack shape of ``excel_mcp.relay.McpRelay`` but
  intentionally does not share the Excel session/workbook state machine. IBKR
  requests are routed by numeric risk ``user_id`` because a local TWS instance is
  per-user, not per-workbook.

  Replay policy deliberately diverges from Excel: requests marked
  ``no_replay=True`` (place/cancel/execute operations) are never redelivered on
  client reconnect. If such a request was delivered and the relay client dies
  before returning a result, the caller times out and the user must reconcile in
  TWS. Replaying could double-submit an order.
  """

  EXPIRED_TTL_SECONDS = 300

  def __init__(self, *, tool_timeout: Optional[float] = None, reconnect_grace_seconds: Optional[int] = None) -> None:
    self._lock = asyncio.Lock()
    self._clients: Dict[int, ClientState] = {}
    self._detached: Dict[int, _DetachedState] = {}
    self._inflight: Dict[str, InflightState] = {}
    self._inflight_meta: Dict[str, Dict[str, Any]] = {}
    self._background_tasks: Set["asyncio.Task[Any]"] = set()
    self.tool_timeout = tool_timeout if tool_timeout is not None else _tool_timeout()
    self.reconnect_grace_seconds = (
      reconnect_grace_seconds
      if reconnect_grace_seconds is not None
      else _reconnect_grace_seconds()
    )

  async def execute(
    self,
    *,
    user_id: int,
    tool_name: str,
    tool_input: Dict[str, Any],
    request_id: str,
    no_replay: bool = False,
    timeout: Optional[int] = None,
  ) -> Dict[str, Any]:
    if not request_id:
      raise RuntimeError("missing_request_id")
    timeout_seconds = timeout if timeout is not None else self.tool_timeout
    nonce = os.urandom(12).hex()
    result_future: "asyncio.Future[Dict[str, Any]]" = asyncio.get_running_loop().create_future()

    async with self._lock:
      self._prune_expired_locked()
      if request_id in self._inflight:
        raise RuntimeError("duplicate_request_id")
      client = self._clients.get(int(user_id))
      if client is None or self._is_detached_locked(int(user_id), client.client_id):
        raise RuntimeError("relay_disconnected")
      self._inflight[request_id] = InflightState(
        int(user_id),
        result_future,
        tool_name,
        bool(no_replay),
      )
      self._inflight_meta[request_id] = {
        "nonce": nonce,
        "tool_input": dict(tool_input or {}),
        "state": "queued",
        "created_at": time.time(),
        "delivered_at": None,
        "acked_at": None,
        "completed_at": None,
        "expired_at": None,
        "delivery_client_id": None,
        "jwt_session_id": client.jwt_session_id,
      }

    delivered = await self._deliver(request_id, replay=False)
    if not delivered:
      async with self._lock:
        self._inflight.pop(request_id, None)
        self._inflight_meta.pop(request_id, None)
      raise RuntimeError("relay_disconnected")

    try:
      result_payload = await asyncio.wait_for(asyncio.shield(result_future), timeout=timeout_seconds)
    except asyncio.TimeoutError:
      async with self._lock:
        meta = self._inflight_meta.get(request_id)
        if meta is not None:
          meta["state"] = "expired"
          meta["expired_at"] = time.time()
      raise

    async with self._lock:
      self._inflight.pop(request_id, None)
      self._inflight_meta.pop(request_id, None)

    return {
      "request_id": request_id,
      "result": result_payload.get("result"),
      "error": result_payload.get("error"),
    }

  async def register_client(
    self,
    *,
    user_id: int,
    jwt_session_id: str,
    client_id: Optional[str] = None,
    authorized_accounts: Optional[Set[str]] = None,
  ) -> ClientState:
    queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
    now = time.time()
    effective_client_id = str(client_id or "").strip() or uuid.uuid4().hex
    client = ClientState(
      jwt_session_id=jwt_session_id,
      queue=queue,
      connected_at=now,
      client_id=effective_client_id,
      authorized_accounts=set(authorized_accounts or set()),
    )
    replaced_queue: Optional["asyncio.Queue[Dict[str, Any]]"] = None
    replays: List[Dict[str, Any]] = []

    async with self._lock:
      self._prune_expired_locked()
      key = int(user_id)
      replaced = self._clients.get(key)
      if replaced is not None and not self._is_detached_locked(key, replaced.client_id):
        replaced_queue = replaced.queue
      self._clients[key] = client
      self._detached.pop(key, None)

      for request_id, inflight in self._inflight.items():
        if inflight.user_id != key:
          continue
        meta = self._inflight_meta.get(request_id)
        if not meta or meta.get("state") != "delivered":
          continue

        if inflight.no_replay:
          # IBKR order-mutating requests may have already reached TWS. Unlike
          # Excel cell writes, redelivery can duplicate a trade/cancel, so the
          # gateway intentionally leaves the original future pending until it
          # completes from the old delivery or times out.
          meta["replay_skipped_at"] = now
          continue

        replays.append(self._mark_delivered_locked(request_id, inflight, meta, client, replay=True))

    if replaced_queue is not None:
      await replaced_queue.put({"type": "replaced", "reason": "Same-user relay reconnect"})

    for payload in replays:
      await queue.put(payload)

    return client

  async def unregister_client(self, user_id: int, client_id: str, *, immediate: bool = False) -> None:
    async with self._lock:
      key = int(user_id)
      client = self._clients.get(key)
      if client is None or client.client_id != client_id:
        return
      if immediate:
        self._clients.pop(key, None)
        self._detached.pop(key, None)
        return
      deadline = time.time() + self.reconnect_grace_seconds
      self._detached[key] = _DetachedState(client_id=client_id, deadline=deadline)
      self._schedule_detached_sweep(key, client_id, self.reconnect_grace_seconds)

  async def ack(
    self,
    request_id: str,
    nonce: str,
    delivery_id: Optional[str],
    *,
    jwt_session_id: Optional[str] = None,
    user_id: Optional[int] = None,
  ) -> str:
    async with self._lock:
      self._prune_expired_locked()
      ownership_status, _inflight, meta = self._validate_result_owner_locked(
        request_id,
        nonce,
        delivery_id,
        jwt_session_id=jwt_session_id,
        user_id=user_id,
      )
      if ownership_status != "ok":
        return ownership_status
      if meta is None:
        return "not_found"
      if meta.get("state") == "completed":
        return "completed"
      if meta.get("state") == "acked":
        return "ok"
      meta["state"] = "acked"
      meta["acked_at"] = time.time()
      return "ok"

  async def complete(
    self,
    request_id: str,
    nonce: str,
    delivery_id: Optional[str],
    result: Optional[Dict[str, Any]],
    error: Optional[Dict[str, Any]],
    *,
    jwt_session_id: Optional[str] = None,
    user_id: Optional[int] = None,
  ) -> str:
    result_future: Optional["asyncio.Future[Dict[str, Any]]"] = None
    async with self._lock:
      self._prune_expired_locked()
      ownership_status, inflight, meta = self._validate_result_owner_locked(
        request_id,
        nonce,
        delivery_id,
        jwt_session_id=jwt_session_id,
        user_id=user_id,
      )
      if ownership_status != "ok":
        return ownership_status
      if inflight is None or meta is None:
        return "not_found"
      if meta.get("state") == "completed":
        return "completed"
      meta["state"] = "completed"
      meta["completed_at"] = time.time()
      result_future = inflight.future

    if result_future is not None and not result_future.done():
      result_future.set_result({"result": result, "error": error})
    return "ok"

  async def status(self, *, user_id: int, jwt_session_id: Optional[str] = None) -> Dict[str, Any]:
    async with self._lock:
      self._prune_expired_locked()
      key = int(user_id)
      client = self._clients.get(key)
      connected = bool(client and not self._is_detached_locked(key, client.client_id))
      if client is not None and jwt_session_id is not None and client.jwt_session_id != jwt_session_id:
        connected = False
      inflight_count = sum(1 for item in self._inflight.values() if item.user_id == key)
      return {
        "user_id": key,
        "connected": connected,
        "client_id": client.client_id if client else None,
        "connected_at": client.connected_at if client else None,
        "authorized_accounts": sorted(client.authorized_accounts) if client else [],
        "inflight": inflight_count,
        "detach_grace_deadline": self._detached[key].deadline if key in self._detached else None,
      }

  async def _deliver(self, request_id: str, replay: bool) -> bool:
    async with self._lock:
      inflight = self._inflight.get(request_id)
      meta = self._inflight_meta.get(request_id)
      if inflight is None or meta is None:
        return False
      client = self._clients.get(inflight.user_id)
      if client is None or self._is_detached_locked(inflight.user_id, client.client_id):
        return False
      payload = self._mark_delivered_locked(request_id, inflight, meta, client, replay=replay)
      queue = client.queue

    await queue.put(payload)
    return True

  def _mark_delivered_locked(
    self,
    request_id: str,
    inflight: InflightState,
    meta: Dict[str, Any],
    client: ClientState,
    *,
    replay: bool,
  ) -> Dict[str, Any]:
    meta["state"] = "delivered"
    meta["delivered_at"] = time.time()
    meta["delivery_client_id"] = client.client_id
    meta["jwt_session_id"] = client.jwt_session_id
    return {
      "type": "mcp_tool_request",
      "request_id": request_id,
      "nonce": meta.get("nonce"),
      "delivery_id": client.client_id,
      "tool_name": inflight.tool_name,
      "tool_input": meta.get("tool_input") or {},
      "replay": replay,
      "no_replay": inflight.no_replay,
    }

  def _validate_result_owner_locked(
    self,
    request_id: str,
    nonce: str,
    delivery_id: Optional[str],
    *,
    jwt_session_id: Optional[str],
    user_id: Optional[int],
  ) -> tuple[str, Optional[InflightState], Optional[Dict[str, Any]]]:
    inflight = self._inflight.get(request_id)
    meta = self._inflight_meta.get(request_id)
    if inflight is None or meta is None:
      return "not_found", None, None
    if user_id is not None and inflight.user_id != int(user_id):
      return "user_mismatch", inflight, meta
    if jwt_session_id is not None and meta.get("jwt_session_id") != jwt_session_id:
      return "session_mismatch", inflight, meta
    if meta.get("nonce") != nonce:
      return "nonce_mismatch", inflight, meta
    if meta.get("state") == "expired":
      self._inflight.pop(request_id, None)
      self._inflight_meta.pop(request_id, None)
      return "expired", None, None
    if delivery_id is None:
      return "delivery_id_required", inflight, meta
    if meta.get("delivery_client_id") != delivery_id:
      return "stale_delivery", inflight, meta
    return "ok", inflight, meta

  def _is_detached_locked(self, user_id: int, client_id: str) -> bool:
    detached = self._detached.get(user_id)
    if detached is None or detached.client_id != client_id:
      return False
    return time.time() < detached.deadline

  def _schedule_detached_sweep(self, user_id: int, client_id: str, delay: float) -> None:
    task: Optional["asyncio.Task[Any]"] = None

    async def _runner() -> None:
      try:
        await asyncio.sleep(max(delay, 0))
        async with self._lock:
          detached = self._detached.get(user_id)
          client = self._clients.get(user_id)
          if detached is None or detached.client_id != client_id:
            return
          if client is not None and client.client_id == client_id:
            self._clients.pop(user_id, None)
          self._detached.pop(user_id, None)
      finally:
        if task is not None:
          self._background_tasks.discard(task)

    task = asyncio.create_task(_runner())
    self._background_tasks.add(task)

  def _prune_expired_locked(self) -> None:
    now = time.time()
    for user_id, detached in list(self._detached.items()):
      if now >= detached.deadline:
        client = self._clients.get(user_id)
        if client is not None and client.client_id == detached.client_id:
          self._clients.pop(user_id, None)
        self._detached.pop(user_id, None)

    for request_id, meta in list(self._inflight_meta.items()):
      if meta.get("state") != "expired":
        continue
      expired_at = float(meta.get("expired_at") or now)
      if now - expired_at >= self.EXPIRED_TTL_SECONDS:
        self._inflight.pop(request_id, None)
        self._inflight_meta.pop(request_id, None)


# JSON helpers copied from excel_mcp.relay so both SSE paths sanitize NaN/Inf
# consistently while keeping the IBKR trade relay state machine separate.
def _sanitize_for_json(obj: Any) -> Any:
  if isinstance(obj, float) and not math.isfinite(obj):
    return None
  if isinstance(obj, dict):
    return {key: _sanitize_for_json(value) for key, value in obj.items()}
  if isinstance(obj, (list, tuple)):
    return [_sanitize_for_json(value) for value in obj]
  if isinstance(obj, (set, frozenset)):
    return [_sanitize_for_json(value) for value in obj]
  return obj


def json_dumps(payload: Dict[str, Any]) -> str:
  sanitized = _sanitize_for_json(payload)
  return JSONResponse(content=sanitized).body.decode("utf-8")
