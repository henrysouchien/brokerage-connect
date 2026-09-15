from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any, AsyncIterator, Callable, Optional, Set

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .relay_state_machine import (
  IBKRExecuteRequest,
  IBKRRelay,
  IBKRToolResultRequest,
)


@dataclass(frozen=True)
class IBKRRouterAuthDeps:
  authenticate_session: Callable[[Request], Any]
  is_valid_service_token: Callable[[str], bool]
  service_auth_error: Callable[[], JSONResponse]
  mcp_auth_error: Callable[[HTTPException], JSONResponse]
  parse_authorized_accounts: Callable[[Optional[str], Request], Set[str]]
  service_token_count: Callable[[], int]
  json_dumps: Callable[[Any], str]
  log: logging.Logger


def build_router(
  relay: IBKRRelay,
  *,
  auth_deps: IBKRRouterAuthDeps,
) -> APIRouter:
  router = APIRouter()

  @router.get("/api/ibkr/events", response_model=None)
  async def ibkr_events(
    request: Request,
    authorized_accounts: Optional[str] = None,
  ) -> JSONResponse | StreamingResponse:
    try:
      auth_context = auth_deps.authenticate_session(request)
    except HTTPException as exc:
      return auth_deps.mcp_auth_error(exc)

    client_info = await relay.register_client(
      user_id=auth_context.user_id,
      jwt_session_id=auth_context.jwt_session_id,
      client_id=request.headers.get("X-IBKR-Client-Id"),
      authorized_accounts=auth_deps.parse_authorized_accounts(authorized_accounts, request),
    )
    client_id = client_info.client_id
    queue = client_info.queue
    auth_deps.log.info("IBKR relay connected | user_id=%s client=%s", auth_context.user_id, client_id[:8])

    async def event_generator() -> AsyncIterator[bytes]:
      last_heartbeat = time.time()
      try:
        connected = {
          "type": "connected",
          "client_id": client_id,
          "user_id": auth_context.user_id,
        }
        yield f"data: {auth_deps.json_dumps(connected)}\n\n".encode("utf-8")

        while True:
          if await request.is_disconnected():
            break

          now = time.time()
          if now - last_heartbeat >= 15:
            heartbeat = {"type": "heartbeat", "timestamp": int(now)}
            yield f"data: {auth_deps.json_dumps(heartbeat)}\n\n".encode("utf-8")
            last_heartbeat = now

          try:
            event = await asyncio.wait_for(queue.get(), timeout=1)
          except asyncio.TimeoutError:
            continue

          yield f"data: {auth_deps.json_dumps(event)}\n\n".encode("utf-8")

          if event.get("type") == "replaced":
            break
      finally:
        await relay.unregister_client(auth_context.user_id, client_id)
        auth_deps.log.info("IBKR relay disconnected | user_id=%s client=%s", auth_context.user_id, client_id[:8])

    return StreamingResponse(
      event_generator(),
      headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
      },
    )

  @router.post("/api/ibkr/execute")
  async def ibkr_execute(request: Request, payload: IBKRExecuteRequest) -> JSONResponse:
    token = request.headers.get("X-IBKR-Service-Token", "") or request.headers.get("x-ibkr-service-token", "")
    if not auth_deps.is_valid_service_token(token):
      return auth_deps.service_auth_error()

    try:
      response = await relay.execute(
        user_id=payload.user_id,
        tool_name=payload.tool_name,
        tool_input=payload.tool_input or {},
        request_id=payload.request_id,
        no_replay=payload.no_replay,
      )
    except RuntimeError as exc:
      code = str(exc)
      if code == "relay_disconnected":
        return JSONResponse({"error": "relay_disconnected"}, status_code=503)
      if code in {"missing_request_id", "duplicate_request_id"}:
        return JSONResponse({"error": code}, status_code=409 if code == "duplicate_request_id" else 400)
      return JSONResponse({"error": f"IBKR relay failed: {code}"}, status_code=500)
    except asyncio.TimeoutError:
      return JSONResponse({"error": "relay_timeout"}, status_code=504)

    return JSONResponse(response)

  @router.post("/api/ibkr/tool-result")
  async def ibkr_tool_result(request: Request, payload: IBKRToolResultRequest) -> JSONResponse:
    try:
      auth_context = auth_deps.authenticate_session(request)
    except HTTPException as exc:
      return auth_deps.mcp_auth_error(exc)

    if payload.ack and (payload.result is not None or payload.error is not None):
      return JSONResponse({"error": "Ack payload cannot include result or error"}, status_code=400)

    if payload.ack:
      status = await relay.ack(
        payload.request_id,
        payload.nonce,
        payload.delivery_id,
        jwt_session_id=auth_context.jwt_session_id,
        user_id=auth_context.user_id,
      )
    else:
      status = await relay.complete(
        request_id=payload.request_id,
        nonce=payload.nonce,
        delivery_id=payload.delivery_id,
        result=payload.result,
        error=payload.error,
        jwt_session_id=auth_context.jwt_session_id,
        user_id=auth_context.user_id,
      )

    if status in {"ok", "completed"}:
      return JSONResponse({"status": "ok"})
    if status == "not_found":
      return JSONResponse({"error": "Unknown request_id"}, status_code=404)
    if status == "expired":
      return JSONResponse({"error": "Request expired"}, status_code=410)
    if status == "nonce_mismatch":
      return JSONResponse({"error": "Nonce mismatch"}, status_code=409)
    if status == "delivery_id_required":
      return JSONResponse({"error": "delivery_id is required"}, status_code=400)
    if status == "stale_delivery":
      return JSONResponse({"error": "Stale delivery_id"}, status_code=409)
    if status in {"session_mismatch", "user_mismatch"}:
      return JSONResponse({"error": "Request does not belong to this session"}, status_code=403)
    return JSONResponse({"error": "Invalid request state"}, status_code=409)

  @router.get("/api/ibkr/status")
  async def ibkr_status(request: Request) -> JSONResponse:
    try:
      auth_context = auth_deps.authenticate_session(request)
    except HTTPException as exc:
      return auth_deps.mcp_auth_error(exc)
    return JSONResponse(
      await relay.status(
        user_id=auth_context.user_id,
        jwt_session_id=auth_context.jwt_session_id,
      )
    )

  @router.get("/api/ibkr/internal/health")
  async def ibkr_internal_health(request: Request) -> JSONResponse:
    token = request.headers.get("X-IBKR-Service-Token", "") or request.headers.get("x-ibkr-service-token", "")
    if not auth_deps.is_valid_service_token(token):
      return auth_deps.service_auth_error()
    return JSONResponse({"status": "ok", "tokens_active": auth_deps.service_token_count()})

  return router
