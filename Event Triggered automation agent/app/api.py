"""FastAPI receiver: verify signature, validate, dedupe, enqueue. Plus admin API."""
import asyncio
import hmac
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request
from pydantic import ValidationError

from . import audit, dlq, streams
from .config import get_settings
from .schemas import Event
from .security import verify_signature


@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()
    app.state.redis = redis.from_url(s.redis_url, decode_responses=True)
    await streams.ensure_group(app.state.redis, s)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Event-Triggered Automation Agent", lifespan=lifespan)


@app.get("/health")
async def health(request: Request):
    await request.app.state.redis.ping()
    return {"status": "ok"}


@app.post("/webhooks/{source}", status_code=202)
async def receive(
    source: str, request: Request, x_signature: str | None = Header(default=None)
):
    s = get_settings()
    r = request.app.state.redis
    body = await request.body()

    if not verify_signature(body, x_signature, s.webhook_secret):
        raise HTTPException(status_code=401, detail="invalid signature")
    try:
        event = Event.model_validate_json(body)
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid event payload")

    # Fast-path dedupe at the edge; the worker still enforces idempotency.
    fresh = await r.set(f"enq:{event.id}", "1", nx=True, ex=s.idempotency_ttl_seconds)
    if not fresh:
        await r.hincrby(streams.METRICS, "edge_duplicates", 1)
        return {"status": "duplicate", "event_id": event.id}

    try:
        await streams.enqueue(r, s, event, source=source)
    except Exception:
        await r.delete(f"enq:{event.id}")
        raise HTTPException(status_code=503, detail="queue unavailable")

    await r.hincrby(streams.METRICS, "received", 1)
    return {"status": "queued", "event_id": event.id}


async def require_admin(x_admin_token: str | None = Header(default=None)):
    token = get_settings().admin_token
    if token and not hmac.compare_digest(x_admin_token or "", token):
        raise HTTPException(status_code=401, detail="admin token required")


admin = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])


@admin.get("/stats")
async def stats(request: Request):
    s, r = get_settings(), request.app.state.redis
    pending = await r.xpending(s.stream_key, s.consumer_group)
    return {
        "metrics": await r.hgetall(streams.METRICS),
        "stream_length": await r.xlen(s.stream_key),
        "pending": pending["pending"] if pending else 0,
        "dlq_length": await r.xlen(s.dlq_key),
    }


@admin.get("/events")
async def events(limit: int = 50):
    return await asyncio.to_thread(audit.recent, get_settings().db_path, limit)


@admin.get("/dlq")
async def dlq_list(request: Request):
    return await dlq.list_entries(request.app.state.redis, get_settings())


@admin.post("/dlq/replay-all")
async def dlq_replay_all(request: Request):
    count = await dlq.replay_all(request.app.state.redis, get_settings())
    return {"replayed": count}


@admin.post("/dlq/{entry_id}/replay")
async def dlq_replay(entry_id: str, request: Request):
    if not await dlq.replay(request.app.state.redis, get_settings(), entry_id):
        raise HTTPException(status_code=404, detail="entry not found")
    return {"replayed": entry_id}


app.include_router(admin)