"""Dead letter queue: a separate Redis stream with failure details + replay."""
import time

from . import idempotency, streams
from .schemas import Event


async def send(r, s, event: Event, reason: str, attempts: int, error: str) -> str:
    return await r.xadd(
        s.dlq_key,
        {
            "event_id": event.id,
            "type": event.type,
            "payload": event.model_dump_json(),
            "reason": reason,
            "attempts": str(attempts),
            "error": error[:1000],
            "failed_at": str(int(time.time())),
        },
    )


async def list_entries(r, s, count: int = 100) -> list[dict]:
    rows = await r.xrevrange(s.dlq_key, count=count)
    return [{"entry_id": entry_id, **fields} for entry_id, fields in rows]


async def replay(r, s, entry_id: str) -> bool:
    rows = await r.xrange(s.dlq_key, min=entry_id, max=entry_id)
    if not rows:
        return False
    _, fields = rows[0]
    event = Event.model_validate_json(fields["payload"])
    await idempotency.release(r, event.id)
    await streams.enqueue(r, s, event, source="dlq-replay")
    await r.xdel(s.dlq_key, entry_id)
    return True


async def replay_all(r, s) -> int:
    count = 0
    for entry in await list_entries(r, s, count=1000):
        if await replay(r, s, entry["entry_id"]):
            count += 1
    return count