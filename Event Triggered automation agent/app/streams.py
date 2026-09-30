"""Thin helpers around a Redis Stream with a consumer group."""
from redis.exceptions import ResponseError

METRICS = "events:metrics"


async def ensure_group(r, s) -> None:
    try:
        await r.xgroup_create(s.stream_key, s.consumer_group, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def enqueue(r, s, event, source: str = "webhook") -> str:
    return await r.xadd(
        s.stream_key,
        {"payload": event.model_dump_json(), "source": source},
        maxlen=s.stream_maxlen,
        approximate=True,
    )


async def read_batch(r, s, consumer: str, block_ms: int = 2000, count: int = 10):
    resp = await r.xreadgroup(
        s.consumer_group, consumer, {s.stream_key: ">"}, count=count, block=block_ms
    )
    if not resp:
        return []
    return [(mid, fields) for _, msgs in resp for mid, fields in msgs]


async def reclaim_stale(r, s, consumer: str, min_idle_ms: int = 60000, count: int = 10):
    """Pick up messages another (crashed) consumer never acknowledged."""
    res = await r.xautoclaim(
        s.stream_key,
        s.consumer_group,
        consumer,
        min_idle_time=min_idle_ms,
        start_id="0-0",
        count=count,
    )
    return [(mid, fields) for mid, fields in res[1] if fields]


async def ack(r, s, msg_id: str) -> None:
    await r.xack(s.stream_key, s.consumer_group, msg_id)