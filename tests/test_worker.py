import fakeredis.aioredis
import pytest

from app import dlq, streams
from app.config import Settings
from app.retry import PermanentError, TransientError
from app.schemas import Event
from app.worker import process


class FakeExecutor:
    """Returns/raises the scripted outcomes in order; repeats the last one."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    async def __call__(self, event, settings):
        self.calls += 1
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
async def env(tmp_path):
    s = Settings(max_attempts=3, retry_base_seconds=0, db_path=str(tmp_path / "a.db"))
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await streams.ensure_group(r, s)
    yield r, s
    await r.aclose()


async def deliver(r, s, event, execute):
    await streams.enqueue(r, s, event)
    ((msg_id, fields),) = await streams.read_batch(r, s, "test", block_ms=10)
    return await process(r, s, msg_id, fields, execute=execute)


EVENT = Event(id="evt_1", type="order.created", data={})


async def test_success_then_duplicate_is_ignored(env):
    r, s = env
    ex = FakeExecutor({"ok": True})
    assert await deliver(r, s, EVENT, ex) == "succeeded"
    assert await deliver(r, s, EVENT, ex) == "duplicate"
    assert ex.calls == 1  # side effect ran exactly once


async def test_transient_failures_are_retried(env):
    r, s = env
    ex = FakeExecutor(TransientError("boom"), TransientError("boom"), {"ok": True})
    assert await deliver(r, s, EVENT, ex) == "succeeded"
    assert ex.calls == 3
    assert await r.hget(streams.METRICS, "retries") == "2"


async def test_exhausted_retries_go_to_dlq(env):
    r, s = env
    ex = FakeExecutor(TransientError("n8n returned 500"))
    assert await deliver(r, s, EVENT, ex) == "dead_lettered"
    assert ex.calls == 3
    (entry,) = await dlq.list_entries(r, s)
    assert entry["reason"] == "retries_exhausted"
    assert entry["attempts"] == "3"


async def test_permanent_error_is_not_retried(env):
    r, s = env
    ex = FakeExecutor(PermanentError("n8n returned 400"))
    assert await deliver(r, s, EVENT, ex) == "dead_lettered"
    assert ex.calls == 1
    (entry,) = await dlq.list_entries(r, s)
    assert entry["reason"] == "permanent_error"


async def test_replay_reprocesses_dead_lettered_event(env):
    r, s = env
    assert await deliver(r, s, EVENT, FakeExecutor(TransientError("x"))) == "dead_lettered"
    (entry,) = await dlq.list_entries(r, s)

    assert await dlq.replay(r, s, entry["entry_id"])
    assert await dlq.list_entries(r, s) == []

    ((msg_id, fields),) = await streams.read_batch(r, s, "test", block_ms=10)
    result = await process(r, s, msg_id, fields, execute=FakeExecutor({"ok": True}))
    assert result == "succeeded"