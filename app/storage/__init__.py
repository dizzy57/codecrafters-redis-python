import abc
import asyncio
import collections
import datetime
import functools
import itertools
import logging
from typing import Final, cast, Callable

from app.protocol import NullString, RedisError, NullArray, SimpleString
from app.storage.list_ import List
from app.storage.stream import Stream, StreamOutput
from app.storage.string import String

logger: Final = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


type Value = String | List | Stream


def notify_blocked[*Ts, T](
    f: Callable[["Storage", bytes, *Ts], T],
) -> Callable[["Storage", bytes, *Ts], T]:
    @functools.wraps(f)
    def wrapper(self: "Storage", k: bytes, *args: *Ts) -> T:
        res = f(self, k, *args)
        self.blocking.notify_key(k, self.kv[k])
        return res

    return wrapper


class Storage:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.kv: dict[bytes, Value] = {}
        self.blocking = BlockingDispatcher()

    def set(self, k: bytes, v: bytes, *, ttl: datetime.timedelta | None = None) -> None:
        logger.debug("SET %r %r, ttl=%r", k, v, ttl)
        expiration = None
        if ttl is not None:
            expiration = self.loop.time() + ttl.total_seconds()
            self.loop.call_at(expiration, self._delete_if_expired, k, expiration)
        self.kv[k] = String(v=v, expiration=expiration)

    def get(self, k: bytes) -> bytes | NullString:
        value = self.kv.get(k)
        logger.debug("GET %r -> %r", k, value)
        match value:
            case None:
                return NullString()
            case String(expiration=t) if t is not None and t < self.loop.time():
                logger.debug("GET del_ttl %r", k)
                del self.kv[k]
                return NullString()
            case String(v=v):
                return v
            case _:
                raise RedisError(f"expected string value, got {value!r}")

    def _delete_if_expired(self, k: bytes, expiration: float) -> None:
        value = self.kv.get(k)
        logger.debug("_delete_if_expired %r -> %r", k, value)
        match value:
            case String(expiration=t) if t == expiration:
                logger.debug("_delete_if_expired del_ttl %r", k)
                del self.kv[k]

    @notify_blocked
    def rpush(self, k: bytes, xs: list[bytes]) -> int:
        v = self.kv.setdefault(k, List())
        if not isinstance(v, List):
            raise RedisError(f"key {k!r} is not list: {v!r}")
        return v.rpush(xs)

    def lrange(self, k: bytes, l: bytes, r: bytes) -> list[bytes]:
        try:
            li = int(l)
            ri = int(r)
        except ValueError as e:
            raise RedisError(f"unable to parse integer {l=} {r=}") from e

        v = self.kv.get(k)
        match v:
            case None:
                return []
            case List():
                return v.lrange(li, ri)
            case _:
                raise RedisError(f"key {k!r} is not list: {v!r}")

    @notify_blocked
    def lpush(self, k: bytes, xs: list[bytes]) -> int:
        v = self.kv.setdefault(k, List())
        if not isinstance(v, List):
            raise RedisError(f"key {k!r} is not list: {v!r}")
        return v.lpush(xs)

    def llen(self, k: bytes) -> int:
        v = self.kv.get(k)
        match v:
            case None:
                return 0
            case List():
                return v.llen()
            case _:
                raise RedisError(f"key {k!r} is not list: {v!r}")

    def lpop(self, k: bytes) -> bytes | NullString:
        v = self.kv.get(k)
        match v:
            case None:
                return NullString()
            case List():
                return v.lpop()
            case _:
                raise RedisError(f"key {k!r} is not list: {v!r}")

    def lpop_many(self, k: bytes, n: bytes) -> list[bytes] | NullString:
        try:
            ni = int(n)
        except ValueError as e:
            raise RedisError(f"unable to parse integer {n=}") from e

        v = self.kv.get(k)
        match v:
            case None:
                return NullString()
            case List():
                return v.lpop_many(ni)
            case _:
                raise RedisError(f"key {k!r} is not list: {v!r}")

    async def blpop(self, k: bytes, timeout: bytes) -> tuple[bytes, bytes] | NullArray:
        try:
            timeoutf = float(timeout)
        except ValueError as e:
            raise RedisError(f"unable to parse {timeout=}") from e

        v = self.kv.get(k)
        match v:
            case None:
                pass
            case List(empty=True):
                pass
            case List():
                return k, cast(bytes, v.lpop())
            case _:
                raise RedisError(f"key {k!r} is not list: {v!r}")
        request = BLPopRequest()
        self.blocking.add_block_request(k, request)

        done, pending = await asyncio.wait(
            [request.future], timeout=None if timeoutf == 0 else timeoutf
        )
        if done:
            return k, done.pop().result()
        self.blocking.remove_block_request(k, request)
        return NullArray()

    def type(self, k: bytes) -> SimpleString:
        v = self.kv.get(k)
        if v is None:
            return SimpleString("none")
        return v.type

    def xadd(self, k: bytes, id_or_template: bytes, kv: list[bytes]) -> bytes:
        if len(kv) % 2:
            raise RedisError(f"key-value list of odd length {kv=}")

        v = self.kv.setdefault(k, Stream())
        if not isinstance(v, Stream):
            raise RedisError(f"key {k!r} is not a stream: {v!r}")
        return v.xadd(id_or_template, kv)

    def xrange(self, k: bytes, start: bytes, end: bytes) -> StreamOutput:
        v = self.kv.get(k)
        if not isinstance(v, Stream):
            raise RedisError(f"key {k!r} is not a stream: {v!r}")
        return v.xrange(start, end)

    async def xread(
        self, keys_and_starts: list[bytes], block: bytes | None = None
    ) -> list[tuple[bytes, StreamOutput]] | NullArray:
        if len(keys_and_starts) % 2:
            raise RedisError(f"key-id list of odd length {keys_and_starts=}")
        n = len(keys_and_starts) // 2

        streams_starts = dict(
            zip(
                itertools.islice(keys_and_starts, n),
                itertools.islice(keys_and_starts, n, None),
            )
        )

        read, unread = self._xread_sync(streams_starts)
        if read or block is None:
            return read

        tasks = {
            asyncio.create_task(ev.wait()): key_start
            for ev, key_start in unread.items()
        }

        if block == b"0":
            timeout = None
        else:
            timeout = datetime.timedelta(milliseconds=int(block)).total_seconds()

        done, _ = await asyncio.wait(tasks, timeout=timeout)
        if done:
            streams_starts = dict(tasks[x] for x in done)
            read, _ = self._xread_sync(streams_starts)
            return read

        return NullArray()

    def _xread_sync(self, streams_starts: dict[bytes, bytes]) -> tuple[
        list[tuple[bytes, StreamOutput]],
        dict[asyncio.Event, tuple[bytes, bytes]],
    ]:
        read = []
        unread = {}
        for k, start in streams_starts.items():
            v = self.kv.get(k)
            if not isinstance(v, Stream):
                raise RedisError(f"key {k!r} is not a stream: {v!r}")
            if start != b"$":
                res = v.xread(start)
                if res:
                    read.append((k, res))
                else:
                    unread[v.ev] = (k, start)
            else:
                unread[v.ev] = (k, bytes(v.last_id()))

        return read, unread


class BlockingRequest(abc.ABC):
    def __init__(self) -> None:
        self.future = asyncio.get_running_loop().create_future()

    @abc.abstractmethod
    def notify(self, v: Value) -> bool: ...


class BlockingDispatcher:
    def __init__(self) -> None:
        self.kv: dict[bytes, list[BlockingRequest]] = collections.defaultdict(list)

    def add_block_request(self, k: bytes, request: BlockingRequest) -> None:
        self.kv[k].append(request)

    def remove_block_request(self, k: bytes, request: BlockingRequest) -> None:
        self.kv[k].remove(request)

    def notify_key(self, k: bytes, v: Value) -> None:
        l = self.kv[k]
        for i, r in enumerate(l):
            if r.notify(v):
                break
        else:
            return
        del l[i]


class BLPopRequest(BlockingRequest):
    def notify(self, v: Value) -> bool:
        match v:
            case List(empty=False):
                self.future.set_result(v.lpop())
                return True
        return False

