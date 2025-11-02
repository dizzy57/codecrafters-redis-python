import abc
import asyncio
import bisect
import dataclasses
import datetime
import functools
import itertools
import logging
import time
from typing import Final, Callable, cast, ClassVar

from app.protocol import NullString, RedisError, NullArray, SimpleString

logger: Final = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


@dataclasses.dataclass(slots=True, frozen=True)
class String:
    type: ClassVar = SimpleString("string")
    v: bytes
    expiration: float | None = None


class List:
    type: ClassVar = SimpleString("list")

    def __init__(self) -> None:
        self.l: list[bytes] = []

    def rpush(self, xs: list[bytes]) -> int:
        self.l.extend(xs)
        return len(self.l)

    def lrange(self, l: int, r: int) -> list[bytes]:
        inclusive_r = None if r == -1 else r + 1
        return self.l[l:inclusive_r]

    def lpush(self, xs: list[bytes]) -> int:
        self.l[:0] = reversed(xs)
        return len(self.l)

    def llen(self) -> int:
        return len(self.l)

    def lpop(self) -> bytes | NullString:
        try:
            return self.l.pop(0)
        except IndexError:
            return NullString()

    def lpop_many(self, n: int) -> list[bytes] | NullString:
        if not self.l:
            return NullString()
        res = self.l[:n]
        del self.l[:n]
        return res

    @property
    def empty(self) -> bool:
        return not self.l


@dataclasses.dataclass(frozen=True, slots=True, order=True)
class StreamId:
    time: int
    sequence: int

    def __bytes__(self) -> bytes:
        return f"{self.time}-{self.sequence}".encode()


@dataclasses.dataclass(frozen=True, slots=True)
class StreamEntry:
    id: StreamId
    kv: list[bytes]


class Stream:
    type: ClassVar = SimpleString("stream")

    def __init__(self) -> None:
        self.l: list[StreamEntry] = []

    def _generate_id(self, id_or_template: bytes) -> StreamId:
        if id_or_template == b"*":
            now = int(time.time() * 1000)
            return self._generate_sequence_number(now)
        assert b"-" in id_or_template
        a, b = id_or_template.split(b"-", 1)
        if b == b"*":
            return self._generate_sequence_number(a)
        return self._validate_full_id(a, b)

    def _validate_full_id(self, a: bytes, b: bytes) -> StreamId:
        ai, bi = int(a), int(b)
        new = StreamId(ai, bi)
        if new <= StreamId(0, 0):
            raise RedisError("The ID specified in XADD must be greater than 0-0")
        if self.l:
            old = self.l[-1].id
            if new <= old:
                raise RedisError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )
        return new

    def _generate_sequence_number(self, a: bytes | int) -> StreamId:
        ai = int(a)
        old = StreamId(0, 0)
        if self.l:
            old = self.l[-1].id
        if old.time >= ai:
            return StreamId(old.time, old.sequence + 1)
        return StreamId(ai, 0)

    def xadd(self, id_or_template: bytes, kv: list[bytes]) -> bytes:
        generated_id = self._generate_id(id_or_template)
        self.l.append(StreamEntry(generated_id, kv))
        return bytes(generated_id)

    def xrange(self, start: bytes, end: bytes) -> list[tuple[bytes, list[bytes]]]:
        get_id: Callable[[StreamEntry], StreamId] = lambda x: x.id
        get_id_time: Callable[[StreamEntry], int] = lambda x: x.id.time
        
        if b"-" not in start:
            start_id = StreamId(int(start), 0)
        else:
            start_id = StreamId(*map(int, start.split(b"-", 1)))
        start_idx = bisect.bisect_left(self.l, start_id, key=get_id)

        if b"-" not in end:
            end_time = int(end)
            end_idx = bisect.bisect_right(self.l, end_time, key=get_id_time)
        else:
            end_id = StreamId(*map(int, end.split(b"-", 1)))
            end_idx = bisect.bisect_right(self.l, end_id, key=get_id)
        return [
            (bytes(x.id), x.kv) for x in itertools.islice(self.l, start_idx, end_idx)
        ]


type Value = String | List | Stream


class BlockingRequest(abc.ABC):
    def __init__(self) -> None:
        self.future = asyncio.get_running_loop().create_future()

    @abc.abstractmethod
    def notify(self, v: Value) -> bool: ...


class BLPopRequest(BlockingRequest):
    def notify(self, v: Value) -> bool:
        match v:
            case List(empty=False):
                self.future.set_result(v.lpop())
                return True
        return False


class BlockingDispatcher:
    def __init__(self) -> None:
        self.kv: dict[bytes, list[BlockingRequest]] = {}

    def add_block_request(self, k: bytes, request: BlockingRequest) -> None:
        l = self.kv.setdefault(k, [])
        l.append(request)

    def remove_block_request(self, k: bytes, request: BlockingRequest) -> None:
        l = self.kv.setdefault(k, [])
        l.remove(request)

    def notify_key(self, k: bytes, v: Value) -> None:
        l = self.kv.get(k, [])
        for i, r in enumerate(l):
            if r.notify(v):
                break
        else:
            return
        del l[i]


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

    def xrange(
        self, k: bytes, start: bytes, end: bytes
    ) -> list[tuple[bytes, list[bytes]]]:
        v = self.kv.setdefault(k, Stream())
        if not isinstance(v, Stream):
            raise RedisError(f"key {k!r} is not a stream: {v!r}")
        return v.xrange(start, end)
