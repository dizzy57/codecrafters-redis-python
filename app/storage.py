import asyncio
import dataclasses
import datetime
import logging
from typing import Final

from app.protocol import NullString, RedisError

logger: Final = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


@dataclasses.dataclass(slots=True, frozen=True)
class String:
    v: bytes
    expiration: float | None = None


class List:
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


type Value = String | List


class Storage:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.kv: dict[bytes, Value] = {}

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
