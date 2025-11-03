import asyncio
import bisect
import dataclasses
import itertools
import time
from typing import ClassVar

from app.protocol import SimpleString, RedisError

type StreamOutput = list[tuple[bytes, list[bytes]]]


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


def entry_id(x: StreamEntry) -> StreamId:
    return x.id


def entry_id_time(x: StreamEntry) -> int:
    return x.id.time


class Stream:
    type: ClassVar = SimpleString("stream")

    def __init__(self) -> None:
        self.l: list[StreamEntry] = []
        self.ev = asyncio.Event()

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
        self.ev.set()
        self.ev.clear()
        return bytes(generated_id)

    def xrange(self, start: bytes, end: bytes) -> StreamOutput:
        if start == b"-":
            start_idx = 0
        elif b"-" not in start:
            start_time = int(start)
            start_idx = bisect.bisect_left(self.l, start_time, key=entry_id_time)
        else:
            start_id = StreamId(*map(int, start.split(b"-", 1)))
            start_idx = bisect.bisect_left(self.l, start_id, key=entry_id)

        if end == b"+":
            end_idx = None
        elif b"-" not in end:
            end_time = int(end)
            end_idx = bisect.bisect_right(self.l, end_time, key=entry_id_time)
        else:
            end_id = StreamId(*map(int, end.split(b"-", 1)))
            end_idx = bisect.bisect_right(self.l, end_id, key=entry_id)
        return [
            (bytes(x.id), x.kv) for x in itertools.islice(self.l, start_idx, end_idx)
        ]

    def xread(self, start: bytes) -> StreamOutput:
        start_id = StreamId(*map(int, start.split(b"-", 1)))
        start_idx = bisect.bisect_right(self.l, start_id, key=entry_id)
        return [(bytes(x.id), x.kv) for x in itertools.islice(self.l, start_idx, None)]

    def last_id(self) -> StreamId:
        if not self.l:
            return StreamId(0, 0)
        return self.l[-1].id
