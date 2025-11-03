from typing import ClassVar

from app.protocol import SimpleString, NullString


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
