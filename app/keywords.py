from enum import ReprEnum, auto
from types import NotImplementedType


class CaseInsensitiveBytesEnum(bytes, ReprEnum):
    def __eq__(self, other: object) -> NotImplementedType | bool:
        if not isinstance(other, bytes):
            return NotImplemented
        return bool(self.value == other.upper())

    @staticmethod
    def _generate_next_value_(
        name: str,
        start: object,
        count: object,
        last_values: object,
    ) -> bytes:
        return name.upper().encode()


class Keyword(CaseInsensitiveBytesEnum):
    PING = auto()
    ECHO = auto()
    SET = auto()
    GET = auto()
    EX = auto()
    PX = auto()
    RPUSH = auto()
    LRANGE = auto()
    LPUSH = auto()
    LLEN = auto()
    LPOP = auto()
    BLPOP = auto()
