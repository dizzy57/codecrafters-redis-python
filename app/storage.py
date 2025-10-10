import logging
from typing import Final

from app.protocol import NullString

logger: Final = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class Storage:
    def __init__(self) -> None:
        self.kv: dict[bytes, bytes] = {}

    def set(self, k: bytes, v: bytes) -> None:
        logger.debug("SET %r %r", k, v)
        self.kv[k] = v

    def get(self, k: bytes) -> bytes | NullString:
        v = self.kv.get(k, NullString())
        logger.debug("GET %r -> %r", k, v)
        return v
