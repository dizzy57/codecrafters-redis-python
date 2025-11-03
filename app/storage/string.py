import dataclasses
from typing import ClassVar

from app.protocol import SimpleString


@dataclasses.dataclass(slots=True, frozen=True)
class String:
    type: ClassVar = SimpleString("string")
    v: bytes
    expiration: float | None = None
