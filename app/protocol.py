import asyncio
import functools
from collections.abc import AsyncGenerator, Generator
from typing import Final, Sequence, Self

CRLF: Final = b"\r\n"
type Command = list[bytes]


class NullString:
    __slots__ = ()

    def __repr__(self) -> str:
        return "(nil)"


class NullArray:
    __slots__ = ()

    def __repr__(self) -> str:
        return "(nil)"


class RedisError(Exception):
    def __init__(self, message: str):
        self.message: Final = message


class SimpleString(str):
    def __new__(cls, s: str) -> Self:
        assert "\n" not in s and "\r" not in s
        return super().__new__(cls, s)


async def read_command(reader: asyncio.StreamReader) -> AsyncGenerator[Command]:
    while True:
        star = await reader.read(1)
        if not star:
            break
        assert star == b"*"

        array_len = int(await reader.readuntil(CRLF))
        assert array_len > 0

        command: Command = []
        for _ in range(array_len):
            assert await reader.readexactly(1) == b"$"
            str_len = int(await reader.readuntil(CRLF))
            assert str_len >= 0
            command.append(await reader.readexactly(str_len))
            assert await reader.readexactly(2) == CRLF

        yield command


type Primitive = bytes | NullString | int | SimpleString
type Encodeable = RedisError | Primitive | Sequence[Primitive] | NullArray


@functools.singledispatch
def encode(x: Encodeable) -> Generator[bytes]:
    raise NotImplementedError


@encode.register
def _(x: bytes) -> Generator[bytes]:
    yield b"$"
    yield str(len(x)).encode()
    yield CRLF
    yield x
    yield CRLF


@encode.register
def _(x: NullString) -> Generator[bytes]:
    yield b"$-1\r\n"


@encode.register
def _(x: RedisError) -> Generator[bytes]:
    yield b"-ERR"
    yield x.message.encode()
    yield CRLF


@encode.register
def _(x: int) -> Generator[bytes]:
    yield b":"
    yield str(x).encode()
    yield CRLF


@encode.register(list)
@encode.register(tuple)
def _(xs: Sequence[Primitive]) -> Generator[bytes]:
    yield b"*"
    yield str(len(xs)).encode()
    yield CRLF
    for x in xs:
        yield from encode(x)


@encode.register
def _(x: NullArray) -> Generator[bytes]:
    yield b"*-1\r\n"


@encode.register
def _(x: SimpleString) -> Generator[bytes]:
    yield b"+"
    yield x.encode()
    yield CRLF
