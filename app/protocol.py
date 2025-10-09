import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import Final

CRLF: Final = b"\r\n"
type Command = list[bytes]


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


def encode(x: bytes) -> Generator[bytes]:
    yield b"$"
    yield str(len(x)).encode()
    yield CRLF
    yield x
    yield CRLF
