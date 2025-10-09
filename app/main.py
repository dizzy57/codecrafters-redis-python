import asyncio
import logging
import sys
from typing import Final

logger: Final = logging.getLogger(__name__)


async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    logger.info("Client connected")
    writer.write(b"+PONG\r\n")
    await writer.drain()


async def main() -> None:
    logger.info("Starting server")
    server = await asyncio.start_server(handler, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname).1s%(asctime)s.%(msecs)03d %(process)5d %(filename)s:%(lineno)d] %(message)s",
        datefmt="%m%d %H:%M:%S",
        level="INFO",
        stream=sys.stdout,
    )
    asyncio.run(main())
