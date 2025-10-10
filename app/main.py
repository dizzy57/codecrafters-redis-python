import asyncio
import contextvars
import copy
import logging
import sys
from typing import Final

from app.protocol import encode, read_command
from app.storage import Storage

logger: Final = logging.getLogger(__name__)
ctx_client: Final = contextvars.ContextVar("client", default="SERVER")


class Redis:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    async def client_handler(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        ctx_client.set(f"client={writer.get_extra_info("peername")}")
        logger.info("Client connected")
        async for command in read_command(reader):
            logger.debug("Received %r", command)

            command[0] = command[0].upper()
            match command:
                case [b"PING"]:
                    writer.write(b"+PONG\r\n")
                case [b"ECHO", x]:
                    writer.writelines(encode(x))
                case [b"SET", k, v]:
                    self.storage.set(k, v)
                    writer.write(b"+OK\r\n")
                case [b"GET", k]:
                    writer.writelines(encode(self.storage.get(k)))
                case _:
                    writer.write(b"-ERR unknown command\r\n")
            await writer.drain()
        logger.info("Client disconnected")


async def main() -> None:
    logger.info("Initializing server")
    storage = Storage()
    redis = Redis(storage)

    logger.info("Starting server")
    server = await asyncio.start_server(redis.client_handler, "localhost", 6379)
    async with server:
        await server.serve_forever()


def filter_add_context(record: logging.LogRecord) -> logging.LogRecord:
    record = copy.copy(record)
    record.client = ctx_client.get()
    return record


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname).1s%(asctime)s.%(msecs)03d %(process)5d %(filename)s:%(lineno)d] %(client)s %(message)s",
        datefmt="%m%d %H:%M:%S",
        level="INFO",
        stream=sys.stdout,
    )
    for h in logging.getLogger().handlers:
        h.addFilter(filter_add_context)

    asyncio.run(main())
