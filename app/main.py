import asyncio
import contextvars
import copy
import logging
import sys
from datetime import timedelta
from typing import Final

from app.keywords import Keyword
from app.protocol import encode, read_command, RedisError
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
        kw = Keyword
        async for command in read_command(reader):
            logger.debug("Received %r", command)
            try:
                match command:
                    case [kw.PING]:
                        writer.write(b"+PONG\r\n")
                    case [kw.ECHO, x]:
                        writer.writelines(encode(x))
                    case [kw.SET, k, x]:
                        self.storage.set(k, x)
                        writer.write(b"+OK\r\n")
                    case [kw.SET, k, x, kw.EX, s]:
                        self.storage.set(k, x, ttl=timedelta(seconds=int(s)))
                        writer.write(b"+OK\r\n")
                    case [kw.SET, k, x, kw.PX, ms]:
                        self.storage.set(k, x, ttl=timedelta(milliseconds=int(ms)))
                        writer.write(b"+OK\r\n")
                    case [kw.GET, k]:
                        writer.writelines(encode(self.storage.get(k)))
                    case [kw.RPUSH, k, x, *xs]:
                        writer.writelines(encode(self.storage.rpush(k, [x, *xs])))
                    case [kw.LRANGE, k, l, r]:
                        writer.writelines(encode(self.storage.lrange(k, l, r)))
                    case [kw.LPUSH, k, x, *xs]:
                        writer.writelines(encode(self.storage.lpush(k, [x, *xs])))
                    case [kw.LLEN, k]:
                        writer.writelines(encode(self.storage.llen(k)))
                    case [kw.LPOP, k]:
                        writer.writelines(encode(self.storage.lpop(k)))
                    case [kw.LPOP, k, n]:
                        writer.writelines(encode(self.storage.lpop_many(k, n)))
                    case [kw.BLPOP, k, b"0"]:
                        writer.writelines(encode(await self.storage.blpop(k)))
                    case _:
                        raise RedisError("unknown command")
            except RedisError as e:
                logger.exception("returning error")
                writer.writelines(encode(e))
            await writer.drain()
        logger.info("Client disconnected")


async def main() -> None:
    logger.info("Initializing server")
    loop = asyncio.get_running_loop()
    storage = Storage(loop)
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

    asyncio.run(
        main(),
        debug=True,
    )
