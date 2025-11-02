import asyncio
import contextvars
import copy
import logging
import sys
from datetime import timedelta
from typing import Final

from app.keywords import Keyword
from app.protocol import encode, read_command, RedisError, SimpleString, Encodeable
from app.storage import Storage

logger: Final = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
ctx_client: Final = contextvars.ContextVar("client", default="SERVER")

PONG = SimpleString("PONG")
OK = SimpleString("OK")


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
            try:
                res = await dispatch_command(command, self.storage)
            except RedisError as e:
                logger.exception("returning error")
                res = e
            writer.writelines(encode(res))
            await writer.drain()
        logger.info("Client disconnected")


async def dispatch_command(command: list[bytes], storage: Storage) -> Encodeable:
    kw = Keyword
    match command:
        case [kw.PING]:
            return PONG
        case [kw.ECHO, x]:
            return x
        case [kw.SET, k, x]:
            storage.set(k, x)
            return OK
        case [kw.SET, k, x, kw.EX, s]:
            storage.set(k, x, ttl=timedelta(seconds=int(s)))
            return OK
        case [kw.SET, k, x, kw.PX, ms]:
            storage.set(k, x, ttl=timedelta(milliseconds=int(ms)))
            return OK
        case [kw.GET, k]:
            return storage.get(k)
        case [kw.RPUSH, k, x, *xs]:
            return storage.rpush(k, [x, *xs])
        case [kw.LRANGE, k, l, r]:
            return storage.lrange(k, l, r)
        case [kw.LPUSH, k, x, *xs]:
            return storage.lpush(k, [x, *xs])
        case [kw.LLEN, k]:
            return storage.llen(k)
        case [kw.LPOP, k]:
            return storage.lpop(k)
        case [kw.LPOP, k, n]:
            return storage.lpop_many(k, n)
        case [kw.BLPOP, k, timeout]:
            return await storage.blpop(k, timeout)
        case [kw.TYPE, k]:
            return storage.type(k)
        case [kw.XADD, k, id_or_template, *kv]:
            return storage.xadd(k, id_or_template, kv)
        case [kw.XRANGE, k, start, end]:
            return storage.xrange(k, start, end)
        case [kw.XREAD, kw.STREAMS, k, start]:
            return storage.xread(k, start)
        case _:
            raise RedisError("unknown command")


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
