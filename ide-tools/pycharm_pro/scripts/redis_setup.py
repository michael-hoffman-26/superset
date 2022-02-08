import logging
import os
from typing import cast

import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_MSG = "start flushing Redis DBs on redis://{host}:{port}"

redis_host = os.environ.get("REDIS_HOST", "localhost")
redis_port = os.environ.get("REDIS_PORT", 6379)
redis_dbs = {
    "Celery": os.environ.get("REDIS_CELERY_DB", 1),
    "Results": os.environ.get("REDIS_RESULTS_DB", 2),
    "Cache": os.environ.get("REDIS_CACHE_DB", 4),
    "global_async_queries": os.environ.get("GLOBAL_ASYNC_QUERIES_REDIS_DB", 5),
}


def flush_db(db_name: str) -> None:
    db_index = redis_dbs[db_name]
    logger.info(
        "going to flush {name} Redis db ({db_index})".format(
            name=db_name, db_index=db_index
        )
    )
    with redis.Redis(
        host=redis_host, port=cast(int, redis_port), db=cast(int, db_index)
    ) as redis_client:
        redis_client.flushdb()


logger.info(START_MSG.format(host=redis_host, port=redis_port))
for db in redis_dbs.keys():
    flush_db(db)

logger.info("flush redis DBs done")
