import os
from shared.cache import CachePersistence, RedisCachePersistence, InMemoryCachePersistence
from redis import Redis

redis_configured = all(
    [os.environ.get(v) is not None for v in ["REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD"]]
)

cache_persistence: CachePersistence = None

if not redis_configured:
    print("****************************************************************************")  # noqa: T201
    print('Redis not configured for cache persistence, using in-memory "persistence".')  # noqa: T201
    print("Every process restart will therefore result in an empty cache.")  # noqa: T201
    print("****************************************************************************")  # noqa: T201
    cache_persistence = InMemoryCachePersistence()
else:
    redis_client = Redis(
        host=os.environ.get("REDIS_HOST"),
        port=int(os.environ.get("REDIS_PORT")),
        db=int(os.environ.get("REDIS_DB")),
        password=os.environ.get("REDIS_PASSWORD"),
    )
    cache_persistence = RedisCachePersistence(redis_client)
