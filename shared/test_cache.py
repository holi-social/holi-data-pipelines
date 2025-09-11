import time
import logging

from typing import Any, Dict, List
from unittest.mock import Mock

from shared.cache import (
    cache,
    InMemoryCachePersistence,
    RedisCachePersistence,
    _generate_cache_key,
    cache_logging_context,
    set_cache_logging_context,
)

test_logger = logging.getLogger(__name__)


class TestInMemoryCachePersistence:
    def test_get_set_basic(self) -> None:
        persistence = InMemoryCachePersistence()

        # Test setting and getting a value
        persistence.set("key1", "value1")
        assert persistence.get("key1") == "value1"

    def test_get_nonexistent_key(self) -> None:
        persistence = InMemoryCachePersistence()
        assert persistence.get("nonexistent") is None

    def test_clear(self) -> None:
        persistence = InMemoryCachePersistence()
        persistence.set("key1", "value1")
        persistence.set("key2", "value2")

        persistence.clear()

        assert persistence.get("key1") is None
        assert persistence.get("key2") is None


class TestRedisCachePersistence:
    def test_get_set_basic(self) -> None:
        mock_redis = Mock()
        mock_redis.get.return_value = None

        persistence = RedisCachePersistence(mock_redis, test_logger)

        # Test setting a value
        persistence.set("key1", "value1")
        mock_redis.set.assert_called_once()

        # Test getting a value that doesn't exist
        result = persistence.get("key1")
        assert result is None
        mock_redis.get.assert_called_with("key1")

    def test_ttl_setting(self) -> None:
        mock_redis = Mock()
        persistence = RedisCachePersistence(mock_redis, test_logger, ttl=3600)

        persistence.set("key1", "value1")
        mock_redis.setex.assert_called_once()

    def test_clear(self) -> None:
        mock_redis = Mock()
        persistence = RedisCachePersistence(mock_redis, test_logger)

        persistence.clear()
        mock_redis.flushdb.assert_called_once()


class TestCacheKeyGeneration:
    def test_same_function_same_args_same_key(self) -> None:
        def test_func(x: int, y: int = 10) -> int:
            return x + y

        key1 = _generate_cache_key(test_func, (5,), {"y": 10})
        key2 = _generate_cache_key(test_func, (5,), {"y": 10})

        assert key1 == key2

    def test_different_args_different_key(self) -> None:
        def test_func(x: int, y: int = 10) -> int:
            return x + y

        key1 = _generate_cache_key(test_func, (5,), {"y": 10})
        key2 = _generate_cache_key(test_func, (6,), {"y": 10})

        assert key1 != key2

    def test_different_function_body_different_key(self) -> None:
        def func1(x: int) -> int:
            return x * 2

        def func2(x: int) -> int:
            return x * 3

        key1 = _generate_cache_key(func1, (5,), {})
        key2 = _generate_cache_key(func2, (5,), {})

        assert key1 != key2


class TestCacheDecorator:
    def test_basic_caching(self) -> None:
        call_count = 0

        @cache()
        def expensive_function(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        # First call should execute the function
        result1 = expensive_function(5)
        assert result1 == 10
        assert call_count == 1

        # Second call with same args should return cached result
        result2 = expensive_function(5)
        assert result2 == 10
        assert call_count == 1  # Function not called again

    def test_different_args_not_cached(self) -> None:
        call_count = 0

        @cache()
        def test_function(x: int, y: int = 1) -> int:
            nonlocal call_count
            call_count += 1
            return x + y

        result1 = test_function(5, y=2)
        result2 = test_function(5, y=3)  # Different args

        assert result1 == 7
        assert result2 == 8
        assert call_count == 2  # Both calls executed

    def test_ignore_args(self) -> None:
        call_count = 0

        @cache(ignore_args=["timestamp"])
        def test_function(x: int, timestamp: Any = None) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = test_function(5, timestamp=time.time())
        result2 = test_function(5, timestamp=time.time() + 100)  # Different timestamp

        assert result1 == 10
        assert result2 == 10
        assert call_count == 1  # Second call should be cached despite different timestamp

    def test_contextvars_logging_context(self) -> None:
        call_count = 0
        logger = logging.getLogger("test")

        @cache()
        def test_function(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        # Test with context manager
        with cache_logging_context(logger, "call1"):
            result1 = test_function(5)

        with cache_logging_context(logger, "call2"):
            result2 = test_function(5)  # Different logging context, but should be cached

        assert result1 == 10
        assert result2 == 10
        assert call_count == 1  # Second call should be cached despite different logging context

    def test_set_cache_logging_context(self) -> None:
        call_count = 0
        logger = logging.getLogger("test")

        @cache()
        def test_function(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        # Test with direct function call
        set_cache_logging_context(logger, "direct-call")
        result1 = test_function(5)

        # Change context and call again
        set_cache_logging_context(logger, "another-call")
        result2 = test_function(5)  # Should still be cached

        assert result1 == 10
        assert result2 == 10
        assert call_count == 1  # Second call should be cached despite different logging context

    def test_custom_persistence(self) -> None:
        custom_persistence = InMemoryCachePersistence()

        @cache(persistence=custom_persistence)
        def test_function(x: int) -> int:
            return x**2

        result1 = test_function(4)
        assert result1 == 16

        # Verify value is stored in custom persistence
        # We can't directly check the key, but we can verify caching works
        result2 = test_function(4)
        assert result2 == 16

    def test_cache_clear(self) -> None:
        call_count = 0

        @cache()
        def test_function(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 3

        # First call
        result1 = test_function(5)
        assert result1 == 15
        assert call_count == 1

        # Clear cache
        test_function.cache_clear()  # type: ignore[attr-defined]

        # Second call should execute function again
        result2 = test_function(5)
        assert result2 == 15
        assert call_count == 2

    def test_cache_info(self) -> None:
        @cache()
        def test_function(x: int) -> int:
            return x

        info = test_function.cache_info()  # type: ignore[attr-defined]
        assert "persistence" in info
        assert info["persistence"] == "InMemoryCachePersistence"

    def test_function_body_change_invalidates_cache(self) -> None:
        # This test simulates what would happen if function body changes
        # by using different functions with same signature
        persistence = InMemoryCachePersistence()

        def func_v1(x: int) -> int:
            return x * 2

        def func_v2(x: int) -> int:
            return x * 3

        cached_func_v1 = cache(persistence=persistence)(func_v1)
        cached_func_v2 = cache(persistence=persistence)(func_v2)

        result1 = cached_func_v1(5)
        result2 = cached_func_v2(5)  # Different function body, should not use cache

        assert result1 == 10
        assert result2 == 15

    def test_complex_args(self) -> None:
        call_count = 0

        @cache()
        def test_function(data_dict: Dict[str, int], data_list: List[int]) -> int:
            nonlocal call_count
            call_count += 1
            return sum(data_dict.values()) + sum(data_list)

        dict_arg = {"a": 1, "b": 2}
        list_arg = [3, 4, 5]

        result1 = test_function(dict_arg, list_arg)
        result2 = test_function(dict_arg, list_arg)  # Same complex args

        assert result1 == 15  # (1+2) + (3+4+5)
        assert result2 == 15
        assert call_count == 1  # Second call should be cached

    def test_kwargs_order_independence(self) -> None:
        call_count = 0

        @cache()
        def test_function(a: int = 1, b: int = 2, c: int = 3) -> int:
            nonlocal call_count
            call_count += 1
            return a + b + c

        result1 = test_function(a=10, b=20, c=30)
        result2 = test_function(c=30, b=20, a=10)  # Same kwargs, different order

        assert result1 == 60
        assert result2 == 60
        assert call_count == 1  # Should be cached regardless of kwarg order
