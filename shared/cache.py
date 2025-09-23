import logging
import os
import hashlib
import inspect
import pickle
import contextvars
from redis import Redis
from abc import ABC, abstractmethod
from functools import wraps
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, cast, ParamSpec, Generator

P = ParamSpec("P")
T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

CACHE_DEBUG = os.environ.get("CACHE_DEBUG", "false").lower() in ["true", "1"]

# Context variables for cache logging
cache_logger: contextvars.ContextVar[Optional[logging.Logger]] = contextvars.ContextVar("cache_logger", default=None)
cache_log_prefix: contextvars.ContextVar[str] = contextvars.ContextVar("cache_log_prefix", default="")


class CachePersistence(ABC):
    """Abstract base class for cache persistence implementations."""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from cache by key."""
        pass

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Store a value in cache with the given key."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all cached values."""
        pass


class InMemoryCachePersistence(CachePersistence):
    """In-memory cache persistence for testing and simple use cases."""

    def __init__(self) -> None:
        self._cache: Dict[str, Any] = {}

    def get(self, key: str) -> Optional[Any]:
        return self._cache.get(key)

    def set(self, key: str, value: Any) -> None:
        self._cache[key] = value

    def clear(self) -> None:
        self._cache.clear()


class RedisCachePersistence(CachePersistence):
    """Redis-based cache persistence."""

    def __init__(self, redis_client: Redis, logger: logging.Logger, ttl: Optional[int] = None) -> None:
        """
        Initialize Redis cache persistence.

        Args:
            redis_client: Redis client instance
            ttl: Time to live in seconds for cached values
        """
        self.redis = redis_client
        self.ttl = ttl
        self.logger = logger

    def get(self, key: str) -> Optional[Any]:
        try:
            data: Any = self.redis.get(key)
            if data is None:
                return None
            return pickle.loads(data)  # noqa: S301
        except Exception as e:
            self.logger.error(f"RedisCachePersistence caught exception while trying to load {key}: {e}")
            return None

    def set(self, key: str, value: Any) -> None:
        try:
            serialized = pickle.dumps(value)
            if self.ttl:
                self.redis.setex(key, self.ttl, serialized)
            else:
                self.redis.set(key, serialized)
        except Exception:
            self.logger.error(f"RedisCachePersistence failed to store {key}")
            pass

    def clear(self) -> None:
        try:
            self.redis.flushdb()
        except Exception as e:
            self.logger.error(f"RedisCachePersistence failed to clear cache: {e}")
            pass


def _generate_cache_key(func: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> str:
    """
    Generate a cache key based on function body and parameters.

    Args:
        func: The function being cached
        args: Positional arguments passed to the function
        kwargs: Keyword arguments passed to the function

    Returns:
        SHA256 hash string to use as cache key
    """
    # Get function source code (body)
    try:
        func_body = inspect.getsource(func)
    except OSError:
        # Fallback for built-in or C functions - use bytecode
        try:
            func_body = func.__code__.co_code.hex()
        except AttributeError:
            # Last resort fallback
            func_body = f"{func.__module__}.{func.__name__}"

    # Serialize args and kwargs for hashing using deterministic approach
    def _sort_recursively(obj: Any) -> Any:
        """Recursively sort dictionary keys at all levels to ensure deterministic serialization."""
        if isinstance(obj, dict):
            return {k: _sort_recursively(v) for k, v in sorted(obj.items())}
        elif isinstance(obj, list):
            return [_sort_recursively(item) for item in obj]
        else:
            return obj

    def _serialize_deterministically(obj: Any) -> bytes:
        """Serialize objects deterministically for consistent cache keys."""
        try:
            # Try to use the object's dict representation if it has one (works for most objects)
            if hasattr(obj, "__dict__"):
                # Sort dict items to ensure consistent ordering
                obj_dict = _sort_recursively(dict(obj.__dict__.items()))
                return str(obj_dict).encode("utf-8")
            # For objects with model_dump method (like Pydantic models)
            elif hasattr(obj, "model_dump"):
                # Use model_dump for consistent serialization
                dumped = obj.model_dump()
                sorted_dumped = _sort_recursively(dumped) if isinstance(dumped, (dict, list)) else dumped
                return str(sorted_dumped).encode("utf-8")
            # For basic types, use string representation
            else:
                return str(obj).encode("utf-8")
        except Exception:
            # Final fallback to string representation
            return str(obj).encode("utf-8")

    # Serialize each argument individually for better determinism
    serialized_args = b""
    for arg in args:
        serialized_args += _serialize_deterministically(arg)

    serialized_kwargs = b""
    for key in sorted(kwargs.keys()):
        serialized_kwargs += key.encode("utf-8") + _serialize_deterministically(kwargs[key])

    # Generate SHA256 hash from function body and parameters
    hasher = hashlib.sha256()

    # Hash function body (most important for cache invalidation)
    hasher.update(func_body.encode("utf-8"))

    # Hash function parameters (serialized_args and serialized_kwargs are now always bytes)
    hasher.update(serialized_args)
    hasher.update(serialized_kwargs)

    hexdigest = hasher.hexdigest()

    return hexdigest


def set_cache_logging_context(logger: logging.Logger, log_prefix: str = "") -> None:
    """Set the logging context for cache operations.

    Args:
        logger: Logger instance to use for cache debug messages
        log_prefix: Prefix to add to log messages
    """
    cache_logger.set(logger)
    cache_log_prefix.set(log_prefix)


def get_cache_logging_context() -> tuple[Optional[logging.Logger], str]:
    """Get the current cache logging context.

    Returns:
        Tuple of (logger, log_prefix)
    """
    return cache_logger.get(), cache_log_prefix.get()


@contextmanager
def cache_logging_context(logger: logging.Logger, log_prefix: str = "") -> Generator[None, None, None]:
    """Context manager for setting cache logging context.

    Args:
        logger: Logger instance to use for cache debug messages
        log_prefix: Prefix to add to log messages

    Example:
        with cache_logging_context(my_logger, "request-123"):
            result = cached_function(42)  # Uses the provided logger and prefix
    """
    # Save current context
    old_logger = cache_logger.get()
    old_prefix = cache_log_prefix.get()

    try:
        # Set new context
        cache_logger.set(logger)
        cache_log_prefix.set(log_prefix)
        yield
    finally:
        # Restore previous context
        cache_logger.set(old_logger)
        cache_log_prefix.set(old_prefix)


def cache(
    persistence: Optional[CachePersistence] = None, ignore_args: Optional[List[str]] = None
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator that caches function results based on function body and parameters.

    Args:
        persistence: Cache persistence implementation. Defaults to InMemoryCachePersistence.
        ignore_args: List of argument names to ignore when generating cache key.

    Returns:
        Decorated function with caching capability.
    """
    if persistence is None:
        persistence = InMemoryCachePersistence()

    ignore_args = ignore_args or []

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Filter out ignored arguments for cache key generation
            filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ignore_args}

            logger = cache_logger.get()
            log_prefix = cache_log_prefix.get()

            # Handle ignored positional arguments
            sig = inspect.signature(func)
            param_names = list(sig.parameters.keys())
            filtered_args = []

            for i, arg in enumerate(args):
                if i < len(param_names) and param_names[i] not in ignore_args:
                    filtered_args.append(arg)
                elif i >= len(param_names):
                    # Handle *args case
                    filtered_args.append(arg)

            # Generate cache key based on function body and filtered parameters
            cache_key = _generate_cache_key(func, tuple(filtered_args), filtered_kwargs)

            # Try to get cached result
            cached_result = persistence.get(cache_key)
            if cached_result is not None:
                if CACHE_DEBUG and logger is not None:
                    logger.debug(f"{log_prefix} cache hit for key: {cache_key}")  # noqa: T201
                return cached_result

            # Execute function and cache result
            if CACHE_DEBUG and logger is not None:
                logger.debug(f"{log_prefix} cache miss for key: {cache_key}")  # noqa: T201
            result = func(*args, **kwargs)
            persistence.set(cache_key, result)

            return result

        # Add cache management methods to the wrapper
        wrapper.cache_clear = persistence.clear  # type: ignore[attr-defined]
        wrapper.cache_info = lambda: {"persistence": type(persistence).__name__}  # type: ignore[attr-defined]
        wrapper.fn = func  # type: ignore[attr-defined]

        return cast(Callable[P, T], wrapper)

    return decorator
