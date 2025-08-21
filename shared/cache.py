import hashlib
import inspect
import pickle
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

F = TypeVar('F', bound=Callable[..., Any])


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
    
    def __init__(self, redis_client: Any, ttl: Optional[int] = None) -> None:
        """
        Initialize Redis cache persistence.
        
        Args:
            redis_client: Redis client instance
            ttl: Time to live in seconds for cached values
        """
        self.redis = redis_client
        self.ttl = ttl
    
    def get(self, key: str) -> Optional[Any]:
        try:
            data = self.redis.get(key)
            if data is None:
                return None
            return pickle.loads(data)
        except Exception:
            return None
    
    def set(self, key: str, value: Any) -> None:
        try:
            serialized = pickle.dumps(value)
            if self.ttl:
                self.redis.setex(key, self.ttl, serialized)
            else:
                self.redis.set(key, serialized)
        except Exception:
            pass
    
    def clear(self) -> None:
        try:
            self.redis.flushdb()
        except Exception:
            pass


def _generate_cache_key(func: Callable[..., Any], args: tuple, kwargs: Dict[str, Any]) -> str:
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
    
    # Serialize args and kwargs for hashing
    try:
        serialized_args = pickle.dumps(args)
        serialized_kwargs = pickle.dumps(sorted(kwargs.items()))
    except (pickle.PicklingError, TypeError):
        # Fallback to string representation if pickling fails
        serialized_args = str(args).encode('utf-8')
        serialized_kwargs = str(sorted(kwargs.items())).encode('utf-8')
    
    # Generate SHA256 hash from function body and parameters
    hasher = hashlib.sha256()
    
    # Hash function body (most important for cache invalidation)
    hasher.update(func_body.encode('utf-8'))
    
    # Hash function parameters
    if isinstance(serialized_args, bytes):
        hasher.update(serialized_args)
    else:
        hasher.update(str(serialized_args).encode('utf-8'))
    
    if isinstance(serialized_kwargs, bytes):
        hasher.update(serialized_kwargs)
    else:
        hasher.update(str(serialized_kwargs).encode('utf-8'))
    
    return hasher.hexdigest()


def cache(
    persistence: Optional[CachePersistence] = None,
    ignore_args: Optional[List[str]] = None
) -> Callable[[F], F]:
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
    
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Filter out ignored arguments for cache key generation
            filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ignore_args}
            
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
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            persistence.set(cache_key, result)
            
            return result
        
        # Add cache management methods to the wrapper
        wrapper.cache_clear = persistence.clear
        wrapper.cache_info = lambda: {"persistence": type(persistence).__name__}  # type: ignore
        
        return wrapper
    
    return decorator