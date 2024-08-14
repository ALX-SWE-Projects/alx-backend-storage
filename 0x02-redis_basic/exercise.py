#!/usr/bin/env python3
"""
A module for using Redis as a NoSQL data storage with a Cache class.
It includes functionalities for tracking method calls and their history.
"""

import uuid
import redis
from functools import wraps
from typing import Any, Callable, Union


def count_calls(method: Callable) -> Callable:
    """
    Decorator that counts the number of times a method is called.
    """
    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        """
        Invokes the given method after incrementing its call counter.
        """
        if isinstance(self._redis, redis.Redis):
            self._redis.incr(method.__qualname__)
        return method(self, *args, **kwargs)
    return invoker


def call_history(method: Callable) -> Callable:
    """
    Decorator that tracks the call history (inputs and outputs) of a method.
    """
    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        """
        Stores the method's inputs and output in Redis, then returns the output.
        """
        in_key = f"{method.__qualname__}:inputs"
        out_key = f"{method.__qualname__}:outputs"
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(in_key, str(args))
        output = method(self, *args, **kwargs)
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(out_key, output)
        return output
    return invoker


def replay(fn: Callable) -> None:
    """
    Displays the call history of a Cache class' method, including the inputs
    and outputs for each call.
    """
    if not fn or not hasattr(fn, '__self__'):
        return
    redis_store = getattr(fn.__self__, '_redis', None)
    if not isinstance(redis_store, redis.Redis):
        return

    fxn_name = fn.__qualname__
    in_key = f"{fxn_name}:inputs"
    out_key = f"{fxn_name}:outputs"

    # Fetch the number of times the function was called
    fxn_call_count = int(redis_store.get(fxn_name) or 0)
    print(f"{fxn_name} was called {fxn_call_count} times:")

    # Retrieve inputs and outputs
    fxn_inputs = redis_store.lrange(in_key, 0, -1)
    fxn_outputs = redis_store.lrange(out_key, 0, -1)

    for fxn_input, fxn_output in zip(fxn_inputs, fxn_outputs):
        print(f"{fxn_name}(*{fxn_input.decode('utf-8')}) -> {fxn_output}")


class Cache:
    """
    Represents a Cache that stores data in Redis and tracks method calls.
    """

    def __init__(self) -> None:
        """
        Initializes the Cache instance with a Redis client and flushes the database.
        """
        self._redis = redis.Redis()
        self._redis.flushdb()

    @call_history
    @count_calls
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Stores a value in Redis with a randomly generated key and returns the key.

        Args:
            data (Union[str, bytes, int, float]): The data to store in Redis.

        Returns:
            str: The generated key where the data is stored.
        """
        data_key = str(uuid.uuid4())
        self._redis.set(data_key, data)
        return data_key

    def get(self, key: str, fn: Callable = None) -> Union[str, bytes, int, float]:
        """
        Retrieves a value from Redis by key, optionally applying a conversion function.

        Args:
            key (str): The key of the data to retrieve.
            fn (Callable, optional): A function to apply to the retrieved data.

        Returns:
            Union[str, bytes, int, float]: The retrieved data or None.
        """
        data = self._redis.get(key)
        return fn(data) if fn else data

    def get_str(self, key: str) -> str:
        """
        Retrieves a string value from Redis by key.

        Args:
            key (str): The key of the data to retrieve.

        Returns:
            str: The retrieved string data.
        """
        return self.get(key, lambda x: x.decode('utf-8'))

    def get_int(self, key: str) -> int:
        """
        Retrieves an integer value from Redis by key.

        Args:
            key (str): The key of the data to retrieve.

        Returns:
            int: The retrieved integer data.
        """
        return self.get(key, lambda x: int(x))
