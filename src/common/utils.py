from typing import Iterable, Generator, Tuple
import itertools


def batches[T](iterable: Iterable[T], batch_size: int = 1) -> Generator[Tuple[Iterable[T], int], None, None]:
    """Generator function yields batches of the original iterable in sizes of batch_size"""
    batch_number: int = 1
    for batch in itertools.batched(iterable, batch_size):
        yield batch, batch_number
        batch_number += 1

