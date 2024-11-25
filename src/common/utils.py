from typing import Iterable, Generator


def batches(iterable: Iterable, batch_size=1) -> Generator[Iterable, None, None]:
    """Generator function yields batches of the original iterable in sizes of batch_size"""
    length = len(iterable)
    for index in range(0, length, batch_size):
        yield iterable[index : min(index + batch_size, length)], index / batch_size

