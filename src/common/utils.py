def batches(iterable, batch_size=1):
    length = len(iterable)
    for index in range(0, length, batch_size):
        yield iterable[index : min(index + batch_size, length)], index / batch_size
