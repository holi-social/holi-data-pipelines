import farmhash
from typing import cast


def farm_fingerprint(s: str | bytes) -> str:
    """
    Python implementation of FarmHash Fingerprint64 used by BigQuery.

    Args:
        s: The string to hash

    Returns:
        64-bit signed integer hash value as a string
    """
    # Convert string to bytes if it's not already
    if isinstance(s, str):
        s = s.encode("utf-8")

    # Get unsigned 64-bit integer from farmhash
    unsigned_hash = farmhash.fingerprint64(cast(str, s))

    # Convert to signed 64-bit integer (equivalent to np.int64)
    if unsigned_hash > 0x7FFFFFFFFFFFFFFF:  # If highest bit is set (negative in int64)
        signed_hash = unsigned_hash - 0x10000000000000000  # 2^64
    else:
        signed_hash = unsigned_hash

    return str(signed_hash)
