# app/patches.py
# Unified patches for AsyncGenerator typing and PointLoader behavior.

import typing
import collections.abc
import typing_extensions

# Normalize AsyncGenerator across typing variants.
typing.AsyncGenerator = collections.abc.AsyncGenerator
typing_extensions.AsyncGenerator = collections.abc.AsyncGenerator
collections.abc.AsyncGenerator._name = "AsyncGenerator"

# PointLoader monkey-patch: parse "(x,y)" bytes → Point(x, y).
import db.types
from db.types import PointLoader, Buffer, Point  # type: ignore

def _patched_point_load(self, data: Buffer):
    # Accept bytes-like buffer, decode, parse "(<x>,<y>)"; return None on empty.
    raw = data.tobytes() if hasattr(data, "tobytes") else data
    text = raw.decode()
    if text:
        try:
            x_str, y_str = text.strip("()").split(",")
            return Point(float(x_str), float(y_str))
        except Exception as e:
            raise ValueError(f"Invalid point format: {text}") from e
    return None

# Activate patch.
PointLoader.load = _patched_point_load  # type: ignore
