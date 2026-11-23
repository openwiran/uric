"""Generic helpers around the pycrate ASN.1 runtime.

pycrate ASN.1 objects are stateful (``set_val`` / ``to_aper`` work on a shared
object) so every encode/decode goes through :data:`ASN_LOCK`, which makes the
codec safe to call from xApp threads as well as from the asyncio loop.
"""

from __future__ import annotations

import threading
from typing import Any

ASN_LOCK = threading.RLock()


class CodecError(Exception):
    """Raised when an ASN.1 value cannot be encoded or decoded."""


def aper_encode(asn_type, value) -> bytes:
    with ASN_LOCK:
        try:
            asn_type.set_val(value)
            return asn_type.to_aper()
        except Exception as exc:  # pycrate raises a variety of exception types
            raise CodecError(f"cannot encode {asn_type._name}: {exc}") from exc


def aper_decode(asn_type, data: bytes):
    with ASN_LOCK:
        try:
            asn_type.from_aper(data)
            return asn_type.get_val()
        except Exception as exc:
            raise CodecError(f"cannot decode {asn_type._name}: {exc}") from exc


def to_jsonable(value: Any) -> Any:
    """Convert a pycrate value (bytes, bit-string tuples, CHOICE tuples) to JSON.

    * bytes -> hex string
    * CHOICE ``(name, value)`` -> ``{name: value}``
    * BIT STRING ``(int, nbits)`` -> ``{"bits": int, "len": nbits}``
    """
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, set)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        if len(value) == 2 and isinstance(value[0], str):
            return {value[0]: to_jsonable(value[1])}
        if len(value) == 2 and all(isinstance(v, int) for v in value):
            return {"bits": value[0], "len": value[1]}
        return [to_jsonable(v) for v in value]
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, float) and value != value:  # NaN is not valid JSON
        return None
    return value


def real_to_float(value) -> float:
    """pycrate REAL values are (mantissa, base, exponent) tuples."""
    if isinstance(value, tuple) and len(value) == 3:
        m, b, e = value
        return float(m) * (float(b) ** e)
    return float(value)


def float_to_real(value: float):
    """Encode a python float as a pycrate REAL ``(mantissa, 2, exponent)``.

    Base 2 is used because pycrate's base 10 (NR3) encoder is unreliable.
    """
    import math
    value = float(value)
    if value == 0 or not math.isfinite(value):
        return (0, 2, 0)
    f, e = math.frexp(value)
    m = int(f * (1 << 53))
    e -= 53
    while m and m % 2 == 0:
        m //= 2
        e += 1
    return (m, 2, e)
