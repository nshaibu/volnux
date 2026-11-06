import hashlib
import os
import time
import struct
import threading
from typing import Dict

from bson import ObjectId

try:
    from bson.objectid import InvalidId
except ImportError:
    from bson.errors import InvalidId


_PACK_TIMESTAMP = struct.Struct(">H").pack  # 2 bytes big-endian unsigned short
_PACK_COUNTER = struct.Struct(">I").pack  # 4 bytes; slice [1:] → 3 low bytes

_UNPACK_TYPE_ID = struct.Struct(">I").unpack  # 4-byte unsigned int
_UNPACK_TIMESTAMP = struct.Struct(">H").unpack  # 2-byte unsigned short

_TIMESTAMP_MASK = 0xFFFF  # seconds mod 2^16  (~18-hour window)
_MAX_COUNTER = 0xFFFFFF  # 3-byte counter ceiling


class TypedObjectId(ObjectId):
    """A 12-byte ObjectId whose first 4 bytes encode a stable content type.

    Always construct with ``TypedObjectId.generate(class_name)``.
    Deserialise and validate with ``TypedObjectId.from_bytes(data, class_name)``.

    Inherits all ObjectId comparison, hashing, and serialisation behaviour
    because the payload is stored in the parent's ``_ObjectId__id`` slot.
    """

    # SHA-256-derived 4-byte type identifiers, keyed by class name.
    # Written via dict.setdefault() — idempotent under concurrent writes
    # (same class name always produces the same digest).
    _type_cache: Dict[str, bytes] = {}

    # Monotonic counter, independent of ObjectId._inc.
    # Accessed only under _counter_lock.
    _counter: int = 0
    _counter_lock: threading.Lock = threading.Lock()

    def __init__(self, oid=None):
        # Bypass ObjectId.__init__ entirely when called from generate()
        # or from_bytes() — those paths set _ObjectId__id directly via
        # object.__new__ and never call __init__.
        #
        # When called with an oid argument (e.g. ObjectId round-trip,
        # pymongo deserialisation), delegate to the parent so that hex
        # strings and raw bytes are handled correctly.
        if oid is not None:
            super().__init__(oid)
        # oid=None path: do nothing — generate() sets __id before returning.

    @classmethod
    def generate(cls, class_name: str) -> "TypedObjectId":
        """Generate a new TypedObjectId for the given class name.

        Args:
            class_name: Fully qualified class name used to derive the stable
                4-byte type prefix (e.g. ``"myapp.events.SendEmail"``).

        Returns:
            A new ``TypedObjectId`` with a type-stamped 12-byte payload.
        """
        oid = object.__new__(cls)
        # Store directly into the inherited slot. This is the same slot
        # ObjectId.__generate writes to; all parent methods read from here.
        oid._ObjectId__id = cls._build_bytes(class_name)
        return oid

    @classmethod
    def from_bytes(cls, data: bytes, class_name: str) -> "TypedObjectId":
        """Deserialise and validate a 12-byte TypedObjectId payload.

        Args:
            data: Raw 12-byte ObjectId bytes (e.g. from ``oid.binary``).
            class_name: The class name whose type prefix ``data`` must match.

        Returns:
            A ``TypedObjectId`` wrapping ``data``.

        Raises:
            InvalidId: If ``data`` is not exactly 12 bytes.
            ValueError: If the type prefix does not match ``class_name``.
        """
        if len(data) != 12:
            raise InvalidId(
                "TypedObjectId requires exactly 12 bytes, got %d." % len(data)
            )
        expected = cls._compute_type_id(class_name)
        if data[:4] != expected:
            raise ValueError(
                "Type prefix mismatch for '%s': expected %s, got %s."
                % (class_name, expected.hex(), data[:4].hex())
            )
        oid = object.__new__(cls)
        oid._ObjectId__id = data
        return oid

    @property
    def type_prefix(self) -> bytes:
        """The 4-byte type identifier embedded in this ID."""
        return self._ObjectId__id[:4]

    @property
    def type_int(self) -> int:
        """The type identifier as an unsigned 32-bit integer."""
        return _UNPACK_TYPE_ID(self._ObjectId__id[:4])[0]

    @property
    def embedded_timestamp(self) -> int:
        """Seconds mod 2^16 at generation time.

        Suitable for coarse ordering within an ~18-hour window only.
        Do not use for absolute time or cross-window collision detection.
        """
        return _UNPACK_TIMESTAMP(self._ObjectId__id[4:6])[0]

    def matches_type(self, class_name: str) -> bool:
        """True if this ID's type prefix matches ``class_name``."""
        return self.type_prefix == self._compute_type_id(class_name)

    def __getstate__(self):
        return self._ObjectId__id

    def __setstate__(self, value):
        # Mirror ObjectId.__setstate__ to handle both dict form (old pymongo)
        # and raw bytes form (current pymongo).
        if isinstance(value, dict):
            self._ObjectId__id = value["_ObjectId__id"]
        else:
            self._ObjectId__id = value

    @classmethod
    def _build_bytes(cls, class_name: str) -> bytes:
        """Assemble the 12-byte ID payload.

        Layout:
            [0:4] type_id — 4-byte SHA-256 prefix of class_name
            [4:6] timestamp — 2-byte seconds mod 2^16
            [6:9] random — 3 cryptographic random bytes
            [9:12] counter — low 3 bytes of the monotonic counter, big-endian
        """
        type_id = cls._compute_type_id(class_name)
        timestamp = _PACK_TIMESTAMP(cls._current_timestamp())
        random_b = os.urandom(3)
        # _PACK_COUNTER produces 4 big-endian bytes; [1:] drops the high byte
        # leaving the low 3 bytes — sufficient for _MAX_COUNTER = 0xFFFFFF.
        counter_b = _PACK_COUNTER(cls._next_counter())[1:]

        return type_id + timestamp + random_b + counter_b

    @classmethod
    def _compute_type_id(cls, class_name: str) -> bytes:
        """Return the stable 4-byte SHA-256 prefix for ``class_name``.

        Uses dict.setdefault() for thread-safe idempotent caching:
        if two threads race on the same name, both compute the same
        digest and one write wins — the result is always correct.
        """
        cached = cls._type_cache.get(class_name)
        if cached is not None:
            return cached
        digest = hashlib.sha256(class_name.encode()).digest()[:4]
        return cls._type_cache.setdefault(class_name, digest)

    @classmethod
    def _next_counter(cls) -> int:
        """Atomically increment and return the next counter-value."""
        with cls._counter_lock:
            value = cls._counter
            cls._counter = (value + 1) % (_MAX_COUNTER + 1)
        return value

    @staticmethod
    def _current_timestamp() -> int:
        """Current time as a 2-byte-safe integer (seconds mod 2^16)."""
        return int(time.time()) & _TIMESTAMP_MASK
