import struct
from datetime import datetime
from typing import Any
from uuid import UUID

import msgpack  # type: ignore

DATETIME_EXT_ID = 23
UUID_EXT_ID = 25


def ext_default(data: Any) -> msgpack.ExtType:
  if isinstance(data, datetime):
    # This will lose TZ information, not a perfect serialization
    data = struct.pack('d', data.timestamp())
    return msgpack.ExtType(DATETIME_EXT_ID, data)
  if isinstance(data, UUID):
    return msgpack.ExtType(UUID_EXT_ID, data.bytes)

  raise TypeError(f'cannot encode {type(data)}')


def ext_hook(code: int, data: bytes) -> Any:
  if code == DATETIME_EXT_ID:
    return datetime.fromtimestamp(struct.unpack('d', data)[0])
  if code == UUID_EXT_ID:
    return UUID(bytes=data)

  raise TypeError(f'cannot decode field {code}')


def pack_ext(data: Any) -> bytes:
  return msgpack.packb(data, default=ext_default, use_bin_type=True)


def unpack_ext(data: bytes) -> Any:
  return msgpack.unpackb(data, ext_hook=ext_hook, raw=False)
