from datetime import datetime
from aioredis_rpc.msgpack_ext import pack_ext, unpack_ext
from uuid import uuid4

def test_datetime_codec():
  print()

  msg = { 'dt': datetime.now() }
  enc = pack_ext(msg)
  dec = unpack_ext(enc)

  print(msg)
  print(enc)
  print(dec)
  assert msg == dec

def test_uuid_codec():
  print()

  msg = { 'uuid': uuid4() }
  enc = pack_ext(msg)
  dec = unpack_ext(enc)

  print(msg)
  print(enc)
  print(dec)
  assert msg == dec

