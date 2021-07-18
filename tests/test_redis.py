
import asyncio
from tests.fixtures import pytestmark
import pytest
import aioredis
from aioredis.client import Redis

@pytest.fixture
async def redis() -> Redis:
  r = aioredis.from_url('redis://localhost', retry_on_timeout=False)
  yield r
  await r.close()
  await asyncio.sleep(0.001)


async def test_redis_1(redis: Redis):

  whoami = await redis.acl_whoami()
  print(whoami)
  acls = await redis.acl_list()
  print(acls)

  # Update a kv and append a string to the end of v
  # redis.append()

  id_ = await redis.client_id()
  print(id_)

  if 0:
    config = await redis.config_get()
    print(config)

  # get a value from a hash, name[key]

  try:
    config = await redis.hgetall('config')
    print(config)
  except Exception as e:
    print(e)

  await redis.hset('config', mapping={'key': 'value', 'num': 5})
  value = await redis.hget('config', 'key')
  num = await redis.hget('config', 'num')
  await redis.hset('config', 'num', 1)
  assert b'1' == await redis.hget('config', 'num')
  print(value, num)
  config = await redis.hgetall('config')
  print(config)
