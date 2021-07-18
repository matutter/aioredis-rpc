import asyncio
import logging
import os
import signal
from multiprocessing import Process
from secrets import token_bytes
from typing import List

import coloredlogs
import pytest
from aioredis_rpc import (RpcError, RpcNotConnectedError, RpcProvider, create_client, create_server,
                          endpoint)
from pydantic.main import BaseModel

from .fixtures import *

coloredlogs.install(logging.DEBUG)
logging.getLogger('asyncio').setLevel(logging.ERROR)

log = logging.getLogger(__name__)

TEST_RMQ_DSN = os.environ.get("TEST_REDIS_DSN", "redis://localhost")


class Input(BaseModel):
  arg: str


class Obj(BaseModel):
  arg: int


class Return(BaseModel):
  value: bytes


class MyRpc:
  rpc: RpcProvider

  def server_arg(self, arg1):
    print(arg1)

  def server_data(self, data):
    print(data)

  def server_other(self, other):
    print(other)

  def server_o(self, o):
    print(o)

  @endpoint
  async def noreturn(self, arg1: str = None) -> None:
    print('Returned', None)
    return None

  if 0:
    # Not supported (yet)
    @endpoint
    async def returnlist1(self, arg1: str = None) -> List[str]:
      ret = []
      if arg1:
        ret = arg1.split()
      print('Returned', ret)
      return ret

  @endpoint
  async def func1(self, arg1: str, data: Input, other: str = 2, o: Obj = None) -> Return:
    print('Received', arg1, data, other, o)
    self.server_arg(arg1)
    self.server_data(data)
    self.server_other(other)
    self.server_o(o)
    value: bytes = ''.join(list(reversed(data.arg))).encode()
    ret: Return = Return(value=value)
    print('Returned', ret)
    return ret


async def test_rpc_1(cleanup):
  client = create_client(MyRpc)
  server = create_server(MyRpc)
  print('client connecting')
  await client.rpc.connect(dsn=TEST_RMQ_DSN)
  print('client connected')
  print('server connecting')
  await server.rpc.connect(dsn=TEST_RMQ_DSN)
  print('server connected')

  ret = await client.func1('abc', Input(arg='123'), other=1)
  assert ret.value == b'321'
  ret = await client.func1('abc', Input(arg='xyz'))
  assert ret.value == b'zyx'
  ret = await client.func1('abc', Input(arg='xyz'))
  assert ret.value == b'zyx'
  ret = await client.func1('abc', Input(arg='321'), o=Obj(arg=123))
  assert ret.value == b'123'
  with pytest.raises(RpcError):
    ret = await client.func1(Input(arg='321'), o=Obj(arg=123))

  ret = await client.func1(None, Input(arg='321'), o=None)
  assert ret.value == b'123'

  ret = await client.noreturn(None)
  assert ret == None

  if 0:
    # Not supported (yet)
    ret = await client.returnlist1('abc')
    assert ret == ['a', 'b', 'c']

  cleanup(client.rpc.disconnect)
  cleanup(server.rpc.disconnect)


async def test_rpc_type_support_1():

  with pytest.raises(TypeError):
    class MyUnsupportedType:
      pass

    class AnRpcClass:
      @endpoint
      async def func(var: str, unsupported: MyUnsupportedType):
        pass


async def test_subprocess_1():

  def sub():
    sh = Sighandler(signal.SIGTERM, signal.SIGINT)

    async def func():
      while not sh.terminated:
        try:
          await asyncio.sleep(1)
        except:
          break
    asyncio.run(func())

  proc = Process(target=sub)
  proc.start()
  await asyncio.sleep(2)
  proc.terminate()
  await asyncio.sleep(1)


@pytest.mark.parametrize('size, count', [
    (4096, 10),
    (4096, 100),
    # (4096, 1000),
    (4096*256, 10),
    (4096*256, 100),
    # (4096*256, 1000),
])
async def test_rpc_speed_1(cleanup, size, count):

  class MyData(BaseModel):
    data: bytes

  class RpcSpeedTest:
    @endpoint
    async def func(self, data: MyData) -> bytes:
      return data.data

  client = create_client(RpcSpeedTest)
  cleanup(client.rpc.disconnect)

  def server_proc():
    server = create_server(RpcSpeedTest)
    root = logging.getLogger()
    level = root.level
    root.setLevel(logging.INFO)
    sh = Sighandler()

    async def run():
      await server.rpc.connect()
      try:
        log.info('server is running ...')
        while not sh.terminated:
          await asyncio.sleep(1)
      finally:
        await server.rpc.disconnect()
        await asyncio.sleep(1)

    try:
      asyncio.run(run())
    finally:
      root.setLevel(level)
    exit(0)

  process = Process(target=server_proc)
  process.start()
  await client.rpc.connect()
  await asyncio.sleep(1)

  async def run(data: MyData, count: int):
    t = AsyncTimer(client.func, data, count=count)
    await t.timeit()
    print(f'size: {len(data.data)},', t.explain)
    if t.error:
      raise t.error

  data = MyData(data=token_bytes(size))
  await run(data, count)

  process.terminate()


async def test_tpc_no_server_1(cleanup):
  client = create_client(MyRpc, queue_name='no_server')
  cleanup(client.rpc.disconnect)
  print('client connecting')
  await client.rpc.connect(dsn=TEST_RMQ_DSN)
  print('client connected')

  with pytest.raises(RpcNotConnectedError):
    await client.func1('abc', Input(arg='123'), other=1)
