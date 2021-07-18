"""
# Start the server in the bg
python examples/simple_rpc.py server &

# Show current messages
python examples/simple_rpc.py client

# Put a few messages
python examples/simple_rpc.py client -m 'hello world'
python examples/simple_rpc.py client -m 'hello world 2'
python examples/simple_rpc.py client -m 'hello world 3'

# Show current messages
python examples/simple_rpc.py client

fg
# ctrl-c to stop server
"""

try:
  import aioredis_rpc
except:
  import os.path as op
  import sys
  sys.path.insert(0, op.join(op.dirname(__file__), '..'))

import asyncio
import gc
from datetime import datetime
from typing import List

from aioredis_rpc import RpcProvider, create_client, create_server, endpoint
from pydantic import BaseModel
from pydantic.fields import Field


class MyMsg(BaseModel):
  created_at: datetime = Field(default_factory=datetime.now)
  text: str


class MyMsgQuery(BaseModel):
  length: int
  messages: List[MyMsg]


class MyInterface:

  messages: List[MyMsg]

  def __init__(self) -> None:
    self.messages = list()

  @endpoint
  async def put_message(self, msg: MyMsg) -> bool:
    self.messages.append(msg)
    return True

  @endpoint
  async def get_messages(self) -> MyMsgQuery:
    return MyMsgQuery(length=len(self.messages), messages=self.messages)


async def main(mode: str, message: str = None):
  rpc: RpcProvider
  if mode == 'client':

    client = create_client(MyInterface)
    rpc = client.rpc
    await rpc.connect()

    if message:
      res = await client.put_message(MyMsg(text=message))
      print(res)
    else:
      query: MyMsgQuery = await client.get_messages()
      for msg in query.messages:
        print(msg)
      print('messages', query.length)

  elif mode == 'server':
    print('>', 'starting server')
    server = create_server(MyInterface)
    rpc = server.rpc

    try:
      await rpc.connect()
      while True:
        await asyncio.sleep(1)
    except KeyboardInterrupt:
      pass

  if rpc:
    await rpc.disconnect()
    # A weird quirk of using the aioredis connection pool is that all
    # connections must close and be GC'ed before the loop dies.
    await asyncio.sleep(0.01)
    gc.collect()

if __name__ == '__main__':

  from argparse import ArgumentParser
  parser = ArgumentParser(prog='simple-rpc')

  parser.add_argument('mode', choices=['server', 'client'])
  parser.add_argument(
      '-m', '--message', help='A message to send (client mode only)')
  args = parser.parse_args()

  asyncio.run(main(**vars(args)))
