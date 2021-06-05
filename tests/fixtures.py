import asyncio
import logging
import signal
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Tuple

import pytest

log = logging.getLogger(__name__)

pytestmark = pytest.mark.asyncio


class AsyncTimer:
  start: float
  end: float
  total: float
  count: int
  func: Callable[[], Awaitable]
  args: Tuple[Any]
  error_count: int
  error: Exception

  def __init__(self, func: Callable[[], Awaitable], *args, count: int = 1):
    self.start = 0
    self.end = 0
    self.total = 0
    self.count = count
    self.func = func
    self.args = args
    self.error_count = 0
    self.error = None

  async def timeit(self):
    level = logging.getLogger().level
    logging.getLogger().setLevel(logging.ERROR)
    loop = asyncio.get_event_loop()
    self.start = loop.time()
    func = self.func
    args = self.args
    for i in range(0, self.count):
      try:
        await func(*args)
      except Exception as e:
        self.error_count += 1
        self.error = e
    self.end = loop.time()
    logging.getLogger().setLevel(level)
    self.total = self.end - self.start

  @property
  def per_call(self) -> float:
    return self.total / self.count

  @property
  def explain(self) -> str:
    e = ''
    if self.error_count:
      e = f', errors: {self.error_count}'
    return f'calls: {self.count}, c/s: {self.per_call}, total: {self.total}{e}'


class Sighandler:
  terminated: bool

  def __init__(self, *signals):
    if not signals:
      signals = (signal.SIGINT, signal.SIGTERM)
    self.terminated = False
    for sig in signals:
      signal.signal(sig, self.on_signal)

  def on_signal(self, *args):
    self.terminated = True


@pytest.fixture
async def cleanup():
  holder = []
  yield holder.append
  for func in holder:
    try:
      coro = func()
      if isawaitable(coro):
        await coro
    except:
      log.exception('In cleanup %s', func)
  await asyncio.sleep(0.01)
  await asyncio.sleep(0.01)
