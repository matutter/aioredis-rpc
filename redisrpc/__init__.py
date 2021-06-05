__version__ = "v0.1.1"

import asyncio
import logging
from asyncio.futures import Future
from inspect import FullArgSpec, getfullargspec
from typing import (Any, Awaitable, Callable, Dict, List, Optional, Type,
                    TypeVar, Union)
from uuid import uuid1

import aioredis  # type: ignore
import async_timeout
import msgpack  # type: ignore
from aioredis.client import PubSub, Redis  # type: ignore
from pydantic import BaseModel
from pydantic.fields import Field

T = TypeVar('T')

log = logging.getLogger(__name__)


class RpcErrorMessage(BaseModel):
  """
  Used to capture server-side errors so they may be returned to the client.
  """

  msg: str
  error_class: str


class RpcMessage(BaseModel):
  """
  The standard message send for each RPC call and reply.
  """

  id: str = Field(default_factory=lambda: uuid1().hex)
  # queue name to send result
  reply_to: Optional[str]
  # name of the method to call
  name: str
  # client args
  args: List[Any] = Field(default_factory=list)
  # client key word arguments
  kwargs: Dict[str, Any] = Field(default_factory=dict)
  # server result
  result: Optional[Any]

  error: Optional[RpcErrorMessage]

  @classmethod
  def from_exception(cls, e: Exception, orig: 'RpcMessage'):
    error = RpcErrorMessage(msg=str(e), error_class=type(e).__name__)
    if not orig:
      return cls(name='Error', error=error)
    return cls(id=orig.id, name=orig.name, error=error)


class RpcError(Exception):
  """
  An error thrown client-side after a server-side error has been captured and
  sent back to the client.
  """

  send: Optional[RpcMessage]
  reply: Optional[RpcMessage]

  def __init__(self, send: RpcMessage = None, reply: RpcMessage = None) -> None:
    self.send = send
    self.reply = reply
    source: str = 'rpc call'
    if send and send.name:
      source = send.name
    if reply and reply.error:
      e: RpcErrorMessage = reply.error
      msg = f'Remote RPC error: {e.error_class} {e.msg} (from {source})'
    else:
      msg = f'Unexpected RPC error while contacting {source}'
    super().__init__(msg)


class RpcProvider:
  """
  Base class for both RpcClient and RpcServer classes.
  """

  id: str
  queue_name: str
  redis: Optional[Redis]
  _channel: Optional[PubSub]
  _receiver: Optional[Awaitable]

  # Client properties
  callback_timeout: float
  callback_queue_name: Optional[str]
  pending_results: Dict[str, Future]

  # Server properties
  endpoints: Dict[str, 'Endpoint']

  def __init__(
          self,
          id: str = None,
          queue_name: str = None,
          callback_timeout: float = 10.0,
          callback_queue_name: str = None,
          endpoints: Dict[str, 'Endpoint'] = {},
          redis: Redis = None) -> None:

    self.id = id or uuid1().hex
    self.queue_name = queue_name or 'rpc-queue'
    self.callback_timeout = callback_timeout
    self.callback_queue_name = callback_queue_name
    self.endpoints = endpoints
    self.pending_results = dict()
    self.redis = redis
    self._channel = None
    self._receiver = None

  @property
  def consumer_queue_name(self) -> str:
    return self.queue_name

  @property
  def is_connected(self) -> bool:
    return bool(self.redis)

  async def _subscription_loop(self):
    while self._channel:
      try:
        async with async_timeout.timeout(1):
          message = await self._channel.get_message(ignore_subscribe_messages=True)
          if message is None:
            await asyncio.sleep(0.01)
            continue
          await self.receive(message['data'])
      except asyncio.TimeoutError:
        pass
      except (asyncio.exceptions.CancelledError, RuntimeError):
        # Runtime error is thrown on py39+ when the task is cancelled
        break
      except:
        log.exception('from %s', self.id)

  async def connect(self, dsn: str = "redis://localhost") -> None:
    if self.is_connected:
      return

    redis: Redis = aioredis.from_url(dsn)
    channel: PubSub = redis.pubsub()
    await channel.subscribe(self.consumer_queue_name)
    log.debug('%s subscribed to %s', self.id, self.consumer_queue_name)
    task = asyncio.create_task(self._subscription_loop())

    self.redis = redis
    self._channel = channel
    self._receiver = task

  async def disconnect(self) -> None:
    ch = self._channel
    reader = self._receiver
    redis = self.redis

    self.redis = None
    self._channel = None
    self._receiver = None

    if ch and reader:
      await reader
      await ch.unsubscribe()
      await ch.close()

    if redis:
      await redis.close()

  async def send(self, msg: RpcMessage) -> RpcMessage:
    pass

  async def receive(self, data: bytes) -> None:
    pass


class RpcType:
  rpc: RpcProvider


class RpcServer(RpcProvider):

  """
  The RpcServer acts as a dispatcher between the RPC subscriptions and the
  actual RPC method. It will automatically call RPC server-side Endpoints and
  send return values back to the client.
  """

  async def receive(self, data: bytes) -> None:
    if not self.redis:
      raise RuntimeError('cannot send message without redis connection')

    redis: Redis = self.redis  # type: ignore

    msg = RpcMessage.parse_obj(msgpack.unpackb(data))
    endpoint = self.endpoints[msg.name]
    try:
      response = await endpoint.receive(msg)
    except Exception as e:
      log.exception(f'%s failed to receive RPC message on %s',
                    self.id, endpoint)
      response = RpcMessage.from_exception(e, orig=msg)
    data = msgpack.packb(response.dict())
    log.debug('%s publishing to %s', self.id, msg.reply_to)
    await redis.publish(msg.reply_to, data)


class RpcClient(RpcProvider):
  """
  The RpcClient class is used by the RpcFactory to generate client-side RPC
  interfaces in the form a facade class.
  """

  @property
  def consumer_queue_name(self) -> str:
    if self.callback_queue_name:
      return self.callback_queue_name
    return self.queue_name

  async def send(self, msg: RpcMessage) -> RpcMessage:
    if not self.redis:
      raise RuntimeError('cannot send message without redis connection')

    redis: Redis = self.redis  # type: ignore

    msg.reply_to = self.callback_queue_name
    data = msgpack.packb(msg.dict())
    future: Future[RpcMessage] = Future()
    self.pending_results[msg.id] = future
    log.debug('%s publishing to %s', self.id, self.queue_name)
    await redis.publish(self.queue_name, data)
    reply: RpcMessage = await asyncio.wait_for(future, self.callback_timeout)
    if reply.error:
      log.warning('%s rpc error from %s %s', self.id, msg.name, reply.error)
      raise RpcError(send=msg, reply=reply)
    return reply

  async def receive(self, data: bytes):
    msg = RpcMessage.parse_obj(msgpack.unpackb(data))
    future = self.pending_results[msg.id]
    future.set_result(msg)


class Endpoint:
  """
  The Endpoint class wraps each individual RPC method and is responsible for
  (de)serialization from the transport format.
  """

  arg_models: Dict[int, Type[BaseModel]]
  argspec: FullArgSpec
  attribute: str = '__rpc_endpoint__'
  func: Callable[[Any], Awaitable]
  keyword_models: Dict[str, Type[BaseModel]]
  name: str
  return_type: Optional[Type]

  # Only required for client side
  provider: Optional[RpcProvider]

  def __init__(self, func: Callable, provider: RpcProvider = None) -> None:
    """
    Preparses an endpoints type hints so that methods may be (de)serialized with
    less run-time type inspection.
    """

    self.provider = provider
    self.func = func  # type: ignore
    self.name = getattr(func, '__name__', str(func))
    self.argspec: FullArgSpec = getfullargspec(func)
    self.return_type = None
    self.arg_models = dict()
    self.keyword_models = dict()

    annotations = dict(self.argspec.annotations)
    args = [a for a in self.argspec.args if a != 'self']

    for key, val in annotations.items():
      if not self._check_type_support(val):
        raise TypeError(f'unexpected type {val} for {key} of {self.name}')

    if 'return' in annotations:
      self.return_type = annotations.pop('return')

    self.keyword_models = {
        k: v for k, v in annotations.items() if issubclass(v, BaseModel)}

    for i, arg in enumerate(args):
      if arg not in annotations:
        log.warning(
            'missing type annotation for argument %s of %s', arg, self.name)
        continue
      if issubclass(annotations[arg], BaseModel):
        self.arg_models[i] = annotations.pop(arg)

  def set_provider(self, provider: RpcProvider):
    self.provider = provider

  def _check_type_support(self, type_: Type):
    if type_ is None:
      return True
    if not isinstance(type_, type):
      return False
    return issubclass(type_, (BaseModel, str, int, float, str, bytes, bool, list))

  async def send(self, *args_: Any, **kwargs: Any) -> Any:
    """
    Invoked on client side endpoints. All `args` and `kwargs` are converted to
    an RpcMessage and send to the server-side endpoints `receive` message.

    The server response is handled and returned to caller by this method.

    A timeout error may be thrown if the server does not response within the
    `callback_timeout`.
    """
    provider: RpcProvider = self.provider  # type: ignore

    args = list(args_)
    for i in self.arg_models.keys():
      if i >= len(args):
        break
      if args[i]:
        args[i] = args[i].dict()

    for key in self.keyword_models.keys():
      if kwargs.get(key):
        kwargs[key] = kwargs[key].dict()

    msg = RpcMessage(name=self.name, args=args, kwargs=kwargs)
    response: RpcMessage = await provider.send(msg)
    if self.return_type and issubclass(self.return_type, BaseModel):
      result = self.return_type.parse_obj(response.result)
      return result
    return response.result

  async def receive(self, msg: RpcMessage) -> RpcMessage:
    """
    Invokes on server side Endpoints when a message is received. Exception
    thrown from within this method are converted to RpcErrorMessages and return
    to the client.
    """
    args = list(msg.args)
    for i, cls in self.arg_models.items():
      if i >= len(args):
        break
      if args[i]:
        args[i] = cls.parse_obj(args[i])

    kwargs = msg.kwargs
    for key, cls in self.keyword_models.items():
      if kwargs.get(key):
        kwargs[key] = cls.parse_obj(kwargs[key])

    result = await self.func(*args, **kwargs)  # type: ignore
    if self.return_type and issubclass(self.return_type, BaseModel):
      result = result.dict()

    response = RpcMessage(id=msg.id, name=msg.name, result=result)
    return response

  @classmethod
  def has_endpoint(cls, obj) -> bool:
    if not obj:
      return False
    return getattr(obj, cls.attribute, False)

  def __repr__(self) -> str:
    return f'Endpoint({self.name})'


def has_endpoints(cls: Type[Any]) -> bool:
  for attr in dir(cls):
    func = getattr(cls, attr)
    if Endpoint.has_endpoint(func):
      return True
  return False


def get_endpoints(cls: Any, provider: RpcProvider = None) -> List[Endpoint]:
  """
  Return a list Endpoints for each method decorated by `rpc.endpoint` on the
  class `cls`.
  """

  endpoints = list()
  for attr in dir(cls):
    func = getattr(cls, attr)
    if not Endpoint.has_endpoint(func):
      continue
    end = Endpoint(func, provider)
    endpoints.append(end)
  return endpoints


def create_client(
        cls: Type[T],
        *args,
        callback_timeout: float = 10.0,
        queue_name: str = 'rpc_queue',
        id_: str = uuid1().hex,
        **kwargs) -> Union[RpcType, T]:
  """
  Create a facade instance of `cls` where methods decorated with the
  `rpc.endpoint` decorator will send RpcMessage's to remote endpoint.

  Only methods of `cls` decorated with `rpc.endpoint` will be present on the
  facade class returned by this method.
  """

  if not isinstance(cls, type):
    raise TypeError(f'unexpected type {cls}')

  endpoints: List[Endpoint] = get_endpoints(cls, None)
  if not endpoints:
    raise ValueError(f'no rpc endpoint on class {cls}')

  callback_queue_name = f'rpc_client.{id_}'

  rpc = RpcClient(
      id=f'client.{id_}',
      queue_name=queue_name,
      callback_queue_name=callback_queue_name,
      callback_timeout=callback_timeout,
      redis=None)

  for e in endpoints:
    e.set_provider(rpc)

  class_name: str = f'{cls.__name__}Client'
  attributes: Dict[str, Any] = {e.name: e.send for e in endpoints}
  attributes['rpc'] = rpc
  Client = type(class_name, (cls,), attributes)
  return Client(*args, **kwargs)


def create_server(
        cls: Type[T],
        *args,
        callback_timeout: float = 10.0,
        queue_name: str = 'rpc_queue',
        id_: str = uuid1().hex,
        **kwargs) -> Union[RpcType, T]:
  """
  Create a new instance of `cls` backed by an RpcServer. The instance's
  methods will be invoked automatically by the RpcServer. The server is
  accessible by the `rpc` attribute assigned to the instance of `cls`.

  Only methods of `cls` decorated with `rpc.endpoint` will be visible to
  RpcClients.
  """

  if not isinstance(cls, type):
    raise TypeError(f'unexpected type {cls}')

  if not has_endpoints(cls):
    raise ValueError(f'no rpc endpoint on class {cls}')

  instance = cls(*args, **kwargs)  # type: ignore
  endpoints: List[Endpoint] = get_endpoints(instance, None)

  methods = {e.name: e for e in endpoints}
  rpc = RpcServer(
      id=f'server.{id_}',
      queue_name=queue_name,
      callback_timeout=callback_timeout,
      endpoints=methods,
      redis=None)

  setattr(instance, 'rpc', rpc)
  return instance


def endpoint(func: Callable[[Any], Awaitable]):
  """
  Decorator for RPC endpoints - only methods using this decorator will be
  accessible by RpcClients.
  """

  info = Endpoint(func)
  setattr(func, info.attribute, True)
  return func


__all__ = ['create_server', 'create_client', 'endpoint', 'Endpoint', 'RpcProvider', 'RpcError',
           'RpcErrorMessage', 'RpcMessage']
