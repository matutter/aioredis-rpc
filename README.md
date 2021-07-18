# aioredis-rpc

[![ci](https://github.com/matutter/aioredis-rpc/workflows/CI/badge.svg?event=push)](https://github.com/matutter/aioredis-rpc/actions?query=event%3Apush+branch%3Amaster+workflow%3ACI)
[![coverage](https://codecov.io/gh/matutter/aioredis-rpc/branch/master/graph/badge.svg)](https://codecov.io/gh/matutter/aioredis-rpc)
[![pypi](https://img.shields.io/pypi/v/aioredis-rpc.svg)](https://pypi.python.org/pypi/aioredis-rpc)
[![downloads](https://img.shields.io/pypi/dm/aioredis-rpc.svg)](https://pypistats.org/packages/aioredis-rpc)
[![versions](https://img.shields.io/pypi/pyversions/aioredis-rpc.svg)](https://github.com/matutter/aioredis-rpc)
[![license](https://img.shields.io/github/license/matutter/aioredis-rpc.svg)](https://github.com/matutter/aioredis-rpc/blob/master/LICENSE)

A RPC interface using [aioredis](https://github.com/aio-libs/aioredis-py)
and [pydantic](https://github.com/samuelcolvin/pydantic).

## Usage

```bash
pip install aioredis-rpc
```

`pydantic` is used to model complex objects which are transparently serialized
and packed into messages.

```python
# Define Pydantic models
class FileData(BaseModel):
  filename: str
  data: bytes
```

Define a class using the `@endpoint` decorator to specify which methods will be
accessible over the rpc interface.

```python
from redisrpc import endpoint

# Define an RPC class
class Dropbox:
  files: Dict[str, FileData]
  max_files: int

  def __init__(self, max_files: int = 1000):
    self.files = dict()
    self.max_files = max_files

  @endpoint
  async def upload_file(self, file: FileData) -> int:
    if len(self.files) >= self.max_files:
      # Errors are propagated to the client-side
      raise Exception('too many files')
    self.files[file.name] = file
    return len(file.data)

  @endpoint
  async def download_file(self, name: str) -> FileData:
    return self.files[name]
```

Use `create_server` function to create an instance of your server-side rpc
class. The server instance will be assigned an `rpc: RpcProvider` attribute to
access server functions like `connect` and `disconnect`. Once `connect` is
called methods decorated with `@endpoint` will be invoked automatically by
client RPC messages.

Also note that `connect` is non-blocking.

```python
server = create_server(Dropbox, max_files=2)
# Returns once connected to redis
await server.rpc.connect(dsn="redis://localhost")
```

The `create_client` function create a faux instance of the rpc class with only
the methods decorated by `@endpoint` present. When these methods are called by
the client the function arguments are serialized and published to redis.

```python
client = create_client(Dropbox)
await client.rpc.connect(dsn="redis://localhost")
```

Now that both ends are connected the the `@endpoint` decorated methods may be
called like they are accessing the actual class passed to `create_client`.

```python
file1 = FileData(name='file1', data=b'1234')
size = await client.upload_file(file1)
```

## Development

The following items will be handled in future revisions.

- Support generic return-types so `list` and other container types may be
  returned from endpoints.
