#!usr/bin/env python3

import os.path as op
import re

from setuptools import find_packages, setup
from setuptools.command.build_ext import build_ext


def read(file, pattern=None):
  this_dir = op.abspath(op.dirname(__file__))
  with open(op.join(this_dir, file), encoding='utf-8') as fd:
    text = fd.read().strip()
    if pattern:
      text = re.findall(pattern, text)[0]
    return text


# Extract the __version__ value from __init__.py
version = read('redisrpc/__init__.py', r'__version__ = "([^"]+)"')

# Use the entire README
long_description = read('README.md')

# Dependencies from requirements
install_requires = """
pydantic==1.8.2
msgpack==1.0.2
aioredis==1.3.1
hiredis==2.0.0
"""

setup(
    name="redisrpc",
    version=version,
    description="An RPC library based on aioredis, msgpack, and pydantic.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Mathew Utter",
    author_email="mcutter.svc@gmail.com",
    license="MIT",
    url="http://github.com/matutter/redisrpc",
    keywords=' '.join([
        'aioredis',
        'msgpack',
        'python',
        'redis',
        'redisrpc',
        'rpc'
    ]),
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: BSD',
        'Operating System :: Microsoft :: Windows :: Windows Vista',
        'Operating System :: Microsoft :: Windows :: Windows 7',
        'Operating System :: Microsoft :: Windows :: Windows 8',
        'Operating System :: Microsoft :: Windows :: Windows 8.1',
        'Operating System :: Microsoft :: Windows :: Windows 10',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Software Development :: Libraries',
        'Topic :: System :: Filesystems',
        'Topic :: System :: Monitoring',
        'Topic :: Utilities',
    ],
    packages=find_packages(include=['redisrpc']),
    install_requires=install_requires,
    requires=re.findall(r'^\w+', install_requires, re.MULTILINE),
    cmdclass={
        'build_ext': build_ext,
    },
    python_requires='>=3.6.1',
    # Due to README.md, requirements.txt, etc...
    zip_safe=True
)
