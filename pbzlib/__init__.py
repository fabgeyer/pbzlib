#!/usr/bin/env python3
"""
Small library for writing a collection of protobuf objects in a binary file

File format:
The file starts with a magic number
Then a tuple of type, length and value are written to file
- type encoded as int8
- size encoded as varint32

After the magic number, the file descriptor of the protobufs is written.
The messages passed to the function are then written to file, with first
their descriptor full name and then their value.
"""

from pbzlib.reader import PBZReader
from pbzlib.writer import PBZWriter


def write_pbz(fname: str, fdescr: str, *msgs):
    w = PBZWriter(fname, fdescr)
    if len(msgs) == 0:
        # Returns writer to caller
        return w
    else:
        # Directly write the messages to file and close
        for msg in msgs:
            w.write(msg)
        w.close()


def open_pbz(fname: str, module=None):
    sentinel = object()
    r = PBZReader(fname, module=module)
    while True:
        obj = r.next(sentinel)
        if obj is sentinel:
            return
        yield obj
