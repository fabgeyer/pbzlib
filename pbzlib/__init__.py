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

import gzip
from google.protobuf import descriptor
from google.protobuf import descriptor_pool
from google.protobuf import reflection
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _VarintEncoder
from google.protobuf.descriptor_pb2 import FileDescriptorSet


MAGIC = b'\x41\x42'
T_FILE_DESCRIPTOR = 1
T_DESCRIPTOR_NAME = 2
T_MESSAGE = 3


def write_pbz(fname, fdescr, *msgs):
    ve = _VarintEncoder()

    with gzip.open(fname, "wb") as fo:
        fo.write(MAGIC)

        # Read FileDescriptorSet
        with open(fdescr, "rb") as fi:
            fdset = fi.read()
            sz = fi.tell()

        # Write it as header
        fo.write(bytes([T_FILE_DESCRIPTOR]))
        ve(fo.write, sz)
        fo.write(fdset)

        last_descriptor = None
        for msg in msgs:
            if msg.DESCRIPTOR.full_name != last_descriptor:
                fo.write(bytes([T_DESCRIPTOR_NAME]))
                ve(fo.write, len(msg.DESCRIPTOR.full_name))
                fo.write(msg.DESCRIPTOR.full_name.encode("utf8"))
                last_descriptor = msg.DESCRIPTOR.full_name

            fo.write(bytes([T_MESSAGE]))
            ve(fo.write, msg.ByteSize())
            fo.write(msg.SerializeToString())


def open_pbz(fname):
    dpool = None
    descriptor = None

    with gzip.open(fname, "rb") as f:
        assert f.read(len(MAGIC)) == MAGIC
        while True:
            try:
                buf = f.read(5)
            except:
                break
            if len(buf) < 2:
                break

            vtype = buf[0]
            buf = buf[1:]
            size, pos = _DecodeVarint32(buf, 0)
            rsize = size - (4 - pos)
            if rsize < 0:
                data = buf[pos:pos + size]
                f.seek(rsize, 1)
            elif rsize == 0:
                data = buf[pos:]
            else:
                try:
                    data = buf[pos:] + f.read(rsize)
                except:
                    break

            if vtype == T_FILE_DESCRIPTOR:
                ds = FileDescriptorSet()
                ds.ParseFromString(data)
                dpool = descriptor_pool.DescriptorPool()
                for df in ds.file:
                    dpool.Add(df)

            elif vtype == T_DESCRIPTOR_NAME:
                descriptor = dpool.FindMessageTypeByName(data.decode("utf8"))

            elif vtype == T_MESSAGE:
                msg = reflection.ParseMessage(descriptor, data)
                yield msg

            else:
                raise Exception(f"Unknown message type {vtype}")

