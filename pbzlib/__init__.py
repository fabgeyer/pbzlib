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
from google.protobuf.descriptor_pb2 import FileDescriptorSet, FileDescriptorProto

MAGIC = b'\x41\x42'
T_FILE_DESCRIPTOR = 1
T_DESCRIPTOR_NAME = 2
T_MESSAGE = 3


class PBZWriter:
    def __init__(self, fname, fdescr, autoflush=False):
        self._ve = _VarintEncoder()
        self._dpool = descriptor_pool.DescriptorPool()
        self._last_descriptor = None
        self.autoflush = autoflush

        self._fobj = gzip.open(fname, "wb")
        self._write_header(fdescr)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def _write_header(self, fdescr):
        # Read FileDescriptorSet
        with open(fdescr, "rb") as fi:
            fdset = fi.read()
            sz = fi.tell()

        # Parse descriptor for checking that the messages will be defined in
        # the serialized file
        ds = FileDescriptorSet()
        ds.ParseFromString(fdset)
        for df in ds.file:
            self._dpool.Add(df)

        self._fobj.write(MAGIC)

        # Write FileDescriptorSet
        self._write_blob(T_FILE_DESCRIPTOR, sz, fdset)

    def _write_blob(self, mtype, size, value):
        self._fobj.write(bytes([mtype]))
        self._ve(self._fobj.write, size)
        self._fobj.write(value)
        if self.autoflush:
            self._fobj.flush()

    def close(self):
        """
        Close PBZ file
        """
        self._fobj.close()

    def write(self, msg):
        """
        Writes a protobuf message to the file
        """
        if msg.DESCRIPTOR.full_name != self._last_descriptor:
            # Check that the message type has been defined
            self._dpool.FindMessageTypeByName(msg.DESCRIPTOR.full_name)

            self._write_blob(T_DESCRIPTOR_NAME, len(msg.DESCRIPTOR.full_name), msg.DESCRIPTOR.full_name.encode("utf8"))
            self._last_descriptor = msg.DESCRIPTOR.full_name

        self._write_blob(T_MESSAGE, msg.ByteSize(), msg.SerializeToString())


def write_pbz(fname, fdescr, *msgs):
    w = PBZWriter(fname, fdescr)
    if len(msgs) == 0:
        # Returns writer to caller
        return w
    else:
        # Directly write the messages to file and close
        for msg in msgs:
            w.write(msg)
        w.close()


def open_pbz(fname):
    dpool = descriptor_pool.DescriptorPool()
    descr = None

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
                for df in ds.file:
                    dpool.Add(df)

            elif vtype == T_DESCRIPTOR_NAME:
                descr = dpool.FindMessageTypeByName(data.decode("utf8"))

            elif vtype == T_MESSAGE:
                msg = reflection.ParseMessage(descr, data)
                yield msg

            else:
                raise Exception(f"Unknown message type {vtype}")
