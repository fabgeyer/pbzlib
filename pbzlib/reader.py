#!/usr/bin/env python3

import gzip
import warnings
import google.protobuf
from google.protobuf import descriptor_pool
from google.protobuf import reflection
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.descriptor_pb2 import FileDescriptorSet

from pbzlib.constants import MAGIC, T_PROTOBUF_VERSION, T_FILE_DESCRIPTOR, T_DESCRIPTOR_NAME, T_MESSAGE


class PBZReader:
    def __init__(self, fname, return_raw_object=False):
        self.return_raw_object = return_raw_object

        self._next_descr = None
        self._next_descr_name = None

        self._fobj = gzip.open(fname, "rb")
        assert self._fobj.read(len(MAGIC)) == MAGIC
        self._dpool, self._raw_descriptor = self.read_descriptor_pool()

    def _read_next_obj(self):
        try:
            buf = self._fobj.read(5)
        except:
            return None, None

        if len(buf) < 2:
            return None, None

        vtype = buf[0]
        buf = buf[1:]
        size, pos = _DecodeVarint32(buf, 0)
        rsize = size - (4 - pos)
        if rsize < 0:
            data = buf[pos:pos + size]
            self._fobj.seek(rsize, 1)
        elif rsize == 0:
            data = buf[pos:]
        else:
            try:
                data = buf[pos:] + self._fobj.read(rsize)
            except:
                return None, None

        return vtype, data

    def read_descriptor_pool(self):
        dpool = descriptor_pool.DescriptorPool()
        while True:
            vtype, data = self._read_next_obj()
            if vtype is None:
                raise Exception("Unexpected end of file")

            if vtype == T_FILE_DESCRIPTOR:
                ds = FileDescriptorSet()
                ds.ParseFromString(data)
                for df in ds.file:
                    dpool.Add(df)
                return dpool, data

            elif vtype == T_PROTOBUF_VERSION:
                pbversion = data.decode("utf8")
                if google.protobuf.__version__.split(".") < pbversion.split("."):
                    warnings.warn(f"File uses more recent of protobuf ({pbversion})")

            else:
                raise Exception(f"Unknown message type {vtype}")

    def next(self):
        while True:
            vtype, data = self._read_next_obj()
            if vtype is None:
                raise StopIteration

            if vtype == T_DESCRIPTOR_NAME:
                self._next_descr_name = data.decode("utf8")
                self._next_descr = self._dpool.FindMessageTypeByName(self._next_descr_name)

            elif vtype == T_MESSAGE:
                if self.return_raw_object:
                    return self._next_descr_name, self._next_descr, data
                else:
                    return reflection.ParseMessage(self._next_descr, data)

            else:
                raise Exception(f"Unknown message type {vtype}")
