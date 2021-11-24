#!/usr/bin/env python3

import gzip
import warnings
import google.protobuf
from typing import Any, Optional
from google.protobuf import reflection
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.internal.decoder import _DecodeVarint
from google.protobuf.descriptor_pb2 import FileDescriptorSet

from pbzlib.constants import MAGIC, T_PROTOBUF_VERSION, T_FILE_DESCRIPTOR, T_DESCRIPTOR_NAME, T_MESSAGE


class PBZReader:
    def __init__(self, fname: str, module=None, return_raw_object: bool = False):
        self.return_raw_object = return_raw_object

        self._next_descr = None
        self._next_descr_name = None

        self._prevbuf = b""
        self._fobj = gzip.open(fname, "rb")
        assert self._fobj.read(len(MAGIC)) == MAGIC

        dpool, self._raw_descriptor = self.read_descriptor_pool()
        if module is None:
            # Use the DescriptorPool from the pbz file
            self._dpool = dpool
        elif not hasattr(module, "DESCRIPTOR"):
            raise Exception("The passed 'module' argument is not a compiled protobuf module!")
        else:
            # Use the DescriptorPool provided by the passed module
            self._dpool = module.DESCRIPTOR.pool

    def _read_next_obj(self) -> (Optional[int], Optional[bytes]):
        """
        Each object is encoded as:
         - vtype (uint8_t): type of the object
         - size (varint): size of the object as varint
         - object itself
        """
        bufsz = self._prevbuf + self._fobj.read(9 - len(self._prevbuf))
        if len(bufsz) == 0:
            return None, None

        vtype = bufsz[0]
        size, pos = _DecodeVarint(bufsz[1:], 0)

        if pos + size < 8:
            data = bufsz[1 + pos:1 + pos + size]
            self._prevbuf = bufsz[1 + pos + size:]
        elif pos + size < 8:
            data = bufsz[1:]
            self._prevbuf = b""
        else:
            data = bufsz[1 + pos:] + self._fobj.read(size - 8 + pos)
            self._prevbuf = b""

        return vtype, data

    def read_descriptor_pool(self) -> (DescriptorPool, Optional[bytes]):
        dpool = DescriptorPool()
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

        return dpool, None

    def next(self, default: Any = None):
        while True:
            vtype, data = self._read_next_obj()
            if vtype is None:
                if default is None:
                    raise StopIteration
                return default

            elif vtype == T_DESCRIPTOR_NAME:
                self._next_descr_name = data.decode("utf8")
                self._next_descr = self._dpool.FindMessageTypeByName(self._next_descr_name)

            elif vtype == T_MESSAGE:
                if self.return_raw_object:
                    return self._next_descr_name, self._next_descr, data
                else:
                    return reflection.ParseMessage(self._next_descr, data)

            else:
                raise Exception(f"Unknown message type {vtype}")
