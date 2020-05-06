import gzip
import google.protobuf
from google.protobuf import descriptor_pool
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.internal.encoder import _VarintEncoder

from pbzlib.constants import MAGIC, T_PROTOBUF_VERSION, T_FILE_DESCRIPTOR, T_DESCRIPTOR_NAME, T_MESSAGE


class PBZWriter:
    def __init__(self, fname, fdescr, compresslevel=9, autoflush=False):
        self._ve = _VarintEncoder()
        self._dpool = descriptor_pool.DescriptorPool()
        self._last_descriptor = None
        self.autoflush = autoflush

        self._fobj = gzip.open(fname, "wb", compresslevel=compresslevel)
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

        # Write protocol buffer version in header
        self._write_blob(T_PROTOBUF_VERSION, len(google.protobuf.__version__), google.protobuf.__version__.encode("utf8"))

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
