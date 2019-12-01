# Library for serializing protobuf objects - Python version

This library is used for simplifying the serialization and deserialization of [protocol buffer](https://developers.google.com/protocol-buffers/) objects to/from files.
The main use-case is to save and read a large collection of objects of the same type.
Each file contains a header with the description of the protocol buffer, meaning that no compilation of `.proto` description file is required before reading a `pbz` file.


## Installation

```
$ pip install --upgrade https://github.com/fabgeyer/pbzlib-py/archive/master.tar.gz
```

## Example

Reading a `pbz` file:

```
from pbzlib import open_pbz

for msg in open_pbz("/path/to/file.pbz"):
	print(msg)
```


## Versions in other languages

- [Go version](https://github.com/fabgeyer/pbzlib-go)
- [C/C++ version](https://github.com/fabgeyer/pbzlib-c-cpp)
