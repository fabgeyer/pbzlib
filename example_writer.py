#!/usr/bin/env python3

from pbzlib import write_pbz, open_pbz
from tests import messages_pb2

def main():
    objs = [messages_pb2.Header(version=1)]
    for i in range(10):
        objs.append(messages_pb2.Object(id=i))

    write_pbz("output.pbz", "tests/messages.descr", *objs)
    for msg in open_pbz("output.pbz"):
        print(msg)


if __name__ == "__main__":
    main()
