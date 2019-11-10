#!/usr/bin/env python3

import argparse
from pbzlib import open_pbz

def main(path):
    for msg in open_pbz(path):
        print(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str)
    args = parser.parse_args()
    main(args.input)
