#!/usr/bin/env python

DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024   # 10Mb

import os

class splitFile():
    def __init__(self):
        pass

def split(filename, chunk = DEFAULT_CHUNK_SIZE):
    stat = os.stat(filename)
    print stat
    size = stat.st_size
    print 1.0 * size / chunk

    file = splitFile()

def unsplit(filename):
    file = splitFile()
    raise notImplemented

if __name__ == "__main__":
    splittedFile = split('tests/space-loop-2.ogg', 128 * 1024)
    #unsplittedFile = unsplit('tests/space-loop-2.ogg.splitManifest')
