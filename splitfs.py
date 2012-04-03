#!/usr/bin/env python

from __future__ import with_statement

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock
from pprint import pprint
from json import dumps as serialize
from json import loads as unserialize
from sys import exit
from time import sleep
from textwrap import wrap

import os

from fusepy.fuse import FUSE, FuseOSError, Operations

DEFAULT_CHUNK_SIZE = '20M'
FULL_DEBUG = True
__version__ = '0.2.0'

class LoggingMixIn:
    def __call__(self, op, path, *args):
        if op in ['access', 'getattr', 'statfs', 'getxattr']:
            return getattr(self, op)(path, *args)
        if FULL_DEBUG:
            print '->', op, path, wrap(repr(args), 100)[0]
        else:
            print '->', op, path
        ret = '[Unhandled Exception]'
        try:
            ret = getattr(self, op)(path, *args)
            return ret
        except OSError, e:
            ret = str(e)
            raise
        finally:
            if FULL_DEBUG:
                print '<-', op, wrap(repr(ret), 100)[0]
            else:
                print '<-', op

def parseSize(text):
    prefixes = {
        'b': 1,
        'k': 1024, 'ki': 1024, 'kb': 1024,
        'm': 1024 * 1024, 'mb': 1024 * 1024,
        'g': 1024 * 1024 * 1024, 'gb': 1024 * 1024 * 1024
        }
    num = ""
    text = str(text).strip()
    while text and text[0:1].isdigit() or text[0:1] == '.':
        num += text[0]
        text = text[1:]
    num = float(num)
    letter = text.strip().lower()
    return int(num * (prefixes[letter] if letter in prefixes else 1))

def saveManifest(path, sf):
    fh = os.open(path, os.O_WRONLY | os.O_CREAT, 0777)
    os.write(fh, serialize(sf) + '\n')
    os.close(fh)

def getManifest(path):
    if not os.path.isfile(path):
        return None
    fh = open(path)
    data = fh.read()
    fh.close()
    return unserialize(data)

def relativeFileOffset(manifest, offset = 0, size = None, chunk_size = None):
    if chunk_size is None:
        chunk_size = manifest.get('chunk_size')
    start_file, start_offset = int(offset / chunk_size), int(offset % chunk_size)
    if size is None:
        return start_file, start_offset
    end_file, end_offset = int((offset + size) / chunk_size), int((offset + size) % chunk_size)
    #print "offset", offset, "size", size, "end_file", end_file, "end_offset", end_offset
    return start_file, start_offset, end_file, end_offset

def chunkPath(path, nth):
    return path + '.sf-%d' % nth

class SplitFS(LoggingMixIn, Operations):
    def __init__(self, root, sleep = None, chunk_size = parseSize(DEFAULT_CHUNK_SIZE)):
        self.root = realpath(root)
        self.rwlock = Lock()
        self.sleep = sleep
        self.manifests = {}
        self.chunk_size = chunk_size

    def __del__(self):
        pass

    def __call__(self, op, path, *args):
        if self.sleep:
            sleep(self.sleep)
        return super(SplitFS, self).__call__(op, self.root + path, *args)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        saveManifest(path, {'mode': mode, 'path': path, 'chunk_size': self.chunk_size})
        return 0

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        manifest = getManifest(path)
        #print manifest
        if manifest and manifest.get('size', None) is not None:
            ret['st_size'] = manifest.get('size')
        return ret

    def read(self, path, size, offset, fh = None):
        with self.rwlock:
            manifest = getManifest(path)
            chunk_size = manifest.get('chunk_size')
            start_file, start_offset, end_file, end_offset = relativeFileOffset(manifest, offset = offset, size = size)
            print "READ: path=%s,size=%d,offset=%d,fh=%d, start=%s, end=%s, chunk_size: %d\n" % (path, size, offset, fh, (start_file, start_offset,), (end_file, end_offset,), chunk_size)
            buff = ""
            for nth in xrange(start_file, end_file + 1):
                chunk = open(chunkPath(path, nth))
                read_size = chunk_size
                if start_file == nth and start_offset:
                    chunk.seek(start_offset)
                    read_size -= start_offset
                if end_file == nth:
                    read_size -= (chunk_size - end_offset)
                buff += chunk.read(read_size)
                chunk.close()
            return buff

    def readdir(self, path, fh):
        return ['.', '..'] + list(filter(lambda x: x.find('.sf-') == -1, os.listdir(path)))

    def write(self, path, data, offset, fh):
        with self.rwlock:
            manifest = getManifest(path)
            chunk_size = manifest.get('chunk_size')
            start_file, start_offset, end_file, end_offset = relativeFileOffset(manifest, offset = offset, size = len(data))
            wrote_size = 0
            for nth in xrange(start_file, end_file + 1):
                chunk = os.open(chunkPath(path, nth), os.O_WRONLY | os.O_CREAT, 0777)
                write_size = chunk_size
                if start_file == nth and start_offset:
                    os.lseek(chunk, start_offset, os.SEEK_SET)
                    write_size -= start_offset
                if end_file == nth:
                    write_size -= (chunk_size - end_offset)
                #print "____________"
                ##print data
                #print chunk
                #print wrote_size
                #print write_size
                #print wrote_size + write_size
                #print "____________"
                wrote_size += os.write(chunk, data[wrote_size:(wrote_size + write_size)])
                os.close(chunk)
                manifest['size'] = max(manifest.get('size'), nth * chunk_size + end_offset)
            saveManifest(path, manifest)
            return wrote_size

    def truncate(self, path, length, fh=None):
        pass

if __name__ == "__main__":
    if len(argv) != 3:
        print 'usage: %s <root> <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(SplitFS(argv[1], sleep = 0, chunk_size = parseSize(DEFAULT_CHUNK_SIZE)), argv[2], foreground=True, raw_fi = False)
