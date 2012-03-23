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

import os

from fusepy.fuse import FUSE, FuseOSError, Operations, LoggingMixIn

DEFAULT_CHUNK_SIZE = 20   # 20B
DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024 # 1MB

class SplitFILE(object):
    manifest = None
    stat = None
    path = None
    mode = None
    chunks = []
    chunk_size = DEFAULT_CHUNK_SIZE
    size = 0

    def __init__(self, data = None):
        if data is not None and len(data):
            data = unserialize(data)
            for key, value in data.items():
                setattr(self, key, value)

    def __str__(self):
        return serialize({
                'stat': self.stat,
                'path': self.path,
                'mode': self.mode,
                'chunk_size': self.chunk_size,
                'size': self.size
                })

class SplitFUSE(FUSE):
    pass

class SplitFS(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()

    def __del__(self):
        pass

    def __call__(self, op, path, *args):
        return super(SplitFS, self).__call__(op, self.root + path, *args)

    #def access(self, path, mode):
    #    if not os.access(path, mode):
    #        raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        if os.path.basename(path) == 'restart':
            exit(0)
        sf = SplitFILE()
        sf.mode = mode
        sf.path = path
        self._saveManifest(path, sf)
        return 0

    #def flush(self, path, file):
    #    return os.fsync(file.fh)

    #def fsync(self, path, datasync, fh):
    #    return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        manifest = self._getManifest(path)
        if manifest and manifest.size is not None:
            ret['st_size'] = manifest.size
        print "AAAAAAAAAAAA"
        print manifest
        print ret
        print "BBBBBBBBBBBB"
        return ret

    def _saveManifest(self, path, sf):
        manifest = os.open(path, os.O_WRONLY | os.O_CREAT, 0777)
        os.write(manifest, str(sf) + '\n')
        os.close(manifest)

    def _getManifest(self, path):
        if not os.path.isfile(path):
            return None
        fh = open(path)
        data = fh.read()
        fh.close()
        return SplitFILE(data)

    def _relativeFileOffset(self, manifest, offset = 0, size = None, chunk_size = None):
        if chunk_size is None:
            chunk_size = manifest.chunk_size
        #manifest = self._getManifest(path)
        start_file, start_offset = int(offset / chunk_size), int(offset % chunk_size)
        if size is None:
            return start_file, start_offset
        end_file, end_offset = int((offset + size) / chunk_size), int((offset + size) % chunk_size)
        print "offset", offset, "size", size, "end_file", end_file, "end_offset", end_offset
        return start_file, start_offset, end_file, end_offset

    def _chunkPath(self, path, nth):
        return path + '.sf-%d' % nth

    def read(self, path, size, offset, fh = None):
        with self.rwlock:
            manifest = self._getManifest(path)
            chunk_size = manifest.chunk_size
            start_file, start_offset, end_file, end_offset = self._relativeFileOffset(manifest, offset = offset, size = size)
            print "READ: path=%s,size=%d,offset=%d,fh=%d, start=%s, end=%s, chunk_size: %d\n" % (path, size, offset, fh, (start_file, start_offset,), (end_file, end_offset,), chunk_size)
            buff = ""
            for nth in xrange(start_file, end_file + 1):
                chunk = open(self._chunkPath(path, nth))
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
        return ['.', '..'] + list(filter(lambda x: x.find('.sf-') == -1, os.listdir('target')))

    def write(self, path, data, offset, fh):
        with self.rwlock:
            manifest = self._getManifest(path)
            start_file, start_offset, end_file, end_offset = self._relativeFileOffset(manifest, offset = offset, size = len(data))
            wrote_size = 0
            for nth in xrange(start_file, end_file + 1):
                chunk = os.open(self._chunkPath(path, nth), os.O_WRONLY | os.O_CREAT, 0777)
                write_size = manifest.chunk_size
                if start_file == nth and start_offset:
                    chunk.seek(start_offset)
                    write_size -= start_offset
                if end_file == nth:
                    write_size -= (manifest.chunk_size - end_offset)
                wrote_size += os.write(chunk, data[wrote_size:(wrote_size + write_size)])
                os.close(chunk)
            manifest.size += wrote_size
            self._saveManifest(path, manifest)
            return manifest.size

    def truncate(self, path, length, fh=None):
        #with open(path, 'r+') as f:
        #    f.truncate(length)
        pass

    #getxattr = None

    #def link(self, target, source):
    #    return os.link(source, target)

    #def open(self, path, flags):
    #    file.fh = os.open(path, flags)

    #def release(self, path, file):
    #    pprint(file)
    #    return os.close(file.manifest)

    #def rename(self, old, new):
    #    return os.rename(old, self.root + new)

    #def statfs(self, path):
    #    stv = os.statvfs(path)
    #    return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))


    #listxattr = None
    #mkdir = os.mkdir
    #mknod = os.mknod
    #mknod = None
    #readlink = os.readlink
    #rmdir = os.rmdir
    #unlink = os.unlink
    #utimens = os.utime



if __name__ == "__main__":
    if len(argv) != 3:
        print 'usage: %s <root> <mountpoint>' % argv[0]
        exit(1)
    fuse = SplitFUSE(SplitFS(argv[1]), argv[2], foreground=True, raw_fi = False)
