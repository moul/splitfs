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
#DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024 = # 1MB

class SplitFILE(object):
    manifest = None
    stat = None
    path = None
    mode = None
    chunks = []
    chunk_size = DEFAULT_CHUNK_SIZE
    size = None

    def __init__(self, data = None):
        if len(data):
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
"""    def open(self, path, fip):
        print "OPEN"
        print dir(fip)
        pprint(fip)
        fi = fip.contents
        if self.raw_fi:
            return self.operations('open', path, fi)
        else:
            fi.fh = self.operations('open', path, fi.flags)
            return 0

    def create(self, path, mode, fip):
        print "CREATE"
        print dir(fip)
        pprint(fip)
        fi = fip.contents
        if self.raw_fi:
            return self.operations('create', path, mode, fi)
        else:
            fi.fh = self.operations('create', path, mode)
            return 0

    def release(self, path, fip):
        print "RELEASE"
        print dir(fip)
        pprint(fip)
        print dir(fip.contents)
        print dir(fip._type_)
        print dir(fip._objects)
        print dir(fip.contents.fh)
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('release', path, fh)
"""

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
        manifest = os.open(path, os.O_WRONLY | os.O_CREAT, 0777)
        os.write(manifest, str(sf) + '\n')
        os.close(manifest)
        return 0

    #def flush(self, path, file):
    #    return os.fsync(file.fh)

    #def fsync(self, path, datasync, fh):
    #    return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        manifest = self._getManifest(path)
        if manifest:
            ret['st_size'] = manifest.size
        print "AAAAAAAAAAAA"
        print manifest
        print ret
        print "BBBBBBBBBBBB"
        return ret

    def _getManifest(self, path):
        if not os.path.isfile(path):
            return None
        fh = open(path)
        data = fh.read()
        fh.close()
        return SplitFILE(data)

    def _relativeFileOffset(self, path, offset = 0, size = None):
        manifest = self._getManifest(path)
        start = (int(offset / manifest.chunk_size), int(offset % manifest.chunk_size))
        if size is None:
            return start, manifest.chunk_size
        end = (int((offset + size) / manifest.chunk_size), int((offset + size) % manifest.chunk_size))
        return start, end, manifest.chunk_size

    def _chunkPath(self, path, nth):
        return path + '.sf-%d' % nth

    def read(self, path, size, offset, fh = None):
        """Returns a string containing the data requested."""
        with self.rwlock:
            start, end, chunk_size = self._relativeFileOffset(path, offset = offset, size = size)
            ret = "READ: path=%s,size=%d,offset=%d,fh=%d, start=%s, end=%s, chunk_size: %d\n" % (path, size, offset, fh, start, end, chunk_size)
            buff = ""
            for nth in xrange(start[0], end[0] + 1):
                chunk = open(self._chunkPath(path, nth))
                read_size = chunk_size
                if start[0] == nth and start[1]:
                    chunk.seek(start[1])
                    read_size -= start[1]
                if end[0] == nth:
                    read_size -= (chunk_size - end[1])
                buff += chunk.read(read_size)
                chunk.close()
            return buff

    def readdir(self, path, fh):
        """Can return either a list of names, or a list of (name, attrs, offset)
           tuples. attrs is a dict as in getattr."""
        return ['.', '..'] + list(filter(lambda x: x.find('.sf-') == -1, os.listdir('target')))

    def write(self, path, data, offset, fh):
        print "!!!"
        print data
        print "!!!"
        manifest = self._getManifest(path)
        print manifest
        return len(data)
        #with self.rwlock:
        #    os.lseek(fh, offset, 0)
        #    return os.write(file.fh, data)

    #def symlink(self, target, source):
    #    raise MANFRED
    #    return os.symlink(source, target)

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
