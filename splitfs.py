#!/usr/bin/env python

from __future__ import with_statement

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os

from fusepy.fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class SplitFS(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        return super(SplitFS, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode, file):
        file.fh = os.open(path, os.O_WRONLY | os.O_CREAT, mode)
        return 0

    def flush(self, path, file):
        return os.fsync(file.fh)

    def fsync(self, path, datasync, fh):
        return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    def open(self, path, file):
        file.fh = os.open(path, file.flags)
    #open = os.open

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod

    def read(self, path, size, offset, file):
        """Returns a string containing the data requested."""
        with self.rwlock:
            os.lseek(file.fh, offset, 0)
            return os.read(file.fh, size)

    def readdir(self, path, fh):
        """Can return either a list of names, or a list of (name, attrs, offset)
           tuples. attrs is a dict as in getattr."""
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, file):
        return os.close(file.fh)

    def rename(self, old, new):
        return os.rename(old, self.root + new)

    rmdir = os.rmdir

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, file):
        with self.rwlock:
            os.lseek(file.fh, offset, 0)
            return os.write(file.fh, data)


if __name__ == "__main__":
    if len(argv) != 3:
        print 'usage: %s <root> <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(SplitFS(argv[1]), argv[2], foreground=True, raw_fi = True)
