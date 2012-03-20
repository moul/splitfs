#!/usr/bin/env python

import shutil
import time
import os
import stat
import errno
import sys
#import sqlite3

SPLITFS_VERSION = '0.0.1'
import fuse
fuse.fuse_python_api = (0, 2)

log_file = sys.stdout

def debug(text):
    log_file.write(text)
    log_file.write('\n')
    log_file.flush()

class SplitFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        #fuse.Fuse.__init__(self, *args, **kwargs)
        super(SplitFS, self).__init__(*args, **kwargs)
        #self.file_class = 

def main():
    usage = '%prog MOUNTPOINT -o target=SOURCE [options]'
    sfs = SplitFS(version = 'SplitFS %s' % SPLITFS_VERSION,
                  usage = usage,
                  dash_s_do = 'setsingle')

    sfs.parser.add_option(
        mountopt = "target", metavar="PATH",
        default = None,
        help = "Path to be splitted / unsplitted")

    sfs.parse(values = sfs, errex = 1)
    sfs.target = os.path.abspath(sfs.target)
    sfs.multithreaded = 0
    #sfs.split_db = create_db(split_dir)

    print 'Setting up SplitFS %s ...' % SPLITFS_VERSION
    print '  Target       : %s' % sfs.target
    print '  Mount Point  : %s' % os.path.abspath(sfs.fuse_args.mountpoint)
    print
    print 'Unmount through:'
    print '  fusermount -u %s' % sfs.fuse_args.mountpoint
    print
    print 'Done.'
    sfs.main()
    return 0


if __name__ == '__main__':
    main()
