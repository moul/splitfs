#!/bin/sh

clear
umount -f $(pwd)/mountpoint
./splitfs.py ./mountpoint  -o target=target
mount | grep mountpoint | grep -i fuse
