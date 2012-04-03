#!/bin/sh

BIN="$(pwd)/splitfs.py"
MOUNTPOINT="$(pwd)/mountpoint/"
OPTS="target=$(pwd)/target/"
#EXTRA_ARGS=$EXTRA_ARGS -d
OPTS=$OPTS,"nonempty"
EXTRA_ARGS="$EXTRA_ARGS -f"

BIN="$(pwd)/tmp/cachefs.py"
OPTS=$OPTS,"cache=$(pwd)/cache/"



clear
#kill $(ps auxwww | grep -i split | grep python | awk '{print $2}') 2>/dev/null
umount -f $MOUNTPOINT
echo $BIN $MOUNTPOINT -o $OPTS $EXTRA_ARGS
$BIN $MOUNTPOINT -o $OPTS $EXTRA_ARGS
sleep .2
mount | grep $MOUNTPOINT | grep -i fuse
echo
ls -l $MOUNTPOINT

