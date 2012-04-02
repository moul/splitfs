#!/bin/sh

VERSION=$(python -c 'from splitfs import __version__; print __version__')

git archive --format=tar --prefix="splitfs-$VERSION/" HEAD | gzip > splitfs-$VERSION.tar.gz
