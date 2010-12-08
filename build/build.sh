#!/bin/sh

SRCDIR=../src/

# Check for version number
if [ "$1" = "" ]; then
	echo "Usage: build.sh version_number";
	exit 1
else
	VERSION="$1"
fi
BUILDDIR=eipc-$VERSION

# Create the $BUILDDIR dir
if [ -d $BUILDDIR ]; then 
	rm -rf $BUILDDIR;
fi
mkdir -p $BUILDDIR/

# Copy python files into the $BUILDDIR dir.
cp $SRCDIR/*.py $BUILDDIR/
cp setup.py $BUILDDIR/
sed -i "" s/VERSION/$VERSION/ $BUILDDIR/setup.py

tar cfz $BUILDDIR.tar.gz $BUILDDIR

exit 0
