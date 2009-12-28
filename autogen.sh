#!/bin/bash
# $Id$

#
# Script to regenerate all the GNU auto* gunk.
# Run this from the top directory of the source tree.
#
# If it looks like I don't know what I'm doing here, you're right.
#

set -e

. ./cleanup.sh
mkdir -p scripts

ACLOCAL="aclocal"
AUTOHEADER="autoheader"
AUTOMAKE="automake"
AUTOCONF="autoconf"

echo "running aclocal"
${ACLOCAL} ${ACLOCAL_ARGS} -I scripts

echo "running autoheader"
${AUTOHEADER}

echo "running automake"
${AUTOMAKE} --add-missing -c --foreign

echo "running autoconf"
${AUTOCONF} -f -i

