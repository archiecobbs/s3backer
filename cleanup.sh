#!/bin/sh
# $Id$

#
# Script to clean out generated GNU auto* gunk.
#

set -e

echo "cleaning up"
rm -rf autom4te*.cache scripts aclocal.m4 configure config.log config.status .deps stamp-h1
rm -f config.h.in config.h
rm -f scripts
rm -f Makefile Makefile.in
rm -f svnrev.c

