#!/usr/bin/env bash

#
# Script to regenerate all the GNU auto* gunk.
# Run this from the top directory of the source tree.
#
# If it looks like I don't know what I'm doing here, you're right.
#

set -e

. ./cleanup.sh
mkdir -p scripts m4
exec autoreconf -iv
