#!/bin/bash

# Bail on error
set -e

# Reset
. ./cleanup.sh

# Create directory to avoid warning
mkdir -p m4

# Apply autofoo magic
autoreconf -iv

# Configure for debug
./configure --enable-Werror --enable-assertions

# Build
make -j
