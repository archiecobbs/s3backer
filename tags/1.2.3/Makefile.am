
#
# s3backer - FUSE-based single file backing store via Amazon S3
# 
# Copyright 2008 Archie L. Cobbs <archie@dellroad.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# $Id$
#

bin_PROGRAMS=       s3backer

noinst_PROGRAMS=    tester

noinst_HEADERS=     s3backer.h \
			block_cache.h \
			block_part.h \
			ec_protect.h \
			erase.h \
			fuse_ops.h \
			hash.h \
			http_io.h \
			test_io.h \
			s3b_config.h

man_MANS=           s3backer.1

docdir=             $(datadir)/doc/packages/$(PACKAGE)

doc_DATA=           CHANGES COPYING README INSTALL TODO

EXTRA_DIST=         CHANGES s3backer.1 s3backer.spec

s3backer_SOURCES=   main.c \
		    block_cache.c \
		    block_part.c \
		    ec_protect.c \
		    erase.c \
		    fuse_ops.c \
		    hash.c \
		    http_io.c \
		    s3b_config.c \
		    test_io.c \
		    svnrev.c

tester_SOURCES=     tester.c \
		    block_cache.c \
		    block_part.c \
		    ec_protect.c \
		    erase.c \
		    hash.c \
		    http_io.c \
		    s3b_config.c \
		    test_io.c \
		    svnrev.c

AM_CFLAGS=          $(CFLAGS) $(FUSE_CFLAGS)

AM_LDFLAGS=         $(FUSE_LIBS)

SUBDIRS=            debian

svnrev.c:
			printf 'const int s3backer_svnrev = %u;\n' `svn info | grep -w Rev | sed -r 's/^.*Rev: //g'` > svnrev.c

