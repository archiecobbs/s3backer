
#
# s3backer - FUSE-based single file backing store via Amazon S3
#
# Copyright 2008-2020 Archie L. Cobbs <archie.cobbs@gmail.com>
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
# In addition, as a special exception, the copyright holders give
# permission to link the code of portions of this program with the
# OpenSSL library under certain conditions as described in each
# individual source file, and distribute linked combinations including
# the two.
#
# You must obey the GNU General Public License in all respects for all
# of the code used other than OpenSSL. If you modify file(s) with this
# exception, you may extend this exception to your version of the
# file(s), but you are not obligated to do so. If you do not wish to do
# so, delete this exception statement from your version. If you delete
# this exception statement from all source files in the program, then
# also delete it here.

# Setup build for executables: s3backer and tester
bin_PROGRAMS=		s3backer

noinst_PROGRAMS=	tester

noinst_HEADERS=		s3backer.h \
			block_cache.h \
			block_part.h \
			dcache.h \
			ec_protect.h \
			zero_cache.h \
			erase.h \
			fuse_ops.h \
			hash.h \
			util.h \
			compress.h \
			http_io.h \
			reset.h \
			test_io.h \
			s3b_config.h

# Setup build for NBD plugin shared library

nbdplugindir=		$(NBDKIT_PLUGINDIR)

nbdplugin_LTLIBRARIES=	nbdkit-s3backer-plugin.la

nbdkit_s3backer_plugin_la_SOURCES = \
			nbdkit.c \
			block_cache.c \
			block_part.c \
			dcache.c \
			ec_protect.c \
			zero_cache.c \
			erase.c \
			fuse_ops.c \
			hash.c \
			util.c \
			compress.c \
			http_io.c \
			reset.c \
			s3b_config.c \
			test_io.c \
			sslcompat.c \
			gitrev.c

nbdkit_s3backer_plugin_la_LDFLAGS = \
			-module -avoid-version -shared

# See https://www.gnu.org/software/automake/manual/html_node/Objects-created-both-with-libtool-and-without.html
s3backer_CFLAGS=	$(AM_CFLAGS)
tester_CFLAGS=		$(AM_CFLAGS)

# libtool random
ACLOCAL_AMFLAGS=	-I m4

man_MANS=		s3backer.1

docdir=			$(datadir)/doc/packages/$(PACKAGE)

doc_DATA=		CHANGES COPYING README INSTALL TODO

EXTRA_DIST=		CHANGES s3backer.1 s3backer.spec

s3backer_SOURCES=	main.c \
			block_cache.c \
			block_part.c \
			dcache.c \
			ec_protect.c \
			zero_cache.c \
			erase.c \
			fuse_ops.c \
			hash.c \
			util.c \
			compress.c \
			http_io.c \
			reset.c \
			s3b_config.c \
			test_io.c \
			sslcompat.c \
			gitrev.c

tester_SOURCES=		tester.c \
			block_cache.c \
			block_part.c \
			dcache.c \
			ec_protect.c \
			zero_cache.c \
			erase.c \
			hash.c \
			util.c \
			compress.c \
			http_io.c \
			reset.c \
			s3b_config.c \
			test_io.c \
			sslcompat.c \
			gitrev.c

AM_CFLAGS=		$(FUSE_CFLAGS) $(NBDKIT_CFLAGS)

gitrev.c:
			printf 'const char *const s3backer_version = "%s";\n' "`git describe`" > gitrev.c
