
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

bin_PROGRAMS=	    s3backer

noinst_PROGRAMS=	tester

$(bin_PROGRAMS):    svnrev.h

$(noinst_PROGRAMS):	svnrev.h

noinst_HEADERS=     s3backer.h svnrev.h

man_MANS=           s3backer.1

EXTRA_DIST=         s3backer.1

s3backer_SOURCES=	main.c fuse_ops.c s3backer.c config.c

tester_SOURCES=	    tester.c s3backer.c config.c

AM_CFLAGS=	        $(CFLAGS) $(FUSE_CFLAGS) $(GLIB2_CFLAGS)

AM_LDFLAGS=	        $(FUSE_LIBS) $(GLIB2_LIBS)

svnrev.h:
					printf '#define SVNREVISION "%u"\n' `svn info | grep -w Rev | sed -r 's/^.*Rev: //g'` > svnrev.h

