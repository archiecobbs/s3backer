#
# s3backer - FUSE-based single file backing store via Amazon S3
# 
# Copyright 2008-2011 Archie L. Cobbs <archie@dellroad.org>
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

AC_INIT([s3backer FUSE filesystem backed by Amazon S3], [1.4.2], [https://github.com/archiecobbs/s3backer], [s3backer])
AC_CONFIG_AUX_DIR(scripts)
AM_INIT_AUTOMAKE(foreign)
dnl AM_MAINTAINER_MODE
AC_PREREQ(2.59)
AC_PREFIX_DEFAULT(/usr)
AC_PROG_MAKE_SET

[CFLAGS="-g -O3 -pipe -Wall -Waggregate-return -Wcast-align -Wchar-subscripts -Wcomment -Wformat -Wimplicit -Wmissing-declarations -Wmissing-prototypes -Wnested-externs -Wno-long-long -Wparentheses -Wpointer-arith -Wredundant-decls -Wreturn-type -Wswitch -Wtrigraphs -Wuninitialized -Wunused -Wwrite-strings -Wshadow -Wstrict-prototypes -Wcast-qual $CFLAGS"]
AC_SUBST(CFLAGS)

# Compile flags for Linux
AC_DEFINE(_GNU_SOURCE, 1, GNU functions)
AC_DEFINE(_BSD_SOURCE, 1, BSD functions)

# Compile flags for Mac OS
AC_DEFINE(_DARWIN_C_SOURCE, 1, MacOS functions)

# Compile flags for FUSE
AC_DEFINE(FUSE_USE_VERSION, 26, FUSE API version)
AC_DEFINE(FUSE_FALLOCATE, 0, FUSE fallocate() support)

# Check for required programs
AC_PROG_INSTALL
AC_PROG_CC

# Check for required pkg-config'd stuff
PKG_PROG_PKG_CONFIG(0.19)
PKG_CHECK_MODULES(FUSE, fuse,
    [CFLAGS="${CFLAGS} ${FUSE_CFLAGS}"
    LDFLAGS="${LDFLAGS} ${FUSE_LIBS}"],
    [AC_MSG_ERROR(["fuse" not found in pkg-config])])

# Check for required libraries
AC_CHECK_LIB(curl, curl_version,,
	[AC_MSG_ERROR([required library libcurl missing])])
AC_CHECK_LIB(crypto, BIO_new,,
	[AC_MSG_ERROR([required library libcrypto missing])])
AC_CHECK_LIB(expat, XML_ParserCreate,,
	[AC_MSG_ERROR([required library expat missing])])
AC_CHECK_LIB(fuse, fuse_version,,
	[AC_MSG_ERROR([required library libfuse missing])])
AC_CHECK_LIB(z, compressBound,,
	[AC_MSG_ERROR([required library zlib missing])])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <curl/curl.h>
long x = CURLOPT_HTTP_CONTENT_DECODING;
]])],, [AC_MSG_ERROR([unable to compile with curl, or curl version is < 7.16.2])])

# See if FUSE version is 2.9.2 or later
AC_MSG_CHECKING([for fallocate() support in fuse])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <fuse/fuse.h>
struct fuse_operations x = { fallocate: (void*)1 };
]])],[AC_MSG_RESULT([yes]); AC_DEFINE(FUSE_FALLOCATE)],AC_MSG_RESULT([no]))

# Set some O/S specific stuff
case `uname -s` in
    Darwin|FreeBSD)
        AC_CHECK_LIB(pthread, pthread_create,,
            [AC_MSG_ERROR([required library libpthread missing])])
        ;;
    Linux)
        LDFLAGS="${LDFLAGS} -pthread"
        ;;
    *)
        ;;
esac

# Check for some O/S specific functions
AC_CHECK_DECLS(fdatasync)

# Check for required header files
AC_HEADER_STDC
AC_CHECK_HEADERS(assert.h ctype.h curl/curl.h err.h errno.h expat.h pthread.h stdarg.h stddef.h stdint.h stdio.h stdlib.h string.h syslog.h time.h unistd.h sys/queue.h openssl/bio.h openssl/buffer.h openssl/evp.h openssl/hmac.h openssl/md5.h zlib.h, [],
	[AC_MSG_ERROR([required header file '$ac_header' missing])])

# Optional features
AC_ARG_ENABLE(assertions,
    AC_HELP_STRING([--enable-assertions],
        [enable debugging sanity checks (default NO)]),
    [test x"$enableval" = "xyes" || AC_DEFINE(NDEBUG, 1, [disable assertions])],
    [AC_DEFINE(NDEBUG, 1, [disable assertions])])
AC_ARG_ENABLE(gprof,
    AC_HELP_STRING([--enable-gprof],
        [Compile and link with gprof(1) support (default NO)]),
    [test x"$enableval" = "xyes" && CFLAGS="${CFLAGS} -pg"])
AC_ARG_ENABLE(Werror,
    AC_HELP_STRING([--enable-Werror],
        [enable compilation with -Werror flag (default NO)]),
    [test x"$enableval" = "xyes" && CFLAGS="${CFLAGS} -Werror"])

# Generated files
AC_CONFIG_FILES(Makefile)
AC_CONFIG_FILES(s3backer.spec)
AM_CONFIG_HEADER(config.h)

# Go
AC_OUTPUT
