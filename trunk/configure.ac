# $Id$

AC_INIT([s3backer FUSE filesystem backed by Amazon S3], [1.0.6], [http://s3backer.googlecode.com/], [s3backer])
AC_CONFIG_AUX_DIR(scripts)
AM_INIT_AUTOMAKE
dnl AM_MAINTAINER_MODE
AC_PREREQ(2.59)
AC_REVISION($Id$)
AC_PREFIX_DEFAULT(/usr)
AC_PROG_MAKE_SET

[CFLAGS='-g -O3 -pipe -Wall -Waggregate-return -Wcast-align -Wchar-subscripts -Wcomment -Wformat -Wimplicit -Wmissing-declarations -Wmissing-prototypes -Wnested-externs -Wno-long-long -Wparentheses -Wpointer-arith -Wredundant-decls -Wreturn-type -Wswitch -Wtrigraphs -Wuninitialized -Wunused -Wwrite-strings -Wshadow -Wstrict-prototypes -Wcast-qual']
AC_SUBST(CFLAGS)

# Compile flags for Linux
AC_DEFINE(_GNU_SOURCE, 1, GNU functions)
AC_DEFINE(_BSD_SOURCE, 1, BSD functions)
AC_DEFINE(_XOPEN_SOURCE, 500, XOpen functions)

# Compile flags for Mac OS
AC_DEFINE(_DARWIN_C_SOURCE, 1, MacOS functions)

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

# Check for required programs
AC_PROG_INSTALL
AC_PROG_CC

# Check for required pkg-config'd stuff
PKG_PROG_PKG_CONFIG(0.19)
PKG_CHECK_MODULES(FUSE, fuse,,
    [AC_MSG_ERROR(["fuse" not found in pkg-config])])
PKG_CHECK_MODULES(GLIB2, glib-2.0,,
    [AC_MSG_ERROR(["glib-2.0" not found in pkg-config])])

# Check for required libraries
AC_CHECK_LIB(curl, curl_version,,
	[AC_MSG_ERROR([required library libcurl missing])])
AC_CHECK_LIB(crypto, OPENSSL_config,,
	[AC_MSG_ERROR([required library libcrypto missing])])

# Check for required header files
AC_HEADER_STDC
AC_CHECK_HEADERS(assert.h ctype.h curl/curl.h err.h errno.h pthread.h stdarg.h stddef.h stdint.h stdio.h stdlib.h string.h syslog.h time.h unistd.h openssl/bio.h openssl/buffer.h openssl/evp.h openssl/hmac.h openssl/md5.h, [],
	[AC_MSG_ERROR([required header file '$ac_header' missing])])

# Generated files
AC_CONFIG_FILES(Makefile)
AM_CONFIG_HEADER(config.h)

# Go
AC_OUTPUT
