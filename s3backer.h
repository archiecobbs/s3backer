
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 * 
 * Copyright 2008 Archie L. Cobbs <archie@dellroad.org>
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 *
 * $Id$
 */

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

/* Add some queue.h definitions missing on Linux */
#ifndef LIST_FIRST
#define LIST_FIRST(head)        ((head)->lh_first)
#endif
#ifndef LIST_NEXT
#define LIST_NEXT(item, field)  ((item)->field.le_next)
#endif
#ifndef TAILQ_FIRST
#define TAILQ_FIRST(head)       ((head)->tqh_first)
#endif
#ifndef TAILQ_NEXT
#define TAILQ_NEXT(item, field) ((item)->field.tqe_next)
#endif

#include <assert.h>
#include <ctype.h>
#include <curl/curl.h>
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <expat.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>

#define FUSE_USE_VERSION 25
#include <fuse/fuse.h>

#ifndef FUSE_OPT_KEY_DISCARD
#define FUSE_OPT_KEY_DISCARD -4
#endif

/*
 * Integral type for holding a block number.
 */
typedef uint32_t    s3b_block_t;

/*
 * How many hex digits we will use to print a block number.
 */
#define S3B_BLOCK_NUM_DIGITS    (sizeof(s3b_block_t) * 2)

/* Logging function type */
typedef void        log_func_t(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));

/* Block list callback function type */
typedef void        block_list_func_t(void *arg, s3b_block_t block_num);

/* Backing store instance structure */
struct s3backer_store {

    /*
     * Read one block. Never-written-to blocks will return containing all zeroes.
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*read_block)(struct s3backer_store *s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5);

    /*
     * Write one block. Blocks that are all zeroes are actually deleted instead
     * of being written.
     *
     * If src == NULL, block contains all zeroes; otherwise, if md5 == NULL, contents of block are unknown;
     * otherwise, contents of block are known to NOT be all zeroes.
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*write_block)(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, const u_char *md5);

    /*
     * Identify all non-zero blocks.
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*list_blocks)(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);

    /*
     * Destroy this instance.
     */
    void        (*destroy)(struct s3backer_store *s3b);

    /*
     * Implementation private data
     */
    void        *data;
};

/* svnrev.c */
extern const int s3backer_svnrev;

