
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 *
 * Copyright 2008-2020 Archie L. Cobbs <archie.cobbs@gmail.com>
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
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of portions of this program with the
 * OpenSSL library under certain conditions as described in each
 * individual source file, and distribute linked combinations including
 * the two.
 *
 * You must obey the GNU General Public License in all respects for all
 * of the code used other than OpenSSL. If you modify file(s) with this
 * exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do
 * so, delete this exception statement from your version. If you delete
 * this exception statement from all source files in the program, then
 * also delete it here.
 */

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#if HAVE_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif
#if HAVE_DECL_PR_SET_IO_FLUSHER
#include <sys/prctl.h>
#endif
#include <sys/queue.h>
#include <sys/wait.h>

// Add some queue.h definitions missing on Linux
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
#include <regex.h>
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
#include <signal.h>

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <zlib.h>
#include <fuse.h>

#ifdef __APPLE__
extern char **environ;
#endif

#ifndef FUSE_OPT_KEY_DISCARD
#define FUSE_OPT_KEY_DISCARD -4
#endif

// Bail out on error (implies bug)
#define CHECK_RETURN(x)         do {                                                                    \
                                    const int _r = (x);                                                 \
                                    (void)_r;                                                           \
                                    assert(_r == 0);                                                    \
                                } while (0)

// In case we don't have glibc >= 2.18
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE     0x01
#endif
#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE    0x02
#endif

// Integral type for holding a block number
typedef uint32_t    s3b_block_t;

// Integral type used for bitmaps
typedef uintptr_t   bitmap_t;

/*
 * How many hex digits we will use to print a block number.
 */
#define S3B_BLOCK_NUM_DIGITS    ((int)(sizeof(s3b_block_t) * 2))

// Logging function type
typedef void        log_func_t(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));

// Interactive non-zero block list callback function type. Returns zero for success, else positive errno to abort.
typedef int         block_list_func_t(void *arg, const s3b_block_t *block_nums, u_int num_blocks);

// Block write cancel check function type
typedef int         check_cancel_t(void *arg, s3b_block_t block_num);

// Backing store instance structure
struct s3backer_store {

    /*
     * Implementation private data
     */
    void        *data;

    /*
     * Create any background pthreads that may be required.
     *
     * This must be invoked prior to any of the following functions:
     *
     *      o block_read
     *      o block_read_part
     *      o block_write
     *      o block_write_part
     *
     * It should be invoked after the initial process fork() because it may create pthreads.
     *
     * Returns:
     *
     *  0       Success
     *  Other   Other error
     */
    int         (*create_threads)(struct s3backer_store *s3b);

    /*
     * Get meta-data associated with the underlying store.
     *
     * The information we acquire is:
     *  o Block size
     *  o Total size
     *
     * Returns:
     *
     *  0       Success
     *  ENOENT  Information not found
     *  Other   Other error
     */
    int         (*meta_data)(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);

    /*
     * Read and (optionally) set the mount token. The mount token is any 32 bit integer value greater than zero.
     *
     * Previous value, if any, is returned in *old_valuep (if not NULL). A returned value of zero means there was
     * no previous value.
     *
     * new_value can be:
     *   < 0    Don't change anything, just read the existing value, if any
     *   = 0    Clear the flag
     *   > 0    Set flag to new_value
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*set_mount_token)(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value);

    /*
     * Read one block. Never-written-to blocks will return all zeros.
     *
     * If not NULL, 'actual_etag' should be filled in with a value suitable for the 'expect_etag' parameter,
     * or all zeros if unknown.
     *
     * If 'expect_etag' is not NULL:
     *  - expect_etag should be the value returned from a previous call to read_block() or write_block().
     *  - If strict != 0, expect_etag must be the value returned from the most recent call to write_block()
     *    and the data must match it or else an error is returned. Aside from this check, read normally.
     *  - If strict == 0:
     *    - If block's ETag does not match expect_etag, expect_etag is ignored and the block is read normally
     *    - If block's ETag matches expect_etag, the implementation may either:
     *      - Ignore expect_etag and read the block normally; OR
     *      - Return EEXIST; the block may or may not also be read normally into *dest
     *
     * Returns zero on success or a (positive) errno value on error.
     * May return ENOTCONN if create_threads() has not yet been invoked.
     */
    int         (*read_block)(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
                  u_char *actual_etag, const u_char *expect_etag, int strict);

    /*
     * Read part of one block.
     *
     * This is an optional function; if not supported, this hook may be null.
     *
     * Returns zero on success or a (positive) errno value on error.
     * May return ENOTCONN if create_threads() has not yet been invoked.
     */
    int         (*read_block_part)(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);

    /*
     * Write one block.
     *
     * Passing src == NULL is equivalent to passing a block containing all zeros.
     *
     * If check_cancel != NULL, then it may be invoked periodically during the write. If so, and it ever
     * returns a non-zero value, then this function may choose to abort the write and return ECONNABORTED.
     *
     * Upon successful return, etag (if not NULL) will get updated with a value suitable for the 'expect_etag'
     * parameter of read_block(); if the block is all zeros, etag will be zeroed.
     *
     * Returns zero on success or a (positive) errno value on error.
     * May return ENOTCONN if create_threads() has not yet been invoked.
     */
    int         (*write_block)(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *etag,
                  check_cancel_t *check_cancel, void *arg);

    /*
     * Write part of one block.
     *
     * This is an optional function; if not supported, this hook may be null.
     *
     * Returns zero on success or a (positive) errno value on error.
     * May return ENOTCONN if create_threads() has not yet been invoked.
     */
    int         (*write_block_part)(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);

    /*
     * Bulk block zeroing (i.e., deletion).
     *
     * If a block to be deleted does not exist, then that's not an error - just do nothing in that case.
     *
     * Returns zero on success or a (positive) errno value if one or more blocks exist but cannot be deleted.
     */
    int         (*bulk_zero)(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks);

    /*
     * Flush any outstanding changes for the specific blocks to persistent storge.
     *
     * This function will block until (at least) all of the specified blocks are persisted.
     *
     * If "timeout" is greater than zero, impose a maximum time in milliseconds to wait for the flush to succeed;
     * if it takes any longer, return ETIMEDOUT.
     *
     * If "block_nums" is zero, then "num_blocks" is ignored and this means all "dirty" blocks should be flushed.
     *
     * If any attempts are made by other threads to write to any of the specified blocks while this function is waiting
     * (this includes the scenario in which a write is in progress in another thread when this function is invoked), then
     * it is not defined whether those other writes will also be flushed.
     */
    int         (*flush_blocks)(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout);

    /*
     * Identify all blocks that are, or could possibly be, non-zero.
     *
     * The callback must be invoked for all blocks which could possibly be non-zero. Note: the same block
     * may be reported more than once to "callback".
     *
     * If "callback" ever returns a non-zero value, survey should be aborted and an error returned.
     *
     * It's possible for "shutdown" to be invoked before this method returns; if so, the survey should be aborted
     * and ECANCELED returned.
     *
     * This method should never be invoked more than once at a time.
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*survey_non_zero)(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);

    /*
     * Shutdown this instance. Sync any dirty data to the underlying data store (as required).
     *
     * The only other concurrent activity that should possibly be happening when this function is invoked
     * is an invocation of "survey_non_zero". The only functions that may be invoked after this one are
     * "set_mount_token" and "destroy".
     */
    int         (*shutdown)(struct s3backer_store *s3b);

    /*
     * Destroy this instance. Free all resources. Caller must have invoked "shutdown" prior to this.
     */
    void        (*destroy)(struct s3backer_store *s3b);
};

// gitrev.c
extern const char *const s3backer_version;

// Issue #64 OpenSSL 1.1.0 compatibility - sslcompat.c
#if OPENSSL_VERSION_NUMBER < 0x10100000L
HMAC_CTX *HMAC_CTX_new(void);
void HMAC_CTX_free(HMAC_CTX *ctx);
EVP_MD_CTX *EVP_MD_CTX_new(void);
void EVP_MD_CTX_free(EVP_MD_CTX *ctx);
#endif
