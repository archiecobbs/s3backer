
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

#include <assert.h>
#include <ctype.h>
#include <curl/curl.h>
#include <err.h>
#include <errno.h>
#include <glib/ghash.h>
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

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>

#define FUSE_USE_VERSION 26
#include <fuse/fuse.h>

#ifndef FUSE_OPT_KEY_DISCARD
#define FUSE_OPT_KEY_DISCARD -4
#endif

/*
 * Integer holding a block number; must be small enough to fit inside a pointer.
 * If you make this > 32 bits, you may want to change s3backer_get_url() too.
 */
typedef uint32_t    s3b_block_t;

/* Configuration info structure */
struct s3backer_conf {
    const char          *accessId;
    const char          *accessKey;
    const char          *accessFile;
    const char          *accessType;
    const char          *baseURL;
    const char          *bucket;
    const char          *prefix;
    const char          *filename;
    const char          *stats_filename;
    const char          *mount;
    const char          *user_agent;
    int                 debug;
    int                 force;
    int                 assume_empty;
    int                 read_only;
    uid_t               uid;
    gid_t               gid;
    time_t              start_time;
    u_int               block_size;
    u_int               block_bits;
    off_t               file_size;
    off_t               num_blocks;
    int                 file_mode;
    u_int               timeout;
    u_int               initial_retry_pause;
    u_int               max_retry_pause;
    u_int               min_write_delay;
    u_int               cache_time;
    u_int               cache_size;
    struct fuse_args    fuse_args;
    void                (*log)(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));

    // These are only used during parsing
    const char          *file_size_str;
    const char          *block_size_str;
};

/* Statistics structure */
struct s3backer_stats {

    /* Block stats */
    u_int               total_blocks_read;
    u_int               total_blocks_written;
    u_int               zero_blocks_read;
    u_int               zero_blocks_written;
    u_int               empty_blocks_read;          // only when `--assumeEmpty'
    u_int               empty_blocks_written;       // only when `--assumeEmpty'

    /* HTTP transfer stats */
    u_int               http_heads;                 // total successful
    u_int               http_gets;                  // total successful
    u_int               http_puts;                  // total successful
    u_int               http_deletes;               // total successful
    double              http_total_time;            // successful operations
    u_int               http_unauthorized;
    u_int               http_forbidden;
    u_int               http_stale;
    u_int               http_5xx_error;
    u_int               http_4xx_error;
    u_int               http_other_error;

    /* CURL stats */
    u_int               curl_handles_created;
    u_int               curl_handles_reused;
    u_int               curl_timeouts;
    u_int               curl_connect_failed;
    u_int               curl_host_unknown;
    u_int               curl_out_of_memory;
    u_int               curl_other_error;

    /* Retry stats */
    u_int               num_retries;
    uint64_t            retry_delay;

    /* Cache stats */
    u_int               current_cache_size;
    u_int               cache_data_hits;
    uint64_t            cache_full_delay;
    uint64_t            repeated_write_delay;

    /* Misc */
    u_int               out_of_memory_errors;
};

/* Backing store instance structure */
struct s3backer_store {

    /*
     * Read one block. Never-written-to blocks will return containing all zeroes.
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*read_block)(struct s3backer_store *s3b, s3b_block_t block_num, void *dest);

    /*
     * Write one block. Blocks that are all zeroes are actually deleted instead
     * of being written.
     *
     * Returns zero on success or a (positive) errno value on error.
     */
    int         (*write_block)(struct s3backer_store *s3b, s3b_block_t block_num, const void *src);

    /*
     * Auto-detect block size and total size based on the first block.
     *
     * Returns:
     *
     *  0       Success
     *  ENOENT  Block not found
     *  ENXIO   Response was missing one of the two required headers
     *  Other   Other error
     */
    int         (*detect_sizes)(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);

    /*
     * Get a statistics snapshot.
     */
    void        (*get_stats)(struct s3backer_store *s3b, struct s3backer_stats *stats);

    /*
     * Destroy this instance.
     */
    void        (*destroy)(struct s3backer_store *s3b);

    /*
     * Implementation private data
     */
    void        *data;
};

/* s3backer.c */
extern struct s3backer_store *s3backer_create(struct s3backer_conf *config);

/* fuse_ops.c */
const struct fuse_operations *s3backer_get_fuse_ops(struct s3backer_conf *config);

/* config.c */
struct s3backer_conf *s3backer_get_config(int argc, char **argv);

/* svnrev.c */
extern const int s3backer_svnrev;

