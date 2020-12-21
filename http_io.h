
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

/* Upload/download indexes */
#define HTTP_DOWNLOAD       0
#define HTTP_UPLOAD         1

/* Authentication types */
#define AUTH_VERSION_AWS2   "aws2"
#define AUTH_VERSION_AWS4   "aws4"

/* Storage classes */
#define STORAGE_CLASS_STANDARD              "STANDARD"
#define STORAGE_CLASS_STANDARD_IA           "STANDARD_IA"
#define STORAGE_CLASS_ONEZONE_IA            "ONEZONE_IA"
#define STORAGE_CLASS_REDUCED_REDUNDANCY    "REDUCED_REDUNDANCY"
#define STORAGE_CLASS_INTELLIGENT_TIERING   "INTELLIGENT_TIERING"
#define STORAGE_CLASS_GLACIER               "GLACIER"
#define STORAGE_CLASS_DEEP_ARCHIVE          "DEEP_ARCHIVE"
#define STORAGE_CLASS_OUTPOSTS              "OUTPOSTS"

/* Server side encryption types */
#define SSE_AES256                          "AES256"
#define SSE_AWS_KMS                         "aws:kms"

/* Configuration info structure for http_io store */
struct http_io_conf {
    char                *accessId;
    char                *accessKey;
    char                *iam_token;
    const char          *accessType;
    const char          *ec2iam_role;
    const char          *storage_class;
    const char          *authVersion;
    const char          *baseURL;
    const char          *region;
    const char          *bucket;
    const char          *prefix;
    const char          *user_agent;
    const char          *cacert;
    const char          *password;
    const char          *encryption;
    const char          *default_ce;
    u_int               key_length;
    int                 debug;
    int                 debug_http;
    int                 quiet;
    int                 compress;                   // zlib compression level
    int                 vhost;                      // use virtual host style URL
    bitmap_t            *nonzero_bitmap;            // is set to NULL by http_io_create()
    int                 blockHashPrefix;
    int                 insecure;
    u_int               block_size;
    off_t               num_blocks;
    u_int               timeout;
    u_int               initial_retry_pause;
    u_int               max_retry_pause;
    uintmax_t           max_speed[2];
    log_func_t          *log;
    const char          *sse;
    const char          *sse_key_id;
};

/* Statistics structure for http_io store */
struct http_io_evst {
    u_int               count;                      // number of occurrences
    double              time;                       // total time taken
};

struct http_io_stats {

    /* Block stats */
    u_int               normal_blocks_read;
    u_int               normal_blocks_written;
    u_int               zero_blocks_read;
    u_int               zero_blocks_written;
    u_int               empty_blocks_read;          // only when nonzero_bitmap != NULL
    u_int               empty_blocks_written;       // only when nonzero_bitmap != NULL

    /* HTTP transfer stats */
    struct http_io_evst http_heads;                 // total successful
    struct http_io_evst http_gets;                  // total successful
    struct http_io_evst http_puts;                  // total successful
    struct http_io_evst http_deletes;               // total successful
    u_int               http_unauthorized;
    u_int               http_forbidden;
    u_int               http_stale;
    u_int               http_verified;
    u_int               http_mismatch;
    u_int               http_5xx_error;
    u_int               http_4xx_error;
    u_int               http_other_error;
    u_int               http_canceled_writes;

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

    /* Misc */
    u_int               out_of_memory_errors;
};

/* http_io.c */
extern struct s3backer_store *http_io_create(struct http_io_conf *config);
extern void http_io_get_stats(struct s3backer_store *s3b, struct http_io_stats *stats);
extern void http_io_clear_stats(struct s3backer_store *s3b);
extern int http_io_parse_block(const char *prefix, off_t num_blocks, int blockHashPrefix, const char *name, s3b_block_t *block_num);
extern void http_io_format_block_hash(int blockHashPrefix, char *block_hash_buf, size_t bufsiz, s3b_block_t block_num);
extern int http_io_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);

