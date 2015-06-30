
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 * 
 * Copyright 2008-2011 Archie L. Cobbs <archie@dellroad.org>
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
 */

/* Upload/download indexes */
#define HTTP_DOWNLOAD       0
#define HTTP_UPLOAD         1

/* Authentication types */
#define AUTH_VERSION_AWS2   "aws2"
#define AUTH_VERSION_AWS4   "aws4"

#define S3_MAX_LIST_BLOCKS_CHUNK	        1000
#define MAXIMUM_CONCURRENT_BULK_DELETES     16


/* Configuration info structure for http_io store */
struct http_io_conf {
    char                *accessId;
    char                *accessKey;
    char                *iam_token;
    const char          *accessType;
    const char          *ec2iam_role;
    const char          *authVersion;
    const char          *baseURL;
    const char          *region;
    const char          *bucket;
    const char          *prefix;
    const char          *user_agent;
    const char          *cacert;
    const char          *password;
    const char          *encryption;
    u_int               key_length;
    int                 debug;
    int                 debug_http;
    int                 quiet;
    int                 rrs;                        // reduced redundancy storage
    int                 compress;                   // zlib compression level
    int                 vhost;                      // use virtual host style URL
    u_int               *nonzero_bitmap;            // is set to NULL by http_io_create()
    int                 insecure;
    u_int               block_size;
    off_t               num_blocks;
    u_int               timeout;
    u_int               initial_retry_pause;
    u_int               max_retry_pause;
    uintmax_t           max_speed[2];
	int                 max_keys;
    log_func_t          *log;
    unsigned int        list_blocks_threads;
	volatile int        use_bulk_delete;
    unsigned int        max_bulk_delete_threads;
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
    struct http_io_evst http_bulk_deletes;          // total successful
    u_int               http_active_bulk_deletes;  // currently running
    u_int               http_active_connections;    // Active io
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
extern int http_io_parse_block(struct http_io_conf *config, const char *name, s3b_block_t *block_num, s3b_block_t *reversed_block_num);

