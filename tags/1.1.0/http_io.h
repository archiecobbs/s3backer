
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

/* Configuration info structure for http_io store */
struct http_io_conf {
    const char          *accessId;
    const char          *accessKey;
    const char          *accessType;
    const char          *baseURL;
    const char          *bucket;
    const char          *prefix;
    const char          *user_agent;
    int                 debug;
    int                 assume_empty;
    u_int               block_size;
    off_t               num_blocks;
    u_int               timeout;
    u_int               initial_retry_pause;
    u_int               max_retry_pause;
    log_func_t          *log;
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
    u_int               empty_blocks_read;          // only when `--assumeEmpty'
    u_int               empty_blocks_written;       // only when `--assumeEmpty'

    /* HTTP transfer stats */
    struct http_io_evst http_heads;                 // total successful
    struct http_io_evst http_gets;                  // total successful
    struct http_io_evst http_puts;                  // total successful
    struct http_io_evst http_deletes;               // total successful
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

    /* Misc */
    u_int               out_of_memory_errors;
};

/* http_io.c */
extern struct s3backer_store *http_io_create(struct http_io_conf *config);
extern void http_io_get_stats(struct s3backer_store *s3b, struct http_io_stats *stats);

