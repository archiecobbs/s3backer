
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

/* Configuration info structure for block_cache */
struct block_cache_conf {
    u_int               block_size;
    u_int               cache_size;
    u_int               write_delay;
    u_int               max_dirty;
    u_int               synchronous;
    u_int               timeout;
    u_int               num_threads;
    u_int               read_ahead;
    u_int               read_ahead_trigger;
    u_int               no_verify;
    const char          *cache_file;
    log_func_t          *log;
};

/* Usage statistics for block_cache */
/* 36 bytes for each block */
struct block_cache_usage_stats {
    uint16_t            num_reads;                /* Number of read operations */
    uint64_t            cumulative_reads_time;    /* Sum of all the differences between read opeation times */
    uint64_t            last_read_timestamp;      /* Last time we read this block */

    uint16_t            num_writes;               /* Number of write operations */
    uint64_t            cumulative_writes_time;   /* Sum of all the differences between writes opeation times */
    uint64_t            last_write_timestamp;     /* Last time we wrote this block */
}; 

/* Statistics structure for block_cache */
struct block_cache_stats {
    u_int               initial_size;
    u_int               current_size;
    double              dirty_ratio;
    u_int               read_hits;
    u_int               read_misses;
    u_int               write_hits;
    u_int               write_misses;
    u_int               verified;
    u_int               mismatch;
    u_int               out_of_memory_errors;
    struct block_cache_usage_stats *usage_stats;  /* Pointer to array of usage stats (for each block) */
};

/* block_cache.c */
extern struct s3backer_store *block_cache_create(struct block_cache_conf *config, struct s3backer_store *inner);
extern void block_cache_get_stats(struct s3backer_store *s3b, struct block_cache_stats *stats);

