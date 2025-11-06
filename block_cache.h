
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 *
 * Copyright 2008-2023 Archie L. Cobbs <archie.cobbs@gmail.com>
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

// Configuration info structure for block_cache
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
    u_int               no_abort_dirty_write;
    u_int               no_verify;
    u_int               fadvise;
    u_int               recover_dirty_blocks;
    u_int               perform_flush;
    u_int               num_protected;
    const char          *cache_file;
    log_func_t          *log;
};

// Statistics structure for block_cache
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
};

// block_cache.c
extern struct s3backer_store *block_cache_create(struct block_cache_conf *config, struct s3backer_store *inner);
extern void block_cache_get_stats(struct s3backer_store *s3b, struct block_cache_stats *stats);
extern void block_cache_clear_stats(struct s3backer_store *s3b);

