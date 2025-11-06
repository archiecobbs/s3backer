
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

// Configuration info structure for zero_cache store
struct zero_cache_conf {
    u_int               block_size;
    s3b_block_t         num_blocks;
    int                 list_blocks;
    log_func_t          *log;
};

// Statistics structure for zero_cache store
struct zero_cache_stats {
    s3b_block_t         current_cache_size;
    u_int               read_hits;
    u_int               write_hits;
};

// zero_cache.c
extern struct s3backer_store *zero_cache_create(struct zero_cache_conf *config, struct s3backer_store *inner, int is_lower);
extern void zero_cache_init_nonzero(struct s3backer_store *s3b, const u_int *non_zero);
extern void zero_cache_get_stats(struct s3backer_store *s3b, struct zero_cache_stats *stats);
extern void zero_cache_clear_stats(struct s3backer_store *s3b);

