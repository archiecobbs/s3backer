
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

/* Configuration info structure for ec_protect store */
struct ec_protect_conf {
    u_int               block_size;
    off_t               num_blocks;
    u_int               min_write_delay;
    u_int               cache_time;
    u_int               cache_size;
    log_func_t          *log;
};

/* Statistics structure for ec_protect store */
struct ec_protect_stats {
    u_int               current_cache_size;
    u_int               cache_data_hits;
    uint64_t            cache_full_delay;
    uint64_t            repeated_write_delay;
    u_int               out_of_memory_errors;
};

/* ec_protect.c */
extern struct s3backer_store *ec_protect_create(struct ec_protect_conf *config, struct s3backer_store *inner);
extern void ec_protect_get_stats(struct s3backer_store *s3b, struct ec_protect_stats *stats);

