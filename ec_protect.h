
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

/* Configuration info structure for ec_protect store */
struct ec_protect_conf {
    u_int               block_size;
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
extern void ec_protect_clear_stats(struct s3backer_store *s3b);

