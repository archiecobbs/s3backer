
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

/* Forward decl's */
struct s3b_config;
struct s3backer_store;

/* Function types */
typedef void printer_t(void *prarg, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
typedef void print_stats_t(void *prarg, printer_t *printer);
typedef void clear_stats_t(void);

/* Configuration info structure for fuse_ops */
struct fuse_ops_conf {
    struct s3b_config       *s3bconf;
    print_stats_t           *print_stats;
    clear_stats_t           *clear_stats;
    int                     read_only;
    int                     direct_io;
    const char              *filename;
    const char              *stats_filename;
    uid_t                   uid;
    gid_t                   gid;
    u_int                   block_size;
    s3b_block_t             num_blocks;
    int                     file_mode;
    log_func_t              *log;
};

/* Private information */
struct fuse_ops_private {
    struct s3backer_store   *s3b;
    u_int                   block_bits;
    off_t                   file_size;
    time_t                  start_time;
    time_t                  file_atime;
    time_t                  file_mtime;
    time_t                  stats_atime;
};

/* fuse_ops.c */
const struct fuse_operations *fuse_ops_create(struct fuse_ops_conf *config, struct s3backer_store *s3b);
void fuse_ops_destroy(void);

