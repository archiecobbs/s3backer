
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 * 
 * Copyright 2008-2009 Archie L. Cobbs <archie@dellroad.org>
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

/* Forward decl's */
struct s3b_config;

/* Function types */
typedef void printer_t(void *prarg, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
typedef void print_stats_t(void *prarg, printer_t *printer);

/* Configuration info structure for fuse_ops */
struct fuse_ops_conf {
    struct s3b_config       *s3bconf;
    print_stats_t           *print_stats;
    int                     read_only;
    const char              *filename;
    const char              *stats_filename;
    uid_t                   uid;
    gid_t                   gid;
    u_int                   block_size;
    off_t                   num_blocks;
    int                     file_mode;
    log_func_t              *log;
};

/* fuse_ops.c */
const struct fuse_operations *fuse_ops_create(struct fuse_ops_conf *config);

