
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

/* Overal application configuration info */
struct s3b_config {

    /* Various sub-module configurations */
    struct block_cache_conf     block_cache;
    struct fuse_ops_conf        fuse_ops;
    struct ec_protect_conf      ec_protect;
    struct http_io_conf         http_io;

    /* Common/global stuff */
    const char                  *accessFile;
    const char                  *mount;
    u_int                       block_size;
    off_t                       file_size;
    off_t                       num_blocks;
    int                         debug;
    int                         quiet;
    int                         force;
    int                         test;
    int                         ssl;
    int                         no_auto_detect;
    int                         list_blocks;
    struct fuse_args            fuse_args;
    log_func_t                  *log;

    /* These are only used during parsing */
    const char                  *file_size_str;
    const char                  *block_size_str;
};

extern struct s3b_config *s3backer_get_config(int argc, char **argv);
extern struct s3backer_store *s3backer_create_store(struct s3b_config *config);

