
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
    char                        description[768];
    u_int                       block_size;
    off_t                       file_size;
    off_t                       num_blocks;
    int                         debug;
    int                         erase;
    int                         reset;
    int                         quiet;
    int                         force;
    int                         test;
    int                         ssl;
    int                         no_auto_detect;
    int                         list_blocks;
    struct fuse_args            fuse_args;
    log_func_t                  *log;

    /* These are only used during command line parsing */
    const char                  *file_size_str;
    const char                  *block_size_str;
    const char                  *password_file;
    const char                  *max_speed_str[2];
    int                         encrypt;
};

extern struct s3b_config *s3backer_get_config(int argc, char **argv);
extern struct s3backer_store *s3backer_create_store(struct s3b_config *config);

