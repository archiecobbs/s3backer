
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

#include "s3backer.h"

int
main(int argc, char **argv)
{
    const struct fuse_operations *fuse_ops;
    struct s3backer_conf *config;

    /* Get configuration */
    if ((config = s3backer_get_config(argc, argv)) == NULL)
        exit(1);

    /* Get FUSE operation hooks */
    fuse_ops = s3backer_get_fuse_ops(config);

    /* Start */
    (*config->log)(LOG_INFO, "s3backer process %lu for %s started", (u_long)getpid(), config->mount);
    return fuse_main(config->fuse_args.argc, config->fuse_args.argv, fuse_ops, NULL);
}

