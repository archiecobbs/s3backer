
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

#include "s3backer.h"
#include "block_cache.h"
#include "ec_protect.h"
#include "zero_cache.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"
#include "erase.h"
#include "reset.h"
#include "util.h"

int
main(int argc, char **argv)
{
    const struct fuse_operations *fuse_ops;
    struct s3backer_store *s3b;
    struct s3b_config *config;

    // Get configuration
    if ((config = s3backer_get_config(argc, argv, 0, 0)) == NULL)
        return 1;

    // Handle `--erase' flag
    if (config->erase) {
        if (s3backer_erase(config) != 0)
            return 1;
        return 0;
    }

    // Handle `--reset' flag
    if (config->reset) {
        if (s3backer_reset(config) != 0)
            return 1;
        return 0;
    }

    // Create backing store
    if ((s3b = s3backer_create_store(config)) == NULL) {
        (*config->log)(LOG_ERR, "error creating s3backer_store: %s", strerror(errno));
        return 1;
    }

    // Start logging to syslog now
    if (!config->foreground)
        config->log = syslog_logger;

    // Setup FUSE operation hooks
    if ((fuse_ops = fuse_ops_create(&config->fuse_ops, s3b)) == NULL) {
        (*s3b->shutdown)(s3b);
        (*s3b->destroy)(s3b);
        return 1;
    }

    // Start
    (*config->log)(LOG_INFO, "s3backer process %lu for %s started", (u_long)getpid(), config->mount);
    if (fuse_main(config->fuse_args.argc, config->fuse_args.argv, fuse_ops, NULL) != 0) {
        (*config->log)(LOG_ERR, "error starting FUSE");
        fuse_ops_destroy();
        return 1;
    }

    // Done
    return 0;
}

