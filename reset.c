
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

#include "s3backer.h"
#include "block_cache.h"
#include "ec_protect.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"
#include "reset.h"
#include "dcache.h"

int
s3backer_reset(struct s3b_config *config)
{
    struct s3backer_store *s3b = NULL;
    int ok = 0;
    int r;

    /* Logging */
    if (!config->quiet)
        warnx("resetting mounted flag for %s", config->description);

    /* Create temporary lower layer */
    if ((s3b = config->test ? test_io_create(&config->http_io) : http_io_create(&config->http_io)) == NULL) {
        warnx(config->test ? "test_io_create" : "http_io_create");
        goto fail;
    }

    /* Clear mounted flag */
    if ((r = (*s3b->set_mounted)(s3b, NULL, 0)) != 0) {
        warnx("error clearing s3 mounted flag: %s", strerror(r));
        goto fail;
    }

    if (config->block_cache.cache_file != NULL) {
        if (!config->quiet)
            warnx("resetting mounted flag for %s", config->block_cache.cache_file);
        if ((r = s3b_dcache_reset_mount_token(config->block_cache.cache_file)) != 0) {
            warnx("error clearing cache file mounted flag: %s", strerror(r));
            goto fail;
        }
    }

    /* Success */
    if (!config->quiet)
        warnx("done");
    ok = 1;

fail:
    /* Clean up */
    if (s3b != NULL)
        (*s3b->destroy)(s3b);
    return ok ? 0 : -1;
}

