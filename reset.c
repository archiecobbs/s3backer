
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
#include "reset.h"
#include "dcache.h"

int
s3backer_reset(struct s3b_config *config)
{
    struct s3backer_store *s3b = NULL;
    struct s3b_dcache *dcache = NULL;
    struct stat cache_file_stat;
    int ok = 0;
    int r;

    /* Logging */
    if (!config->quiet)
        warnx("resetting mount token for %s", config->description);

    /* Create temporary lower layer */
    if ((s3b = config->test ? test_io_create(&config->test_io) : http_io_create(&config->http_io)) == NULL) {
        warnx(config->test ? "test_io_create" : "http_io_create");
        goto fail;
    }

    /* Clear mount token */
    if ((r = (*s3b->set_mount_token)(s3b, NULL, 0)) != 0) {
        warnx("error clearing s3 mount token: %s", strerror(r));
        goto fail;
    }

    /* Open disk cache file, if any, and clear the mount token there too */
    if (config->block_cache.cache_file != NULL) {
        if (stat(config->block_cache.cache_file, &cache_file_stat) == -1) {
            if (errno != ENOENT) {
                warnx("error opening cache file `%s'", config->block_cache.cache_file);
                goto fail;
            }
        } else {
            if ((r = s3b_dcache_open(&dcache, config->log, config->block_cache.cache_file,
              config->block_cache.block_size, config->block_cache.cache_size, NULL, NULL, 0)) != 0)
                warnx("error opening cache file `%s': %s", config->block_cache.cache_file, strerror(r));
            if ((r = s3b_dcache_set_mount_token(dcache, NULL, 0)) != 0)
                warnx("error reading mount token from `%s': %s", config->block_cache.cache_file, strerror(r));
        }
    }

    /* Success */
    if (!config->quiet)
        warnx("done");
    ok = 1;

fail:
    /* Clean up */
    if (dcache != NULL)
        s3b_dcache_close(dcache);
    if (s3b != NULL) {
        (*s3b->shutdown)(s3b);
        (*s3b->destroy)(s3b);
    }
    return ok ? 0 : -1;
}

