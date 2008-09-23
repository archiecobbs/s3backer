
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
#include "block_cache.h"
#include "ec_protect.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "s3b_config.h"
#include "erase.h"

#define BLOCKS_PER_DOT      0x100

int
s3backer_erase(struct s3b_config *config)
{
    struct s3backer_store *s3b;
    s3b_block_t block_num;
    char response[10];
    u_int *bitmap;
    const int bits_per_word = sizeof(*bitmap) * 8;
    size_t nwords;
    int count;
    int i;
    int j;
    int r;

    /* Double check with user */
    if (!config->force) {
        warnx("`--erase' flag given: erasing all blocks!");
        fprintf(stderr, "s3backer: is this correct? [y/N] ");
        *response = '\0';
        fgets(response, sizeof(response), stdin);
        while (*response && isspace(response[strlen(response) - 1]))
            response[strlen(response) - 1] = '\0';
        if (strcasecmp(response, "y") != 0 && strcasecmp(response, "yes") != 0) {
            warnx("not confirmed");
            return -1;
        }
    }

    /* Steal list of non-zero blocks */
    assert(config->http_io.nonzero_bitmap != NULL);
    bitmap = config->http_io.nonzero_bitmap;
    config->http_io.nonzero_bitmap = NULL;

    /* Create backing store */
    if ((s3b = s3backer_create_store(config)) == NULL) {
        warn("can't create s3backer_store");
        return -1;
    }

    /* Erase all non-zero blocks */
    if (!config->quiet) {
        fprintf(stderr, "s3backer: erasing blocks...");
        fflush(stderr);
    }
    nwords = (config->num_blocks + bits_per_word - 1) / bits_per_word;
    for (count = i = 0; i < nwords; i++) {
        if (bitmap[i] == 0)
            continue;
        for (j = 0; j < bits_per_word; j++) {
            if ((bitmap[i] & (1 << j)) != 0) {
                block_num = (s3b_block_t)i * bits_per_word + j;
                if ((r = (*s3b->write_block)(s3b, block_num, NULL, NULL)) != 0) {
                    warn("can't delete block %0*x", S3B_BLOCK_NUM_DIGITS, block_num);
                    return -1;
                }
            }
        }
        if ((++count % BLOCKS_PER_DOT) == 0 && !config->quiet) {
            fprintf(stderr, ".");
            fflush(stderr);
        }
    }
    if (!config->quiet)
        fprintf(stderr, "done\n");

    /* Close backing store */
    if (!config->quiet)
        fprintf(stderr, "s3backer: flushing cache...");
    (*s3b->destroy)(s3b);
    if (!config->quiet)
        fprintf(stderr, "done\n");

    /* Done */
    return 0;
}

