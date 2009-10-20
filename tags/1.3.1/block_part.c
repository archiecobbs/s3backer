
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

#include "s3backer.h"
#include "block_part.h"

/*
 * Generic "dumb" implementation of the read_block_part function.
 */
int
block_part_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num,
    u_int block_size, u_int off, u_int len, void *dest)
{
    u_char *buf;
    int r;

    /* Sanity check */
    assert(off <= block_size);
    assert(len <= block_size);
    assert(off + len <= block_size);

    /* Check for degenerate cases */
    if (len == 0)
        return 0;
    if (off == 0 && len == block_size)
        return (*s3b->read_block)(s3b, block_num, dest, NULL, NULL, 0);

    /* Allocate buffer */
    if ((buf = malloc(block_size)) == NULL)
        return errno;

    /* Read entire block */
    if ((r = (*s3b->read_block)(s3b, block_num, buf, NULL, NULL, 0)) != 0) {
        free(buf);
        return r;
    }

    /* Copy out desired fragment */
    memcpy(dest, buf + off, len);

    /* Done */
    free(buf);
    return 0;
}

/*
 * Generic "dumb" implementation of the write_block_part function.
 */
int
block_part_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num,
    u_int block_size, u_int off, u_int len, const void *src)
{
    u_char *buf;
    int r;

    /* Sanity check */
    assert(off <= block_size);
    assert(len <= block_size);
    assert(off + len <= block_size);

    /* Check for degenerate cases */
    if (len == 0)
        return 0;
    if (off == 0 && len == block_size)
        return (*s3b->write_block)(s3b, block_num, src, NULL, NULL, NULL);

    /* Allocate buffer */
    if ((buf = malloc(block_size)) == NULL)
        return errno;

    /* Read entire block */
    if ((r = (*s3b->read_block)(s3b, block_num, buf, NULL, NULL, 0)) != 0) {
        free(buf);
        return r;
    }

    /* Write in supplied fragment */
    memcpy(buf + off, src, len);

    /* Write back entire block */
    if ((r = (*s3b->write_block)(s3b, block_num, buf, NULL, NULL, NULL)) != 0) {
        free(buf);
        return r;
    }

    /* Done */
    free(buf);
    return 0;
}

