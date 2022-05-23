
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
#include "block_part.h"
#include "util.h"

// Block read/write lock states: 0x00-0xfe: there are this many readers; 0xff: there is one writer
#define BLOCK_IDLE              ((u_int8_t)0x00)
#define BLOCK_WRITING           ((u_int8_t)0xff)
#define BLOCK_READERS_MAX       ((u_int8_t)0xfe)            // inclusive upper bound

// Internal state
struct block_part {
    u_int                       block_size;
    s3b_block_t                 num_blocks;
    pthread_mutex_t             mutex;
    pthread_cond_t              wakeup;
    u_int8_t                    *block_states;              // read/write locks for each block
};

struct block_part *
block_part_create(u_int block_size, s3b_block_t num_blocks)
{
    struct block_part *priv;
    int r;

    if ((priv = malloc(sizeof(*priv))) == NULL)
        return NULL;
    memset(priv, 0, sizeof(*priv));
    priv->block_size = block_size;
    priv->num_blocks = num_blocks;
    if ((priv->block_states = calloc(num_blocks, sizeof(*priv->block_states))) == NULL) {
        r = errno;
        goto fail1;
    }
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    if ((r = pthread_cond_init(&priv->wakeup, NULL)) != 0)
        goto fail3;

    // Done
    return priv;

    // Fail
fail3:
    pthread_mutex_destroy(&priv->mutex);
fail2:
    free(priv->block_states);
fail1:
    free(priv);
    errno = r;
    return NULL;
}

void
block_part_destroy(struct block_part **block_partp)
{
    struct block_part *const priv = *block_partp;

    *block_partp = NULL;
    pthread_cond_destroy(&priv->wakeup);
    pthread_mutex_destroy(&priv->mutex);
    free(priv->block_states);
    free(priv);
}

/*
 * Read a partial block by reading the whole block, then copying the part we want.
 *
 * For any given block, we can do this simultaneously with other readers, but not while there are any writers.
 */
int
block_part_read_block_part(struct s3backer_store *s3b, struct block_part *const priv, const struct boundary_edge *const edge)
{
    u_int8_t block_state;
    u_char *buf;
    int r;

    // Sanity check
    assert(edge->offset <= priv->block_size);
    assert(edge->length > 0);
    assert(edge->length < priv->block_size);
    assert(edge->offset + edge->length <= priv->block_size);

    // Allocate buffer
    if ((buf = malloc(priv->block_size)) == NULL)
        return errno;

    // Increment readers count
    pthread_mutex_lock(&priv->mutex);
    while (1) {
        switch ((block_state = priv->block_states[(size_t)edge->block])) {
        case BLOCK_WRITING:
        case BLOCK_READERS_MAX:
            pthread_cond_wait(&priv->wakeup, &priv->mutex);
            continue;
        default:
            break;
        }
        priv->block_states[(size_t)edge->block] = (u_int8_t)(block_state + 1);          // increment #readers
        break;
    }
    pthread_mutex_unlock(&priv->mutex);

    // Read entire block
    if ((r = (*s3b->read_block)(s3b, edge->block, buf, NULL, NULL, 0)) != 0)
        goto done;

    // Copy out desired fragment
    memcpy(edge->data, buf + edge->offset, edge->length);

done:
    // Decrement readers count
    pthread_mutex_lock(&priv->mutex);
    block_state = priv->block_states[(size_t)edge->block];
    assert(block_state != BLOCK_IDLE);
    assert(block_state != BLOCK_WRITING);
    if (block_state == BLOCK_READERS_MAX)                                       // there might be a waiting reader
        pthread_cond_signal(&priv->wakeup);
    block_state = (u_int8_t)(block_state - 1);                                  // decrement #readers
    if ((priv->block_states[(size_t)edge->block] = block_state) == BLOCK_IDLE)  // there might be a waiting writer
        pthread_cond_signal(&priv->wakeup);
    pthread_mutex_unlock(&priv->mutex);

    // Done
    free(buf);
    return r;
}

/*
 * Write a partial block by reading the whole block, patching it, and writing it back.
 *
 * If edge->data is NULL then write zeroes.
 *
 * For any given block, we can only do this if there are no other simultaneous readers or writers.
 */
int
block_part_write_block_part(struct s3backer_store *s3b, struct block_part *const priv, const struct boundary_edge *const edge)
{
    const void *data = edge->data != NULL ? edge->data : zero_block;        // if edge->data is NULL then write zeros
    u_char *buf;
    int r;

    // Sanity check
    assert(edge->offset <= priv->block_size);
    assert(edge->length > 0);
    assert(edge->length < priv->block_size);
    assert(edge->offset + edge->length <= priv->block_size);

    // Allocate buffer
    if ((buf = malloc(priv->block_size)) == NULL)
        return errno;

    // Grab exclusive lock on this block
    pthread_mutex_lock(&priv->mutex);
    while (1) {
        if (priv->block_states[(size_t)edge->block] != BLOCK_IDLE) {
            pthread_cond_wait(&priv->wakeup, &priv->mutex);
            continue;
        }
        priv->block_states[(size_t)edge->block] = BLOCK_WRITING;            // grab exclusive write lock
        break;
    }
    pthread_mutex_unlock(&priv->mutex);

    // Read entire block
    if ((r = (*s3b->read_block)(s3b, edge->block, buf, NULL, NULL, 0)) != 0)
        goto done;

    // Write in supplied fragment
    memcpy(buf + edge->offset, data, edge->length);

    // Write back entire block
    r = (*s3b->write_block)(s3b, edge->block, buf, NULL, NULL, NULL);

done:
    // Release exclusive lock on this block
    pthread_mutex_lock(&priv->mutex);
    assert(priv->block_states[(size_t)edge->block] == BLOCK_WRITING);
    priv->block_states[(size_t)edge->block] = BLOCK_IDLE;                   // release exclusive write lock
    pthread_cond_signal(&priv->wakeup);                                     // there might be a waiting reader or writer
    pthread_mutex_unlock(&priv->mutex);

    // Done
    free(buf);
    return r;
}
