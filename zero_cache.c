
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
#include "zero_cache.h"
#include "block_part.h"
#include "util.h"

/*
 * Cache for "known zero blocks.
 *
 * If a process reads or writes a bunch of zero blocks (for whatever reason), the regular
 * block cache can get blown out. This layer sits "on top" of the block cach layer and
 * caches zero blocks so the regular block cache doesn't have to deal with them as much.
 * Because only one bit is used for each block, we can cover the entire s3backer file.
 *
 * This layer caches the following bit of information for each block: whether it is known
 * for certain that the block is zero. If the bit is 1, the block is known to be zero; if
 * the bit is 0, the status of the block is unknown - it might or might not be zero.
 *
 * This layer does not directly utilize the information gather by the "--listBlocks" flag,
 * because that information applies to the HTTP layer, which is underneath the block cache.
 * However, this layer will indirectly learn from that information; for example, if a block
 * is read and the HTTP layer knows it is zero, then this layer will learn that information
 * when the zero block is returned back up the stack.
 *
 * Since (in theory) multiple threads can be reading and writing the same block at the same
 * time, we have to have choose how we behave in that scenario. This layer captures the state
 * of a block (known zero or not) at the end of each successful read or write I/O operation.
 * This new state affects any I/O operations initiated after that point in time. In practice
 * the kernel should never be doing two things at the same time to the same block.
 */

/* Internal state */
struct zero_cache_private {
    struct zero_cache_conf      *config;
    struct s3backer_store       *inner;         // underlying s3backer store
    bitmap_t                    *zeros;         // 1 = known to be zero, 0 = unknown
    pthread_mutex_t             mutex;
    struct zero_cache_stats     stats;
};

/* s3backer_store functions */
static int zero_cache_create_threads(struct s3backer_store *s3b);
static int zero_cache_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static int zero_cache_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value);
static int zero_cache_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict);
static int zero_cache_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *etag,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int zero_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int zero_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);
static int zero_cache_flush(struct s3backer_store *s3b);
static void zero_cache_destroy(struct s3backer_store *s3b);

/* Internal fuctions */
static void zero_cache_update_block(struct zero_cache_private *const priv, s3b_block_t block_num, int zero);

/* Special all-zeros MD5 value signifying a zeroed block */
static const u_char zero_etag[MD5_DIGEST_LENGTH];

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
zero_cache_create(struct zero_cache_conf *config, struct s3backer_store *inner)
{
    struct s3backer_store *s3b;
    struct zero_cache_private *priv;
    int r;

    /* Initialize structures */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail0;
    }
    s3b->create_threads = zero_cache_create_threads;
    s3b->meta_data = zero_cache_meta_data;
    s3b->set_mount_token = zero_cache_set_mount_token;
    s3b->read_block = zero_cache_read_block;
    s3b->write_block = zero_cache_write_block;
    s3b->read_block_part = zero_cache_read_block_part;
    s3b->write_block_part = zero_cache_write_block_part;
    s3b->flush = zero_cache_flush;
    s3b->destroy = zero_cache_destroy;
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail1;
    }
    priv->config = config;
    priv->inner = inner;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    if ((priv->zeros = bitmap_init(config->num_blocks)) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail3;
    }
    s3b->data = priv;

    /* Done */
    return s3b;

fail3:
    pthread_mutex_destroy(&priv->mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "zero_cache creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

static int
zero_cache_create_threads(struct s3backer_store *s3b)
{
    struct zero_cache_private *const priv = s3b->data;

    return (*priv->inner->create_threads)(priv->inner);
}

static int
zero_cache_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    struct zero_cache_private *const priv = s3b->data;

    return (*priv->inner->meta_data)(priv->inner, file_sizep, block_sizep);
}

static int
zero_cache_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value)
{
    struct zero_cache_private *const priv = s3b->data;

    return (*priv->inner->set_mount_token)(priv->inner, old_valuep, new_value);
}

static int
zero_cache_flush(struct s3backer_store *const s3b)
{
    struct zero_cache_private *const priv = s3b->data;

    return (*priv->inner->flush)(priv->inner);
}

static void
zero_cache_destroy(struct s3backer_store *const s3b)
{
    struct zero_cache_private *const priv = s3b->data;

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->mutex);

    /* Destroy inner store */
    (*priv->inner->destroy)(priv->inner);

    /* Free structures */
    pthread_mutex_destroy(&priv->mutex);
    free(priv->zeros);
    free(priv);
    free(s3b);
}

static int
zero_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;
    int r;

    /* Check for the case where we are reading a block that is already known to be zero */
    pthread_mutex_lock(&priv->mutex);
    if (bitmap_test(priv->zeros, block_num)) {
        priv->stats.read_hits++;
        pthread_mutex_unlock(&priv->mutex);
        if (expect_etag != NULL && strict && memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) != 0)
            return EIO;
        if (actual_etag != NULL)
            memcpy(actual_etag, zero_etag, MD5_DIGEST_LENGTH);
        memset(dest, 0, config->block_size);
        return 0;
    }
    pthread_mutex_unlock(&priv->mutex);

    /* Perform the actual read */
    r = (*priv->inner->read_block)(priv->inner, block_num, dest, actual_etag, expect_etag, strict);

    /* Update cache - if read was successful (or EEXIST case) */
    if (r == 0 || (expect_etag != NULL && !strict && r == EEXIST && memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) == 0)) {
        const int zero = block_is_zeros(dest, config->block_size);
        pthread_mutex_lock(&priv->mutex);
        zero_cache_update_block(priv, block_num, zero);
        pthread_mutex_unlock(&priv->mutex);
    }

    /* Done */
    return r;
}

static int
zero_cache_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *caller_etag,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;
    int zero;
    int r;

    /* Detect zero blocks */
    if (src != NULL && block_is_zeros(src, config->block_size))
        src = NULL;
    zero = src == NULL;

    /* Handle the case where we think this block is zero */
    pthread_mutex_lock(&priv->mutex);
    if (bitmap_test(priv->zeros, block_num)) {
        if (zero) {                                             /* ok, it's still zero -> return immediately */
            priv->stats.write_hits++;
            pthread_mutex_unlock(&priv->mutex);
            if (caller_etag != NULL)
                memcpy(caller_etag, zero_etag, MD5_DIGEST_LENGTH);
            return 0;
        }
        zero_cache_update_block(priv, block_num, 0);            /* be conservative and say we are no longer sure */
    }
    pthread_mutex_unlock(&priv->mutex);

    /* Perform the actual write */
    r = (*priv->inner->write_block)(priv->inner, block_num, src, caller_etag, check_cancel, check_cancel_arg);

    /* Update cache */
    pthread_mutex_lock(&priv->mutex);
    if (r == 0)
        zero_cache_update_block(priv, block_num, zero);         /* update block status based on successful write */
    else
        zero_cache_update_block(priv, block_num, 0);            /* error: be conservative and say we are no longer sure */
    pthread_mutex_unlock(&priv->mutex);

    /* Done */
    return r;
}

static int
zero_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;

    return block_part_read_block_part(s3b, block_num, config->block_size, off, len, dest);
}

static int
zero_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;

    return block_part_write_block_part(s3b, block_num, config->block_size, off, len, src);
}

void
zero_cache_get_stats(struct s3backer_store *s3b, struct zero_cache_stats *stats)
{
    struct zero_cache_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    pthread_mutex_unlock(&priv->mutex);
}

void
zero_cache_clear_stats(struct s3backer_store *s3b)
{
    struct zero_cache_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    priv->stats.read_hits = 0;
    priv->stats.write_hits = 0;
    pthread_mutex_unlock(&priv->mutex);
}

// This assumes mutex is held
static void
zero_cache_update_block(struct zero_cache_private *const priv, s3b_block_t block_num, int zero)
{
    if (bitmap_test(priv->zeros, block_num) == zero)
        return;
    bitmap_set(priv->zeros, block_num, zero);
    if (zero)
        priv->stats.current_cache_size++;
    else
        priv->stats.current_cache_size--;
}
