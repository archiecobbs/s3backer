
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 *
 * Copyright 2008-2023 Archie L. Cobbs <archie.cobbs@gmail.com>
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
#include "util.h"

/*
 * Cache for "known zero" blocks.
 *
 * If a process reads or writes a bunch of zero blocks (for whatever reason), the regular
 * block cache can get blown out. This layer sits "on top" of the block cach layer and
 * caches zero blocks so the regular block cache doesn't have to deal with them as much.
 * Because only one bit is used for each block, we can cover the entire s3backer file.
 *
 * This layer caches the following bit of information for each block: whether it is known
 * for certain that the block is zero. If the bit is 1, the block is known to be zero; if
 * the bit is 0, the status of the block is unknown - it might or might not be zero.
 * Initially all blocks are in the unknown status, and we update the bitmap as we see blocks
 * being read and written.
 *
 * Since (in theory) multiple threads can be reading and writing the same block at the same
 * time, we have to have choose how we behave in that scenario. This layer captures the state
 * of a block (known zero or not) at the end of each successful read or write I/O operation.
 * This new state affects any I/O operations initiated after that point in time. In practice
 * the kernel should never be doing two things at the same time to the same block.
 *
 * If the "--listBlocks" flag was specified, this layer also initiates a survey of non-zero
 * blocks immediately after startup. The survey starts with a separate survey bitmap that
 * initialized with all blocks as zero, and then as we see non-zero blocks from the survey
 * we flip those bits. But the regular operation described above is also occuring during
 * the survey, so when we see any non-zero (or unknown) blocks from regular operation, we
 * flip those bits in the survey bitmap as well. Therefore at the end of the survey, only
 * the truly zero blocks should remain in the survey bitmap. This bitmap is then OR'd into
 * the main zero bitmap.
 */

// Internal state
struct zero_cache_private {
    struct zero_cache_conf      *config;
    struct s3backer_store       *inner;         // underlying s3backer store
    bitmap_t                    *zeros;         // 1 = known to be zero, 0 = unknown
    pthread_mutex_t             mutex;
    struct zero_cache_stats     stats;
    volatile int                stopping;

    // Survey thread info
    int                         thread_started; // the survey thread was started
    bitmap_t                    *survey_zeros;  // 1 = might still be zero, 0 = might not be zero; NULL if no survey running
    pthread_t                   survey_thread;  // the survey thread
    pthread_mutex_t             survey_mutex;   // this protects "survey_zeros" during the survey
    uintmax_t                   survey_count;
};

// s3backer_store functions
static int zero_cache_create_threads(struct s3backer_store *s3b);
static int zero_cache_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static int zero_cache_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value);
static int zero_cache_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict);
static int zero_cache_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *etag,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int zero_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int zero_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);
static int zero_cache_flush_blocks(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout);
static int zero_cache_bulk_zero(struct s3backer_store *const s3b, const s3b_block_t *block_nums, u_int num_blocks);
static int zero_cache_survey_non_zero(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static int zero_cache_shutdown(struct s3backer_store *s3b);
static void zero_cache_destroy(struct s3backer_store *s3b);

// Internal fuctions
static void zero_cache_update_block(struct zero_cache_private *const priv, s3b_block_t block_num, int zero);
static void *zero_cache_survey_main(void *arg);
static block_list_func_t zero_cache_survey_callback;

// Special all-zeros MD5 value signifying a zeroed block
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

    // Initialize structures
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
    if (inner->read_block_part != NULL)
        s3b->read_block_part = zero_cache_read_block_part;
    if (inner->write_block_part != NULL)
        s3b->write_block_part = zero_cache_write_block_part;
    s3b->flush_blocks = zero_cache_flush_blocks;
    s3b->bulk_zero = zero_cache_bulk_zero;
    s3b->survey_non_zero = zero_cache_survey_non_zero;
    s3b->shutdown = zero_cache_shutdown;
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
    if ((r = pthread_mutex_init(&priv->survey_mutex, NULL)) != 0)
        goto fail3;

    // Initialize bit map
    if ((priv->zeros = bitmap_init(config->num_blocks, 0)) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail4;
    }
    s3b->data = priv;

    // Done
    return s3b;

fail4:
    pthread_mutex_destroy(&priv->survey_mutex);
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
    struct zero_cache_conf *const config = priv->config;
    int r;

    // Propagate to lower layer
    if ((r = (*priv->inner->create_threads)(priv->inner)) != 0)
        return r;

    // Anything to do? (was "--listBlocks" specified)
    if (!config->list_blocks)
        return 0;
    (*config->log)(LOG_INFO, "starting non-zero block survey");

    // Lock mutex
    pthread_mutex_lock(&priv->mutex);

    // Initialize survey bitmap (to all 1's)
    assert(priv->survey_zeros == NULL);
    if ((priv->survey_zeros = bitmap_init(config->num_blocks, 1)) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "malloc(): %s", strerror(r));
        goto fail1;
    }

    // Create survey thread
    if ((r = pthread_create(&priv->survey_thread, NULL, zero_cache_survey_main, priv)) != 0) {
        (*config->log)(LOG_ERR, "pthread_create(): %s", strerror(r));
        goto fail2;
    }
    priv->thread_started = 1;

    // Unlock mutex
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Done
    return 0;

fail2:
    bitmap_free(&priv->survey_zeros);
fail1:
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    return r;
}

static void *
zero_cache_survey_main(void *arg)
{
    struct zero_cache_private *const priv = arg;
    struct zero_cache_conf *const config = priv->config;
    uintmax_t survey_count;
    int r;

    // Perform survey
    r = (*priv->inner->survey_non_zero)(priv->inner, zero_cache_survey_callback, priv);

    // Lock main mutex
    pthread_mutex_lock(&priv->mutex);

    // Apply results (only if we completed the survey with no error)
    pthread_mutex_lock(&priv->survey_mutex);
    if (r == 0)
        priv->stats.current_cache_size = bitmap_or2(priv->zeros, priv->survey_zeros, config->num_blocks);
    survey_count = priv->survey_count;
    CHECK_RETURN(pthread_mutex_unlock(&priv->survey_mutex));

    // Finish up
    bitmap_free(&priv->survey_zeros);

    // Unlock main mutex
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Done
    (*config->log)(LOG_INFO, "non-zero block survey %s (%ju non-zero blocks reported)",
      r == 0 ? "completed" : r == ECANCELED ? "canceled" : "failed", survey_count);
    return NULL;
}

static int
zero_cache_survey_callback(void *arg, const s3b_block_t *block_nums, u_int num_blocks)
{
    struct zero_cache_private *const priv = arg;

    // Check for shutdown
    if (priv->stopping)
        return ECANCELED;

    // Reset bits corresponding to non-zero blocks
    pthread_mutex_lock(&priv->survey_mutex);
    assert(priv->survey_zeros != NULL);
    while (num_blocks-- > 0) {
        const s3b_block_t block_num = *block_nums++;

        if (!bitmap_test(priv->survey_zeros, block_num))
            continue;                                           // already reported to us
        bitmap_set(priv->survey_zeros, block_num, 0);
        priv->survey_count++;
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->survey_mutex));

    // Done
    return 0;
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
zero_cache_flush_blocks(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout)
{
    struct zero_cache_private *const priv = s3b->data;

    return (*priv->inner->flush_blocks)(priv->inner, block_nums, num_blocks, timeout);
}

static int
zero_cache_shutdown(struct s3backer_store *const s3b)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;
    int r;

    // Stop the survey, if any
    pthread_mutex_lock(&priv->mutex);
    if (priv->thread_started) {
        priv->stopping = 1;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        if ((r = pthread_join(priv->survey_thread, NULL)) != 0)
            (*config->log)(LOG_ERR, "pthread_join: %s", strerror(r));
        pthread_mutex_lock(&priv->mutex);
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Propagate to lower layer
    return (*priv->inner->shutdown)(priv->inner);
}

static void
zero_cache_destroy(struct s3backer_store *const s3b)
{
    struct zero_cache_private *const priv = s3b->data;

    // Grab lock
    pthread_mutex_lock(&priv->mutex);

    // Destroy inner store
    (*priv->inner->destroy)(priv->inner);

    // Free structures
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    pthread_mutex_destroy(&priv->mutex);
    assert(priv->survey_zeros == NULL);
    bitmap_free(&priv->zeros);
    free(priv);
    free(s3b);
}

static int
zero_cache_survey_non_zero(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    return ENOTSUP;
}

static int
zero_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;
    int r;

    // Check for the case where we are reading a block that is already known to be zero
    pthread_mutex_lock(&priv->mutex);
    if (bitmap_test(priv->zeros, block_num)) {
        priv->stats.read_hits++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        if (expect_etag != NULL && strict && memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) != 0)
            return EIO;
        if (actual_etag != NULL)
            memcpy(actual_etag, zero_etag, MD5_DIGEST_LENGTH);
        memset(dest, 0, config->block_size);
        return 0;
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Perform the actual read
    r = (*priv->inner->read_block)(priv->inner, block_num, dest, actual_etag, expect_etag, strict);

    // Update cache - if read was successful (or EEXIST case)
    if (r == 0 || (expect_etag != NULL && !strict && r == EEXIST && memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) == 0)) {
        const int zero = block_is_zeros(dest);
        pthread_mutex_lock(&priv->mutex);
        zero_cache_update_block(priv, block_num, zero);
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    }

    // Done
    return r;
}

static int
zero_cache_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *caller_etag,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct zero_cache_private *const priv = s3b->data;
    int known_zero;
    int r;

    // Detect zero blocks
    if (src != NULL && block_is_zeros(src))
        src = NULL;
    known_zero = src == NULL;

    // Handle the case where we know this block is zero
    pthread_mutex_lock(&priv->mutex);
    if (bitmap_test(priv->zeros, block_num)) {
        if (known_zero) {                                       // ok, it's still zero -> return immediately
            priv->stats.write_hits++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            if (caller_etag != NULL)
                memcpy(caller_etag, zero_etag, MD5_DIGEST_LENGTH);
            return 0;
        }
        zero_cache_update_block(priv, block_num, 0);            // be conservative and say we are no longer sure
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Perform the actual write
    if ((r = (*priv->inner->write_block)(priv->inner, block_num, src, caller_etag, check_cancel, check_cancel_arg)) != 0)
        known_zero = 0;                                         // be conservative and say we are no longer sure

    // Update cache
    pthread_mutex_lock(&priv->mutex);
    zero_cache_update_block(priv, block_num, known_zero);
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Done
    return r;
}

static int
zero_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;

    // Sanity check
    (void)config;
    assert(len > 0);
    assert(len < config->block_size);
    assert(off + len <= config->block_size);

    // Check for the case where we are reading a block that is already known to be zero
    pthread_mutex_lock(&priv->mutex);
    if (bitmap_test(priv->zeros, block_num)) {
        priv->stats.read_hits++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        memset(dest, 0, len);
        return 0;
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Perform the partial read
    return (*priv->inner->read_block_part)(priv->inner, block_num, off, len, dest);
}

static int
zero_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;
    int data_is_zeros;

    // Sanity check
    (void)config;
    assert(len > 0);
    assert(len < config->block_size);
    assert(off + len <= config->block_size);

    // Check whether data is all zeros
    data_is_zeros = src == NULL || memcmp(src, zero_block, len) == 0;

    // Handle the case where we know this block is zero
    pthread_mutex_lock(&priv->mutex);
    if (bitmap_test(priv->zeros, block_num)) {
        if (data_is_zeros) {                                    // ok, it's still zero -> return immediately
            priv->stats.write_hits++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            return 0;
        }
        zero_cache_update_block(priv, block_num, 0);            // be conservative and say we are no longer sure
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Perform the partial write
    return (*priv->inner->write_block_part)(priv->inner, block_num, off, len, src);
}

static int
zero_cache_bulk_zero(struct s3backer_store *const s3b, const s3b_block_t *block_nums, u_int num_blocks)
{
    struct zero_cache_private *const priv = s3b->data;
    struct zero_cache_conf *const config = priv->config;
    s3b_block_t *edited_block_nums;
    u_int edited_num_blocks = 0;
    int i;
    int r;

    // Create array we can modify
    if ((edited_block_nums = malloc(sizeof(*block_nums) * num_blocks)) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "malloc(): %s", strerror(r));
        return r;
    }

    // Filter out blocks we know are already zero
    pthread_mutex_lock(&priv->mutex);
    while (num_blocks-- > 0) {
        const s3b_block_t block_num = *block_nums++;

        if (bitmap_test(priv->zeros, block_num))
            priv->stats.write_hits++;
        else
            edited_block_nums[edited_num_blocks++] = block_num;
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Any blocks left?
    if (edited_num_blocks == 0) {
        free(edited_block_nums);
        return 0;
    }

    // Perform the bulk zero on the edited array
    if ((r = (*priv->inner->bulk_zero)(priv->inner, edited_block_nums, edited_num_blocks)) != 0)
        goto fail;

    // Update cache to mark all those blocks zero now
    pthread_mutex_lock(&priv->mutex);
    for (i = 0; i < edited_num_blocks; i++)
        zero_cache_update_block(priv, edited_block_nums[i], 1);
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

fail:
    // Done
    free(edited_block_nums);
    return r;
}

void
zero_cache_get_stats(struct s3backer_store *s3b, struct zero_cache_stats *stats)
{
    struct zero_cache_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

void
zero_cache_clear_stats(struct s3backer_store *s3b)
{
    struct zero_cache_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    priv->stats.read_hits = 0;
    priv->stats.write_hits = 0;
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

// This assumes mutex is held
static void
zero_cache_update_block(struct zero_cache_private *const priv, s3b_block_t block_num, int zero)
{
    // If non-zero, ensure any ongoing survey doesn't overwrite this change when it completes
    if (priv->survey_zeros != NULL && !zero)
        bitmap_set(priv->survey_zeros, block_num, 0);

    // Update bitmap
    if (bitmap_test(priv->zeros, block_num) == zero)
        return;
    bitmap_set(priv->zeros, block_num, zero);
    if (zero)
        priv->stats.current_cache_size++;
    else
        priv->stats.current_cache_size--;
}
