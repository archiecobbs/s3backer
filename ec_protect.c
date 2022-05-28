
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
#include "ec_protect.h"
#include "hash.h"
#include "util.h"

/*
 * Written block information caching.
 *
 * The purpose of this is to minimize problems from the weak guarantees provided
 * by S3's "eventual consistency". We do this by:
 *
 *  (a) Enforcing a minimum delay between the completion of one PUT/DELETE
 *      of a block and the initiation of the next PUT/DELETE of the same block.
 *      This is to allow the PUT/DELETE to be propagated across the S3 network.
 *  (b) Caching the ETag (which is usually just the MD5 checksum) of every block
 *      written for some minimum time and verifying that data returned from subsequent
 *      GETs matches, allowing us to verify we are not reading back stale data.
 *
 * The theory here is that if you choose a value of X seconds large enough, and you
 * assume all S3 reads will be up-to-date X seconds after they've been written, then
 * using X seconds with (a) and (b) above guarantees consistent reads. However, this
 * guarantee is only as good as that assumption about maximum convergence time X.
 *
 * These are the relevant configuration parameters:
 *
 *  min_write_delay
 *      Minimum time delay after a PUT/DELETE completes before the next PUT/DELETE
 *      can be initiated (for the same block).
 *  cache_time
 *      How long after writing a block we'll remember its ETag. This must be
 *      at least as long as min_write_delay. This value determines the limit of
 *      our ability to detect out-of-date reads. Zero means infinity.
 *  cache_size
 *      Maximum number of blocks we'll track at one time. When the table is full
 *      and no WRITTEN blocks have exipred yet, additional writes will block.
 *      Note if cache_time is zero (infinity), then this must be big enough to
 *      contain ALL of the blocks, otherwise you will eventually deadlock.
 *
 * Blocks we are currently tracking can be in the following states:
 *
 * State    Meaning                  Hash table  List  Other invariants
 * -----    -------                  ----------  ----  ----------------
 *
 * CLEAN    initial state            No          No
 * WRITING  currently being written  Yes         No    timestamp == 0, u.data valid
 * WRITTEN  written and ETag cached  Yes         Yes   timestamp != 0, u.etag valid
 *
 * The steady state for a block is CLEAN. WRITING means the block is currently
 * being sent; concurrent attempts to write will simply sleep until the first one
 * finishes. WRITTEN is where you go after successfully writing a block. The WRITTEN
 * state will timeout (and the entry revert to CLEAN) after cache_time.
 *
 * If another attempt to write a block in the WRITTEN state occurs occurs before
 * min_write_delay has elapsed, the second attempt will sleep until it has.
 *
 * In the WRITING state, we have the data still so any reads are local. In the WRITTEN
 * state we don't have the data but we do know its ETag, so therefore we can verify what
 * comes back from the read; if it doesn't verify, we retry as we would with any other error.
 *
 * There is a special case that occurs when we get an error while WRITING: in this case,
 * we don't know whether the block was successfully written or not, so we transition to
 * WRITTEN but with an all zeros ETag indicating "don't know".
 *
 * If we hit the 'cache_size' limit, we sleep a little while and then try again.
 *
 * We keep track of blocks in 'struct block_info' structures. These structures
 * are themselves tracked in both (a) a linked list and (b) a hash table.
 *
 * The hash table contains all structures, and is keyed by block number. This
 * is simply so we can quickly find the structure associated with a specific block.
 *
 * The linked list contains WRITTEN blocks, and is sorted in increasing order by timestamp,
 * so the entries that will expire first are at the front of the list.
 */
struct block_info {
    s3b_block_t             block_num;          // block number - MUST BE FIRST
    uint64_t                timestamp;          // time PUT/DELETE completed (if WRITTEN)
    TAILQ_ENTRY(block_info) link;               // list entry link
    union {
        const void      *data;                  // block's actual content (if WRITING)
        u_char          etag[MD5_DIGEST_LENGTH];// block's ETag (if WRITTEN)
    } u;
};

// Internal state
struct ec_protect_private {
    struct ec_protect_conf      *config;
    struct s3backer_store       *inner;
    struct ec_protect_stats     stats;
    struct s3b_hash             *hashtable;
    u_int                       num_sleepers;   // count of sleeping threads
    TAILQ_HEAD(, block_info)    list;
    block_list_func_t           *survey_callback;// non-zero survey is running and this is the callback
    void                        *survey_arg;    // non-zero survey is running and this is the arg
    pthread_mutex_t             mutex;
    pthread_cond_t              space_cond;     // signaled when cache space available
    pthread_cond_t              sleepers_cond;  // signaled when no more threads are sleeping
    pthread_cond_t              never_cond;     // never signaled; used for sleeping only
};

// s3backer_store functions
static int ec_protect_create_threads(struct s3backer_store *s3b);
static int ec_protect_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static int ec_protect_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value);
static int ec_protect_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict);
static int ec_protect_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *etag,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int ec_protect_flush_blocks(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout);
static int ec_protect_shutdown(struct s3backer_store *s3b);
static void ec_protect_destroy(struct s3backer_store *s3b);

// Misc
static uint64_t ec_protect_sleep_until(struct ec_protect_private *priv, pthread_cond_t *cond, uint64_t wake_time_millis);
static void ec_protect_scrub_expired_writtens(struct ec_protect_private *priv, uint64_t current_time);
static uint64_t ec_protect_get_time(void);
static int ec_protect_survey_non_zero(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static s3b_hash_visit_t ec_protect_append_block_list;
static s3b_hash_visit_t ec_protect_free_one;

// Invariants checking
#ifndef NDEBUG
static s3b_hash_visit_t ec_protect_check_one;
static void ec_protect_check_invariants(struct ec_protect_private *priv);

#define EC_PROTECT_CHECK_INVARIANTS(priv)     ec_protect_check_invariants(priv)
#else
#define EC_PROTECT_CHECK_INVARIANTS(priv)     do { } while (0)
#endif

// Special all-zeros MD5 value signifying a zeroed block
static const u_char zero_etag[MD5_DIGEST_LENGTH];

// Special all-ones MD5 value signifying a just-written block whose content is unknown
static u_char unknown_etag[MD5_DIGEST_LENGTH];

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
ec_protect_create(struct ec_protect_conf *config, struct s3backer_store *inner)
{
    struct s3backer_store *s3b;
    struct ec_protect_private *priv;
    int r;

    // Initialize structures
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail0;
    }
    s3b->create_threads = ec_protect_create_threads;
    s3b->meta_data = ec_protect_meta_data;
    s3b->set_mount_token = ec_protect_set_mount_token;
    s3b->read_block = ec_protect_read_block;
    s3b->write_block = ec_protect_write_block;
    s3b->bulk_zero = generic_bulk_zero;
    s3b->flush_blocks = ec_protect_flush_blocks;
    s3b->survey_non_zero = ec_protect_survey_non_zero;
    s3b->shutdown = ec_protect_shutdown;
    s3b->destroy = ec_protect_destroy;
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail1;
    }
    priv->config = config;
    priv->inner = inner;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    if ((r = pthread_cond_init(&priv->space_cond, NULL)) != 0)
        goto fail3;
    if ((r = pthread_cond_init(&priv->sleepers_cond, NULL)) != 0)
        goto fail4;
    if ((r = pthread_cond_init(&priv->never_cond, NULL)) != 0)
        goto fail5;
    TAILQ_INIT(&priv->list);
    if ((r = s3b_hash_create(&priv->hashtable, config->cache_size)) != 0)
        goto fail6;
    s3b->data = priv;
    memset(unknown_etag, 0xff, sizeof(unknown_etag));

    // Done
    EC_PROTECT_CHECK_INVARIANTS(priv);
    return s3b;

fail6:
    pthread_cond_destroy(&priv->never_cond);
fail5:
    pthread_cond_destroy(&priv->sleepers_cond);
fail4:
    pthread_cond_destroy(&priv->space_cond);
fail3:
    pthread_mutex_destroy(&priv->mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "ec_protect creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

static int
ec_protect_create_threads(struct s3backer_store *s3b)
{
    struct ec_protect_private *const priv = s3b->data;

    return (*priv->inner->create_threads)(priv->inner);
}

static int
ec_protect_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    struct ec_protect_private *const priv = s3b->data;

    return (*priv->inner->meta_data)(priv->inner, file_sizep, block_sizep);
}

static int
ec_protect_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value)
{
    struct ec_protect_private *const priv = s3b->data;

    return (*priv->inner->set_mount_token)(priv->inner, old_valuep, new_value);
}

static int
ec_protect_flush_blocks(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout)
{
    struct ec_protect_private *const priv = s3b->data;

    return (*priv->inner->flush_blocks)(priv->inner, block_nums, num_blocks, timeout);
}

static int
ec_protect_shutdown(struct s3backer_store *const s3b)
{
    struct ec_protect_private *const priv = s3b->data;

    // Grab lock and sanity check
    pthread_mutex_lock(&priv->mutex);
    EC_PROTECT_CHECK_INVARIANTS(priv);

    // Wait for all sleeping writers to finish
    while (priv->num_sleepers > 0)
        pthread_cond_wait(&priv->sleepers_cond, &priv->mutex);

    // Release lock
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Propagate to lower layer
    return (*priv->inner->shutdown)(priv->inner);
}

static void
ec_protect_destroy(struct s3backer_store *const s3b)
{
    struct ec_protect_private *const priv = s3b->data;

    // Grab lock and sanity check
    pthread_mutex_lock(&priv->mutex);
    EC_PROTECT_CHECK_INVARIANTS(priv);
    assert(priv->num_sleepers == 0);

    // Destroy inner store
    (*priv->inner->destroy)(priv->inner);

    // Free structures
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    pthread_mutex_destroy(&priv->mutex);
    pthread_cond_destroy(&priv->space_cond);
    pthread_cond_destroy(&priv->sleepers_cond);
    pthread_cond_destroy(&priv->never_cond);
    s3b_hash_foreach(priv->hashtable, ec_protect_free_one, NULL);
    s3b_hash_destroy(priv->hashtable);
    free(priv);
    free(s3b);
}

void
ec_protect_get_stats(struct s3backer_store *s3b, struct ec_protect_stats *stats)
{
    struct ec_protect_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    stats->current_cache_size = s3b_hash_size(priv->hashtable);
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

void
ec_protect_clear_stats(struct s3backer_store *s3b)
{
    struct ec_protect_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memset(&priv->stats, 0, sizeof(priv->stats));
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

static int
ec_protect_survey_non_zero(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    struct ec_protect_private *const priv = s3b->data;
    struct block_list list;
    int r;

    // Lock mutex
    pthread_mutex_lock(&priv->mutex);
    assert(priv->survey_callback == NULL);

    // Record survey in progress
    priv->survey_callback = callback;
    priv->survey_arg = arg;

    // Inventory all blocks currently in the cache; we don't bother trying to discern the zero blocks
    block_list_init(&list);
    if ((r = s3b_hash_foreach(priv->hashtable, ec_protect_append_block_list, &list)) != 0)
        goto done;

    // Unlock mutex
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Report all blocks inventoried above
    (*callback)(arg, list.blocks, list.num_blocks);
    block_list_free(&list);

    // Invoke lower layer
    r = (*priv->inner->survey_non_zero)(priv->inner, callback, arg);

    // Lock mutex
    pthread_mutex_lock(&priv->mutex);

    // Finish up
    assert(priv->survey_callback != NULL);
    priv->survey_callback = NULL;
    priv->survey_arg = NULL;

done:
    // Done
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    return r;
}

static int
ec_protect_append_block_list(void *arg, void *value)
{
    struct block_info *const entry = value;
    struct block_list *const list = arg;

    return block_list_append(list, entry->block_num);
}

static int
ec_protect_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict)
{
    struct ec_protect_private *const priv = s3b->data;
    struct ec_protect_conf *const config = priv->config;
    u_char etag[MD5_DIGEST_LENGTH];
    struct block_info *binfo;

    // Sanity check
    if (config->block_size == 0)
        return EINVAL;

    // Grab lock and sanity check
    pthread_mutex_lock(&priv->mutex);
    EC_PROTECT_CHECK_INVARIANTS(priv);

again:
    // Scrub the list of WRITTENs
    ec_protect_scrub_expired_writtens(priv, ec_protect_get_time());

    // Find info for this block
    if ((binfo = s3b_hash_get(priv->hashtable, block_num)) != NULL) {

        // In WRITING state: we have the data already!
        if (binfo->timestamp == 0) {
            if (binfo->u.data == NULL)
                memset(dest, 0, config->block_size);
            else
                memcpy(dest, binfo->u.data, config->block_size);
            if (actual_etag != NULL)
                memset(actual_etag, 0, MD5_DIGEST_LENGTH);          // we don't know it yet!
            priv->stats.cache_data_hits++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            return 0;
        }

        // In WRITTEN state: special case: unknown ETag. Wait for settle time, then try again
        if (memcmp(binfo->u.etag, unknown_etag, MD5_DIGEST_LENGTH) == 0) {

            // Have we waited long enough already? If so, reset block and try again
            if (ec_protect_get_time() >= binfo->timestamp + config->min_write_delay) {
                TAILQ_REMOVE(&priv->list, binfo, link);
                s3b_hash_remove(priv->hashtable, binfo->block_num);
                free(binfo);
                goto again;
            }

            // Sleep to allow previous failed write to resolve, and then try again
            ec_protect_sleep_until(priv, NULL, binfo->timestamp + config->min_write_delay);
            goto again;
        }

        // In WRITTEN state: special case: zero block
        if (memcmp(binfo->u.etag, zero_etag, MD5_DIGEST_LENGTH) == 0) {
            if (expect_etag != NULL && strict && memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) != 0)
                (*config->log)(LOG_ERR, "ec_protect_read_block(): impossible expected ETag?");
            memset(dest, 0, config->block_size);
            if (actual_etag != NULL)
                memset(actual_etag, 0, MD5_DIGEST_LENGTH);
            priv->stats.cache_data_hits++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            return 0;
        }

        // In WRITTEN state: we know the expected ETag
        memcpy(etag, binfo->u.etag, MD5_DIGEST_LENGTH);
        if (expect_etag != NULL && strict && memcmp(etag, expect_etag, MD5_DIGEST_LENGTH) != 0)
            (*config->log)(LOG_ERR, "ec_protect_read_block(): impossible expected ETag?");
        expect_etag = etag;
        strict = 1;
    }

    // Release lock
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Read block normally
    return (*priv->inner->read_block)(priv->inner, block_num, dest, actual_etag, expect_etag, strict);
}

static int
ec_protect_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *caller_etag,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct ec_protect_private *const priv = s3b->data;
    struct ec_protect_conf *const config = priv->config;
    u_char etag[MD5_DIGEST_LENGTH];
    struct block_info *binfo;
    uint64_t current_time;
    uint64_t delay;
    int r;

    // Sanity check
    if (config->block_size == 0)
        return EINVAL;

    // Grab lock
    pthread_mutex_lock(&priv->mutex);

    // Conservatively disqualify any non-zero block as being zero in any ongoing non-zero survey
    if (src != NULL && priv->survey_callback != NULL)
        (*priv->survey_callback)(priv->survey_arg, &block_num, 1);

again:
    // Sanity check
    EC_PROTECT_CHECK_INVARIANTS(priv);

    // Scrub the list of WRITTENs
    current_time = ec_protect_get_time();
    ec_protect_scrub_expired_writtens(priv, current_time);

    // Find info for this block
    binfo = s3b_hash_get(priv->hashtable, block_num);

    // CLEAN case: add new entry in state WRITING and write the block
    if (binfo == NULL) {

        // If we have reached max cache capacity, wait until there's more room
        if (s3b_hash_size(priv->hashtable) >= config->cache_size) {

            // Report deadlock situation
            if (config->cache_time == 0)
                (*config->log)(LOG_ERR, "md5 cache is full, but timeout is infinite: you have write deadlock!");

            // Sleep until space becomes available
            if ((binfo = TAILQ_FIRST(&priv->list)) != NULL && config->cache_time > 0)
                delay = ec_protect_sleep_until(priv, &priv->space_cond, binfo->timestamp + config->cache_time);
            else
                delay = ec_protect_sleep_until(priv, &priv->space_cond, 0);         // sleep indefinitely...
            priv->stats.cache_full_delay += delay;
            goto again;
        }

        // Create new entry in WRITING state
        if ((binfo = calloc(1, sizeof(*binfo))) == NULL) {
            r = errno;
            (*config->log)(LOG_ERR, "can't alloc new MD5 cache entry: %s", strerror(r));
            priv->stats.out_of_memory_errors++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            return r;
        }
        binfo->block_num = block_num;
        binfo->u.data = src;
        s3b_hash_put_new(priv->hashtable, binfo);

writeit:
        // Write the block
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        r = (*priv->inner->write_block)(priv->inner, block_num, src, etag, check_cancel, check_cancel_arg);
        pthread_mutex_lock(&priv->mutex);
        EC_PROTECT_CHECK_INVARIANTS(priv);

        /*
         * Wake up at least one thread that might be sleeping indefinitely (see above). This handles an obscure
         * case where the cache is full and every entry is in the WRITING state. The next thread that attempts
         * to write could be stuck waiting indefinitely unless we wake it up here.
         */
        pthread_cond_signal(&priv->space_cond);

        /*
         * Move to state WRITTEN.
         *
         * If there was an error, we can't assume we know whether the write succeeded or not,
         * so mark the block as WRITTEN but with a special ETag value meaning "unknown".
         * We have to wait for min_write_delay before trying to read the block again.
         */
        binfo->timestamp = ec_protect_get_time();
        memcpy(binfo->u.etag, r == 0 ? etag : unknown_etag, MD5_DIGEST_LENGTH);
        TAILQ_INSERT_TAIL(&priv->list, binfo, link);
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

        // Copy expected ETag for caller
        if (r == 0 && caller_etag != NULL)
            memcpy(caller_etag, etag, MD5_DIGEST_LENGTH);
        return r;
    }

    /*
     * WRITING case: wait until current write completes (hmm, why is kernel doing overlapping writes?).
     * Since we know after current write completes we'll have to wait another 'min_write_time' milliseconds
     * anyway, we conservatively just wait exactly that long now. There may be an extra wakeup or two,
     * but that's OK.
     */
    if (binfo->timestamp == 0) {
        delay = ec_protect_sleep_until(priv, NULL, current_time + config->min_write_delay);
        priv->stats.repeated_write_delay += delay;
        goto again;
    }

    /*
     * WRITTEN case: wait until at least 'min_write_time' milliseconds has passed since previous write.
     */
    if (current_time < binfo->timestamp + config->min_write_delay) {
        delay = ec_protect_sleep_until(priv, NULL, binfo->timestamp + config->min_write_delay);
        priv->stats.repeated_write_delay += delay;
        goto again;
    }

    /*
     * WRITTEN case: 'min_write_time' milliseconds have indeed passed, so go back to WRITING.
     */
    binfo->timestamp = 0;
    binfo->u.data = src;
    TAILQ_REMOVE(&priv->list, binfo, link);
    goto writeit;
}

/*
 * Return current time in milliseconds.
 */
static uint64_t
ec_protect_get_time(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

/*
 * Remove expired WRITTEN entries from the list.
 * This assumes the mutex is held.
 */
static void
ec_protect_scrub_expired_writtens(struct ec_protect_private *priv, uint64_t current_time)
{
    struct ec_protect_conf *const config = priv->config;
    struct block_info *binfo;
    int num_removed = 0;

    if (config->cache_time > 0) {
        while ((binfo = TAILQ_FIRST(&priv->list)) != NULL && current_time >= binfo->timestamp + config->cache_time) {
            TAILQ_REMOVE(&priv->list, binfo, link);
            s3b_hash_remove(priv->hashtable, binfo->block_num);
            free(binfo);
            num_removed++;
        }
    }
    switch (num_removed) {
    case 0:
        break;
    case 1:
        pthread_cond_signal(&priv->space_cond);
        break;
    default:
        pthread_cond_broadcast(&priv->space_cond);
        break;
    }
}

/*
 * Sleep until specified time (if non-zero) or condition (if non-NULL).
 * Note: in rare cases there can be spurious early wakeups.
 * Returns number of milliseconds slept.
 *
 * This assumes the mutex is locked.
 */
static uint64_t
ec_protect_sleep_until(struct ec_protect_private *priv, pthread_cond_t *cond, uint64_t wake_time_millis)
{
    uint64_t time_before;
    uint64_t time_after;

    assert(cond != NULL || wake_time_millis != 0);
    if (cond == NULL)
        cond = &priv->never_cond;
    time_before = ec_protect_get_time();
    priv->num_sleepers++;
    if (wake_time_millis != 0) {
        struct timespec wake_time;

        wake_time.tv_sec = wake_time_millis / 1000;
        wake_time.tv_nsec = (wake_time_millis % 1000) * 1000000;
        if (pthread_cond_timedwait(cond, &priv->mutex, &wake_time) == ETIMEDOUT)
            time_after = wake_time_millis;
        else
            time_after = ec_protect_get_time();
    } else {
        pthread_cond_wait(cond, &priv->mutex);
        time_after = ec_protect_get_time();
    }
    assert(priv->num_sleepers > 0);
    if (--priv->num_sleepers == 0)
        pthread_cond_broadcast(&priv->sleepers_cond);
    return time_after - time_before;
}

static int
ec_protect_free_one(void *arg, void *value)
{
    free(value);
    return 0;
}

#ifndef NDEBUG

// Accounting structure
struct check_info {
    u_int   num_in_list;
    u_int   written;
    u_int   writing;
};

static int
ec_protect_check_one(void *arg, void *value)
{
    struct block_info *const binfo = value;
    struct check_info *const info = arg;

    if (binfo->timestamp == 0)
        info->writing++;
    else
        info->written++;
    return 0;
}

static void
ec_protect_check_invariants(struct ec_protect_private *priv)
{
    struct block_info *binfo;
    struct check_info info;

    memset(&info, 0, sizeof(info));
    for (binfo = TAILQ_FIRST(&priv->list); binfo != NULL; binfo = TAILQ_NEXT(binfo, link)) {
        assert(binfo->timestamp != 0);
        assert(s3b_hash_get(priv->hashtable, binfo->block_num) == binfo);
        info.num_in_list++;
    }
    s3b_hash_foreach(priv->hashtable, ec_protect_check_one, &info);
    assert(info.written == info.num_in_list);
    assert(info.written + info.writing == s3b_hash_size(priv->hashtable));
}
#endif

