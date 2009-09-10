
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
#include "dcache.h"
#include "hash.h"

/*
 * This file implements a simple block cache that acts as a "layer" on top
 * of an underlying s3backer_store.
 *
 * Blocks in the cache are in one of these states:
 *
 *  CLEAN       Data is consistent with underlying s3backer_store
 *  CLEAN2      Data is belived consistent with underlying s3backer_store, but need to verify MD5
 *  DIRTY       Data is inconsistent with underlying s3backer_store (needs writing)
 *  READING     Data is being read from the underlying s3backer_store
 *  READING2    Data is being read/verified from the underlying s3backer_store
 *  WRITING     Data is being written to underlying s3backer_store
 *  WRITING2    Same as WRITING, but a subsequent write has stored new data
 *
 * Blocks in the CLEAN and CLEAN2 states are linked in a list in order from least recently
 * used to most recently used (where 'used' means either read or written). CLEAN2 is the
 * same as CLEAN except that the data must be MD5 verified before being used.
 *
 * Blocks in the DIRTY state are linked in a list in the order they should be written.
 * A pool of worker threads picks them off and writes them through to the underlying
 * s3backer_store; while being written they are in state WRITING, or WRITING2 if another
 * write to the same block happens during that time. If the write is unsuccessful, the
 * block goes back to DIRTY and to the head of the DIRTY list: the result is that failed
 * writes of DIRTY blocks will retry indefinitely. If the write is successful, the
 * block moves to CLEAN if still in state WRITING, or DIRTY if in WRITING2.
 *
 * Because we allow writes to update the data in a block while that block is being
 * written, the worker threads always write from the original buffer, and a new buffer
 * will get created on demand when a block moves to state WRITING2. When it completes
 * its write attempt, the worker thread then checks for this condition and, if indeed
 * the block has changed to WRITING2, it knows to free the original buffer.
 *
 * Blocks in the READING, WRITING and WRITING2 states are not in either list.
 *
 * Only CLEAN and CLEAN2 blocks are eligible to be evicted from the cache. We evict entries
 * either when they timeout or the cache is full and we need to add a new entry to it.
 */

/* Cache entry states */
#define CLEAN           0
#define CLEAN2          1
#define DIRTY           2
#define READING         3
#define READING2        4
#define WRITING         5
#define WRITING2        6

/*
 * One cache entry. In order to keep this structure as small as possible, we do
 * two size optimizations:
 *
 *  1. We use the low-order bit of '_data' as the dirty flag (we assume all valid
 *     pointers are aligned to an even address).
 *  2. When not linked into either list (i.e., in WRITING state), we set link.tqe_prev
 *     to NULL to indicate this; this is safe because link.tqe_prev is always non-NULL
 *     when the structure is linked into a list.
 *
 * Invariants:
 *
 *  State       ENTRY_IN_LIST()?    dirty?   timeout == -1  verify  dcache
 *  -----       ----------------    ------   -------------  ------  ------
 *
 *  CLEAN       YES: priv->cleans   NO       ?                0     recorded
 *  CLEAN2      YES: priv->cleans   NO       ?                1     recorded
 *  READING     NO                  NO       YES              0     allocated
 *  READING2    NO                  NO       YES              1     allocated
 *  DIRTY       YES: priv->dirties  YES      ?                ?     allocated
 *  WRITING     NO                  NO       NO               ?     allocated
 *  WRITING2    NO                  YES      NO               ?     allocated
 *
 * Timeouts: we track time in units of TIME_UNIT_MILLIS milliseconds from when we start.
 * This is so we can jam them into 30 bits instead of 64. It's possible for the time value
 * to wrap after about two years; the effect would be mis-timed writes and evictions.
 *
 * In state CLEAN2 only, the MD5 to verify immediately follows the structure.
 */
struct cache_entry {
    s3b_block_t                     block_num;      // block number - MUST BE FIRST
    u_int                           dirty:1;        // indicates state DIRTY or WRITING2
    u_int                           verify:1;       // data should be verified first
    uint32_t                        timeout:30;     // when to evict (CLEAN[2]) or write (DIRTY)
    TAILQ_ENTRY(cache_entry)        link;           // next in list (cleans or dirties)
    union {
        void                        *data;          // data buffer in memory
        s3b_block_t                 dslot;          // disk cache data slot
    }                               u;
    u_char                          md5[0];         // MD5 checksum (CLEAN2)
};
#define ENTRY_IN_LIST(entry)                ((entry)->link.tqe_prev != NULL)
#define ENTRY_RESET_LINK(entry)             do { (entry)->link.tqe_prev = NULL; } while (0)
#define ENTRY_GET_STATE(entry)              (ENTRY_IN_LIST(entry) ?                             \
                                                ((entry)->dirty ? DIRTY :                \
                                                  ((entry)->verify ? CLEAN2 : CLEAN)) :         \
                                                ((entry)->timeout == READING_TIMEOUT ?          \
                                                  ((entry)->verify ? READING2 : READING) :      \
                                                  (entry)->dirty ? WRITING2 : WRITING))

/* One time unit in milliseconds */
#define TIME_UNIT_MILLIS            64

/* The dirty ratio at which we want to be writing out dirty blocks immediately */
#define DIRTY_RATIO_WRITE_ASAP      0.90            // 90%

/* Special timeout value for entries in state READING and READING2 */
#define READING_TIMEOUT             ((uint32_t)0x3fffffff)

/* Private data */
struct block_cache_private {
    struct block_cache_conf         *config;        // configuration
    struct s3backer_store           *inner;         // underlying s3backer store
    struct block_cache_stats        stats;          // statistics
    TAILQ_HEAD(, cache_entry)       cleans;         // list of clean blocks (LRU order)
    TAILQ_HEAD(, cache_entry)       dirties;        // list of dirty blocks (write order)
    struct s3b_hash                 *hashtable;     // hashtable of all cached blocks
    struct s3b_dcache               *dcache;        // on-disk persistent cache
    u_int                           num_cleans;     // length of the 'cleans' list
    u_int64_t                       start_time;     // when we started
    u_int32_t                       clean_timeout;  // timeout for clean entries in time units
    u_int32_t                       dirty_timeout;  // timeout for dirty entries in time units
    s3b_block_t                     seq_last;       // last block read in sequence by upper layer
    u_int                           seq_count;      // # of blocks read in sequence by upper layer
    u_int                           ra_count;       // # of blocks of read-ahead initiated
    u_int                           thread_id;      // next thread id
    u_int                           num_threads;    // number of alive worker threads
    int                             stopping;       // signals worker threads to exit
    pthread_mutex_t                 mutex;          // my mutex
    pthread_cond_t                  space_avail;    // there is new space available in cache
    pthread_cond_t                  end_reading;    // some entry in state READING[2] changed state
    pthread_cond_t                  worker_work;    // there is new work for worker thread(s)
    pthread_cond_t                  worker_exit;    // a worker thread has exited
    pthread_cond_t                  write_complete; // a write has completed
};

/* Callback info */
struct cbinfo {
    block_list_func_t           *callback;
    void                        *arg;
};

/* s3backer_store functions */
static int block_cache_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict);
static int block_cache_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *md5);
static int block_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int block_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);
static int block_cache_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static void block_cache_destroy(struct s3backer_store *s3b);

/* Other functions */
static s3b_dcache_visit_t block_cache_dcache_load;
static int block_cache_read(struct block_cache_private *priv, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int block_cache_do_read(struct block_cache_private *priv, s3b_block_t block_num, u_int off, u_int len, void *dest, int stats);
static int block_cache_write(struct block_cache_private *priv, s3b_block_t block_num, u_int off, u_int len, const void *src);
static void *block_cache_worker_main(void *arg);
static int block_cache_get_entry(struct block_cache_private *priv, struct cache_entry **entryp, void **datap);
static void block_cache_free_one(void *arg, void *value);
static struct cache_entry *block_cache_verified(struct block_cache_private *priv, struct cache_entry *entry);
static void block_cache_dirty_callback(void *arg, void *value);
static double block_cache_dirty_ratio(struct block_cache_private *priv);
static void block_cache_worker_wait(struct block_cache_private *priv, struct cache_entry *entry);
static uint32_t block_cache_get_time(struct block_cache_private *priv);
static uint64_t block_cache_get_time_millis(void);
static int block_cache_read_data(struct block_cache_private *priv, struct cache_entry *entry, void *dest, u_int off, u_int len);
static int block_cache_write_data(struct block_cache_private *priv, struct cache_entry *entry, const void *src, u_int off,
  u_int len);

/* Invariants checking */
#ifndef NDEBUG
static void block_cache_check_invariants(struct block_cache_private *priv);
static void block_cache_check_one(void *arg, void *value);
#define S3BCACHE_CHECK_INVARIANTS(priv)     block_cache_check_invariants(priv)
#else
#define S3BCACHE_CHECK_INVARIANTS(priv)     do { } while (0)
#endif

/*
 * Wrap an underlying s3backer store with a block cache. Invoking the
 * destroy method will destroy both this and the inner s3backer store.
 *
 * Returns NULL and sets errno on failure.
 */
struct s3backer_store *
block_cache_create(struct block_cache_conf *config, struct s3backer_store *inner)
{
    struct s3backer_store *s3b;
    struct block_cache_private *priv;
    struct cache_entry *entry;
    pthread_t thread;
    int r;

    /* Initialize s3backer_store structure */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }
    s3b->read_block = block_cache_read_block;
    s3b->write_block = block_cache_write_block;
    s3b->read_block_part = block_cache_read_block_part;
    s3b->write_block_part = block_cache_write_block_part;
    s3b->list_blocks = block_cache_list_blocks;
    s3b->destroy = block_cache_destroy;

    /* Initialize block_cache_private structure */
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        goto fail1;
    }
    priv->config = config;
    priv->inner = inner;
    priv->start_time = block_cache_get_time_millis();
    priv->clean_timeout = (config->timeout + TIME_UNIT_MILLIS - 1) / TIME_UNIT_MILLIS;
    priv->dirty_timeout = (config->write_delay + TIME_UNIT_MILLIS - 1) / TIME_UNIT_MILLIS;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    if ((r = pthread_cond_init(&priv->space_avail, NULL)) != 0)
        goto fail3;
    if ((r = pthread_cond_init(&priv->end_reading, NULL)) != 0)
        goto fail4;
    if ((r = pthread_cond_init(&priv->worker_work, NULL)) != 0)
        goto fail5;
    if ((r = pthread_cond_init(&priv->worker_exit, NULL)) != 0)
        goto fail6;
    if ((r = pthread_cond_init(&priv->write_complete, NULL)) != 0)
        goto fail7;
    TAILQ_INIT(&priv->cleans);
    TAILQ_INIT(&priv->dirties);
    if ((r = s3b_hash_create(&priv->hashtable, config->cache_size)) != 0)
        goto fail8;
    s3b->data = priv;

    /* Initialize on-disk cache and read in directory */
    if (config->cache_file != NULL) {
        if ((r = s3b_dcache_open(&priv->dcache, config->log, config->cache_file, config->block_size,
          config->cache_size, block_cache_dcache_load, priv)) != 0)
            goto fail9;
    }

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Create threads */
    for (priv->num_threads = 0; priv->num_threads < config->num_threads; priv->num_threads++) {
        if ((r = pthread_create(&thread, NULL, block_cache_worker_main, priv)) != 0)
            goto fail10;
    }

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    return s3b;

fail10:
    priv->stopping = 1;
    while (priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->mutex);
    }
    if (config->cache_file != NULL) {
        while ((entry = TAILQ_FIRST(&priv->cleans)) != NULL) {
            TAILQ_REMOVE(&priv->cleans, entry, link);
            free(entry);
        }
        s3b_dcache_close(priv->dcache);
    }
fail9:
    s3b_hash_destroy(priv->hashtable);
fail8:
    pthread_cond_destroy(&priv->write_complete);
fail7:
    pthread_cond_destroy(&priv->worker_exit);
fail6:
    pthread_cond_destroy(&priv->worker_work);
fail5:
    pthread_cond_destroy(&priv->end_reading);
fail4:
    pthread_cond_destroy(&priv->space_avail);
fail3:
    pthread_mutex_destroy(&priv->mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "block_cache creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

/*
 * Callback function to pre-load the cache from a pre-existing cache file.
 */
static int
block_cache_dcache_load(void *arg, s3b_block_t dslot, s3b_block_t block_num, const u_char *md5)
{
    struct block_cache_private *const priv = arg;
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;

    /* Sanity check */
    assert(config->cache_file != NULL);

    /* Sanity check a block is not listed twice */
    if ((entry = s3b_hash_get(priv->hashtable, block_num)) != NULL) {
        (*config->log)(LOG_ERR, "corrupted cache file: block 0x%0*jx listed twice (in dslots %ju and %ju)",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, (uintmax_t)entry->u.dslot, (uintmax_t)dslot);
        return EINVAL;
    }

    /* Create a new cache entry in state CLEAN[2] */
    assert(config->cache_file != NULL);
    if ((entry = calloc(1, sizeof(*entry) + (!config->no_verify ? MD5_DIGEST_LENGTH : 0))) == NULL) {
        priv->stats.out_of_memory_errors++;
        return errno;
    }
    entry->block_num = block_num;
    entry->verify = !config->no_verify;
    entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;
    if (entry->verify)
        memcpy(&entry->md5, md5, MD5_DIGEST_LENGTH);
    entry->u.dslot = dslot;
    TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
    priv->num_cleans++;
    s3b_hash_put_new(priv->hashtable, entry);
    assert(ENTRY_GET_STATE(entry) == (config->no_verify ? CLEAN : CLEAN2));
    return 0;
}

/*
 * Destructor
 */
static void
block_cache_destroy(struct s3backer_store *const s3b)
{
    struct block_cache_private *const priv = s3b->data;
    struct block_cache_conf *const config = priv->config;

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Wait for all dirty blocks to be written and all worker threads to exit */
    priv->stopping = 1;
    while (TAILQ_FIRST(&priv->dirties) != NULL || priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->mutex);
    }

    /* Destroy inner store */
    (*priv->inner->destroy)(priv->inner);

    /* Free structures */
    if (config->cache_file != NULL)
        s3b_dcache_close(priv->dcache);
    s3b_hash_foreach(priv->hashtable, block_cache_free_one, priv);
    s3b_hash_destroy(priv->hashtable);
    pthread_cond_destroy(&priv->write_complete);
    pthread_cond_destroy(&priv->worker_exit);
    pthread_cond_destroy(&priv->worker_work);
    pthread_cond_destroy(&priv->end_reading);
    pthread_cond_destroy(&priv->space_avail);
    pthread_mutex_destroy(&priv->mutex);
    free(priv);
    free(s3b);
}

void
block_cache_get_stats(struct s3backer_store *s3b, struct block_cache_stats *stats)
{
    struct block_cache_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    stats->current_size = s3b_hash_size(priv->hashtable);
    stats->dirty_ratio = block_cache_dirty_ratio(priv);
    pthread_mutex_unlock(&priv->mutex);
}

static int
block_cache_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    struct block_cache_private *const priv = s3b->data;
    struct cbinfo cbinfo;
    int r;

    if ((r = (*priv->inner->list_blocks)(priv->inner, callback, arg)) != 0)
        return r;
    cbinfo.callback = callback;
    cbinfo.arg = arg;
    pthread_mutex_lock(&priv->mutex);
    s3b_hash_foreach(priv->hashtable, block_cache_dirty_callback, &cbinfo);
    pthread_mutex_unlock(&priv->mutex);
    return 0;
}

static int
block_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict)
{
    struct block_cache_private *const priv = s3b->data;
    struct block_cache_conf *const config = priv->config;

    assert(expect_md5 == NULL);
    assert(actual_md5 == NULL);
    return block_cache_read(priv, block_num, 0, config->block_size, dest);
}

static int
block_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct block_cache_private *const priv = s3b->data;

    return block_cache_read(priv, block_num, off, len, dest);
}

/*
 * Read a block, and trigger read-ahead if necessary.
 */
static int
block_cache_read(struct block_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct block_cache_conf *const config = priv->config;
    int r = 0;

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Update count of block(s) read sequentially by the upper layer */
    if (block_num == priv->seq_last + 1) {
        priv->seq_count++;
        if (priv->ra_count > 0)
            priv->ra_count--;
    } else if (block_num != priv->seq_last) {
        priv->seq_count = 1;
        priv->ra_count = 0;
    }
    priv->seq_last = block_num;

    /* Wakeup a worker thread to read the next read-ahead block if needed */
    if (priv->seq_count >= config->read_ahead_trigger && priv->ra_count < config->read_ahead)
        pthread_cond_signal(&priv->worker_work);

    /* Peform the read */
    r = block_cache_do_read(priv, block_num, off, len, dest, 1);

    /* Release lock */
    pthread_mutex_unlock(&priv->mutex);
    return r;
}

/*
 * Read a block or a portion thereof.
 *
 * Assumes the mutex is held.
 */
static int
block_cache_do_read(struct block_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, void *dest, int stats)
{
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    u_char md5[MD5_DIGEST_LENGTH];
    int verified_but_not_read = 0;
    void *data = NULL;
    int r;

    /* Sanity check */
    assert(off <= priv->config->block_size);
    assert(len <= priv->config->block_size);
    assert(off + len <= priv->config->block_size);

again:
    /* Check to see if a cache entry already exists */
    if ((entry = s3b_hash_get(priv->hashtable, block_num)) != NULL) {
        assert(entry->block_num == block_num);
        switch (ENTRY_GET_STATE(entry)) {
        case READING:       /* Wait for other thread already reading this block to finish */
        case READING2:
            pthread_cond_wait(&priv->end_reading, &priv->mutex);
            goto again;
        case CLEAN2:        /* Go into READING2 state to read/verify the data */

            /* Allocate temporary buffer for reading the data if necessary */
            if (config->cache_file != NULL) {
                if ((data = malloc(config->block_size)) == NULL)
                    return errno;
            } else
                data = entry->u.data;

            /* Change from CLEAN2 to READING2 */
            if (config->cache_file != NULL) {
                if ((r = s3b_dcache_erase_block(priv->dcache, entry->u.dslot)) != 0)
                    (*config->log)(LOG_ERR, "can't erase cached block! %s", strerror(r));
                if ((r = s3b_dcache_fsync(priv->dcache)) != 0)
                    (*config->log)(LOG_ERR, "can't sync disk cache! %s", strerror(r));
            }
            TAILQ_REMOVE(&priv->cleans, entry, link);
            ENTRY_RESET_LINK(entry);
            priv->num_cleans--;
            entry->timeout = READING_TIMEOUT;
            assert(entry->verify);
            assert(ENTRY_GET_STATE(entry) == READING2);

            /* Now go read/verify the data */
            goto read;
        case CLEAN:         /* Update timestamp and move to the end of the list to maintain LRU ordering */
            TAILQ_REMOVE(&priv->cleans, entry, link);
            TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
            entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;
            // FALLTHROUGH
        case DIRTY:         /* Copy the cached data */
        case WRITING:
        case WRITING2:
            if ((r = block_cache_read_data(priv, entry, dest, off, len)) != 0)
                return r;
            break;
        default:
            assert(0);
            break;
        }
        if (stats)
            priv->stats.read_hits++;
        return 0;
    }

    /* Create a new cache entry in state READING */
    if ((r = block_cache_get_entry(priv, &entry, &data)) != 0)
        return r;
    if (entry == NULL) {                                            /* no free entries right now */
        pthread_cond_wait(&priv->space_avail, &priv->mutex);
        goto again;
    }
    entry->block_num = block_num;
    entry->dirty = 0;
    entry->verify = 0;
    entry->timeout = READING_TIMEOUT;
    ENTRY_RESET_LINK(entry);
    s3b_hash_put_new(priv->hashtable, entry);
    assert(ENTRY_GET_STATE(entry) == READING);

    /* Update stats */
    if (stats)
        priv->stats.read_misses++;

read:
    /* Read the block from the underlying s3backer_store */
    assert(ENTRY_GET_STATE(entry) == READING || ENTRY_GET_STATE(entry) == READING2);
    pthread_mutex_unlock(&priv->mutex);
    r = (*priv->inner->read_block)(priv->inner, block_num, data, md5, entry->verify ? entry->md5 : NULL, 0);
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* The entry should still exist and be in state READING[2] */
    assert(s3b_hash_get(priv->hashtable, block_num) == entry);
    assert(ENTRY_GET_STATE(entry) == READING || ENTRY_GET_STATE(entry) == READING2);
    assert(config->cache_file != NULL || entry->u.data == data);

    /*
     * We know two things at this point: the state is going to
     * change from READING[2] and we will create new available space
     * in the cache. Wake up any threads waiting on those events.
     */
    pthread_cond_broadcast(&priv->end_reading);
    pthread_cond_signal(&priv->space_avail);

    /* Check for unexpected error from underlying s3backer_store */
    if (r != 0 && !(entry->verify && r == EEXIST))
        goto fail;

    /* Handle READING2 blocks that were verified (revert to READING) */
    if (entry->verify) {
        if (r == EEXIST) {                  /* MD5 matched our expectation, download avoided */
            priv->stats.read_hits++;
            priv->stats.verified++;
            verified_but_not_read = 1;
            r = 0;
        } else {
            assert(r == 0);
            priv->stats.read_misses++;
            priv->stats.mismatch++;
        }
        entry = block_cache_verified(priv, entry);
        assert(ENTRY_GET_STATE(entry) == READING);
    }

    /* Copy the block data's into the destination buffer */
    if (!verified_but_not_read)
        memcpy(dest, (char *)data + off, len);

    /* Copy data into the disk cache and free temporary buffer (if necessary) */
    if (config->cache_file != NULL) {
        if (!verified_but_not_read) {
            if ((r = s3b_dcache_write_block(priv->dcache, entry->u.dslot, data, 0, config->block_size)) != 0)
                goto fail;
        }
        free(data);
    }

    /* Change entry from READING to CLEAN */
    assert(ENTRY_GET_STATE(entry) == READING);
    assert(!entry->verify);
    if (config->cache_file != NULL) {
        if ((r = s3b_dcache_fsync(priv->dcache)) != 0)
            (*config->log)(LOG_ERR, "can't sync disk cache! %s", strerror(r));
        if ((r = s3b_dcache_record_block(priv->dcache, entry->u.dslot, entry->block_num, md5)) != 0)
            (*config->log)(LOG_ERR, "can't record cached block! %s", strerror(r));
    }
    entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;
    TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
    priv->num_cleans++;
    assert(ENTRY_GET_STATE(entry) == CLEAN);

    /* If data was only verified, we have to actually go read it now */
    if (verified_but_not_read)
        goto again;

    /* Done */
    return 0;

fail:
    assert(r != 0);
    assert(ENTRY_GET_STATE(entry) == READING || ENTRY_GET_STATE(entry) == READING2);
    if (config->cache_file != NULL)
        s3b_dcache_free_block(priv->dcache, entry->u.dslot);
    s3b_hash_remove(priv->hashtable, entry->block_num);
    free(data);
    free(entry);
    return r;
}

static int
block_cache_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *md5)
{
    struct block_cache_private *const priv = s3b->data;
    struct block_cache_conf *const config = priv->config;

    assert(md5 == NULL);
    return block_cache_write(priv, block_num, 0, config->block_size, src);
}

static int
block_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct block_cache_private *const priv = s3b->data;

    return block_cache_write(priv, block_num, off, len, src);
}

/*
 * Write a block or a portion thereof.
 */
static int
block_cache_write(struct block_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    int r;

    /* Sanity check */
    assert(off <= config->block_size);
    assert(len <= config->block_size);
    assert(off + len <= config->block_size);

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);

again:
    /* Sanity check */
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Find cache entry */
    if ((entry = s3b_hash_get(priv->hashtable, block_num)) != NULL) {
        assert(entry->block_num == block_num);
        switch (ENTRY_GET_STATE(entry)) {
        case READING:               /* wait for entry to leave READING */
        case READING2:
            pthread_cond_wait(&priv->end_reading, &priv->mutex);
            goto again;
        case CLEAN2:                /* convert to CLEAN, then proceed */
            entry = block_cache_verified(priv, entry);
            // FALLTHROUGH
        case CLEAN:                /* update data, move to state DIRTY */
            if (config->cache_file != NULL) {
                if ((r = s3b_dcache_erase_block(priv->dcache, entry->u.dslot)) != 0)
                    (*config->log)(LOG_ERR, "can't erase cached block! %s", strerror(r));
                if ((r = s3b_dcache_fsync(priv->dcache)) != 0)
                    (*config->log)(LOG_ERR, "can't sync disk cache! %s", strerror(r));
            }
            TAILQ_REMOVE(&priv->cleans, entry, link);
            priv->num_cleans--;
            TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
            entry->timeout = block_cache_get_time(priv) + priv->dirty_timeout;
            pthread_cond_signal(&priv->worker_work);
            // FALLTHROUGH
        case WRITING2:              /* update data, stay in state WRITING2 */
        case WRITING:               /* update data, move to state WRITING2 */
        case DIRTY:                 /* update data, stay in state DIRTY */
            if ((r = block_cache_write_data(priv, entry, src, off, len)) != 0)
                (*config->log)(LOG_ERR, "error updating dirty block! %s", strerror(r));
            entry->dirty = 1;
            priv->stats.write_hits++;
            break;
        default:
            assert(0);
            break;
        }
        goto success;
    }

    /*
     * The block is not in the cache. If we're writing a partial block,
     * we have to read it into the cache first.
     */
    if (off != 0 || len != config->block_size) {
        if ((r = block_cache_do_read(priv, block_num, 0, 0, NULL, 0)) != 0)
            goto fail;
        goto again;
    }

    /* Get a cache entry, evicting a CLEAN[2] entry if necessary */
    if ((r = block_cache_get_entry(priv, &entry, NULL)) != 0)
        goto fail;

    /* If cache is full, wait for an entry to go CLEAN[2] so we can evict it */
    if (entry == NULL) {
        pthread_cond_wait(&priv->space_avail, &priv->mutex);
        goto again;
    }

    /* Record block data */
    if ((r = block_cache_write_data(priv, entry, src, off, len)) != 0)
        (*config->log)(LOG_ERR, "error updating dirty block! %s", strerror(r));

    /* Initialize a new DIRTY cache entry */
    priv->stats.write_misses++;
    entry->block_num = block_num;
    entry->timeout = block_cache_get_time(priv) + priv->dirty_timeout;
    entry->dirty = 1;
    assert(off == 0 && len == config->block_size);
    s3b_hash_put_new(priv->hashtable, entry);
    TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
    assert(ENTRY_GET_STATE(entry) == DIRTY);

    /* Wake up a worker thread to go write it */
    pthread_cond_signal(&priv->worker_work);

success:
    /* If doing synchronous writes, wait for write to complete */
    if (config->synchronous) {
        while (1) {
            int state;

            /* Wait for notification */
            pthread_cond_wait(&priv->write_complete, &priv->mutex);

            /* Sanity check */
            S3BCACHE_CHECK_INVARIANTS(priv);

            /* Find cache entry */
            if ((entry = s3b_hash_get(priv->hashtable, block_num)) == NULL)
                break;

            /* See if it is now clean */
            state = ENTRY_GET_STATE(entry);
            if (state == CLEAN || state == CLEAN2 || state == READING || state == READING2)
                break;

            /* Not written yet, go back to sleep */
            continue;
        }
    }
    r = 0;

fail:
    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    return r;
}

/*
 * Acquire a new cache entry. If the cache is full, and there is at least one
 * CLEAN[2] entry, evict and return it (uninitialized). Otherwise, return NULL entry.
 *
 * On successful return, *datap will point to a malloc'd buffer for the data. If using
 * the disk cache, this will be a temporary buffer, otherwise it's the in-memory buffer.
 * If datap == NULL, then in the case of the disk cache only, no buffer is allocated.
 *
 * This assumes the mutex is held.
 *
 * Returns non-zero on error.
 */
static int
block_cache_get_entry(struct block_cache_private *priv, struct cache_entry **entryp, void **datap)
{
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    void *data = NULL;
    int r;

    /*
     * If cache is not full, allocate a new entry. We allocate the structure
     * and the data separately in hopes that the malloc() implementation will
     * put the data into its own page of virtual memory.
     *
     * If the cache is full, try to evict a clean entry.
     */
    if (s3b_hash_size(priv->hashtable) < config->cache_size) {
        if ((entry = malloc(sizeof(*entry))) == NULL) {
            priv->stats.out_of_memory_errors++;
            return errno;
        }
    } else if ((entry = TAILQ_FIRST(&priv->cleans)) != NULL) {
        TAILQ_REMOVE(&priv->cleans, entry, link);
        s3b_hash_remove(priv->hashtable, entry->block_num);
        priv->num_cleans--;
    } else
        goto done;
    memset(entry, 0, sizeof(*entry));

    /* Get associated data buffer */
    if (datap != NULL || config->cache_file == NULL) {
        if ((data = malloc(config->block_size)) == NULL) {
            r = errno;
            priv->stats.out_of_memory_errors++;
            free(entry);
            return r;
        }
    }

    /* Get permanent data buffer */
    if (config->cache_file == NULL)
        entry->u.data = data;
    else if ((r = s3b_dcache_alloc_block(priv->dcache, &entry->u.dslot)) != 0) {
        free(data);             /* OK if NULL */
        free(entry);
        return r;
    }

done:
    /* Return what we got */
    *entryp = entry;
    if (datap != NULL)
        *datap = data;
    return 0;
}

/*
 * Worker thread main entry point.
 */
static void *
block_cache_worker_main(void *arg)
{
    struct block_cache_private *const priv = arg;
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    struct cache_entry *clean_entry = NULL;
    u_char md5[MD5_DIGEST_LENGTH];
    uint32_t adjusted_now;
    uint32_t now;
    u_int thread_id;
    void *buf;
    int r;

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);

    /* Assign myself a thread ID (for debugging purposes) */
    thread_id = priv->thread_id++;

    /*
     * Allocate buffer for outgoing block data. We have to copy it before we send it in case
     * another write to this block comes in and updates the data associated with the cache entry.
     */
    if ((buf = malloc(config->block_size)) == NULL) {
        (*config->log)(LOG_ERR, "block_cache worker %u can't alloc buffer, exiting: %s", thread_id, strerror(errno));
        goto done;
    }

    /* Repeatedly do stuff until told to stop */
    while (1) {

        /* Sanity check */
        S3BCACHE_CHECK_INVARIANTS(priv);

        /* Get current time */
        now = block_cache_get_time(priv);

        /* Evict any CLEAN[2] blocks that have timed out (if enabled) */
        if (priv->clean_timeout != 0) {
            while ((clean_entry = TAILQ_FIRST(&priv->cleans)) != NULL && now >= clean_entry->timeout) {

                /* Free the data */
                if (config->cache_file != NULL) {
                    if ((r = s3b_dcache_erase_block(priv->dcache, clean_entry->u.dslot)) != 0)
                        (*config->log)(LOG_ERR, "can't erase cached block! %s", strerror(r));
                    if ((r = s3b_dcache_fsync(priv->dcache)) != 0)
                        (*config->log)(LOG_ERR, "can't sync disk cache! %s", strerror(r));
                    if ((r = s3b_dcache_free_block(priv->dcache, clean_entry->u.dslot)) != 0)
                        (*config->log)(LOG_ERR, "can't free cached block! %s", strerror(r));
                } else
                    free(clean_entry->u.data);

                /* Free the entry */
                s3b_hash_remove(priv->hashtable, clean_entry->block_num);
                TAILQ_REMOVE(&priv->cleans, clean_entry, link);
                free(clean_entry);
                priv->num_cleans--;
                pthread_cond_signal(&priv->space_avail);
            }
        }

        /*
         * As the dirty ratio increases, force earlier than planned writes of those dirty entries
         * to relieve cache pressure. When the dirty ratio reaches DIRTY_RATIO_WRITE_ASAP, write
         * out all dirty blocks immediately.
         */
        adjusted_now = now + (uint32_t)((double)priv->dirty_timeout
          * block_cache_dirty_ratio(priv) * (1.0 / DIRTY_RATIO_WRITE_ASAP));

        /* See if there is a block that needs writing */
        if ((entry = TAILQ_FIRST(&priv->dirties)) != NULL && (priv->stopping || adjusted_now >= entry->timeout)) {

            /* If we are also supposed to do read-ahead, wake up a sibling to handle it */
            if (priv->seq_count >= config->read_ahead_trigger && priv->ra_count < config->read_ahead)
                pthread_cond_signal(&priv->worker_work);

            /* Copy data to our private buffer; it may change while we're writing */
            if ((r = block_cache_read_data(priv, entry, buf, 0, config->block_size)) != 0) {
                (*config->log)(LOG_ERR, "error reading cached block! %s", strerror(r));
                sleep(5);
                continue;
            }

            /* Move to WRITING state */
            assert(ENTRY_GET_STATE(entry) == DIRTY);
            TAILQ_REMOVE(&priv->dirties, entry, link);
            ENTRY_RESET_LINK(entry);
            entry->dirty = 0;
            entry->timeout = 0;
            assert(ENTRY_GET_STATE(entry) == WRITING);

            /* Attempt to write the block */
            pthread_mutex_unlock(&priv->mutex);
            r = (*priv->inner->write_block)(priv->inner, entry->block_num, buf, md5);
            pthread_mutex_lock(&priv->mutex);
            S3BCACHE_CHECK_INVARIANTS(priv);

            /* Sanity checks */
            assert(ENTRY_GET_STATE(entry) == WRITING || ENTRY_GET_STATE(entry) == WRITING2);

            /* If write attempt failed, go back to the DIRTY state and try again later */
            if (r != 0) {
                entry->dirty = 1;
                TAILQ_INSERT_HEAD(&priv->dirties, entry, link);
                continue;
            }

            /* If block was not modified while being written (WRITING), it is now CLEAN */
            if (!entry->dirty) {
                if (config->cache_file != NULL) {
                    if ((r = s3b_dcache_fsync(priv->dcache)) != 0)
                        (*config->log)(LOG_ERR, "can't sync disk cache! %s", strerror(r));
                    if ((r = s3b_dcache_record_block(priv->dcache, entry->u.dslot, entry->block_num, md5)) != 0)
                        (*config->log)(LOG_ERR, "can't record cached block! %s", strerror(r));
                }
                TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
                entry->verify = 0;
                entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;
                priv->num_cleans++;
                assert(ENTRY_GET_STATE(entry) == CLEAN);
                pthread_cond_signal(&priv->space_avail);
                if (config->synchronous)
                    pthread_cond_broadcast(&priv->write_complete);
                continue;
            }

            /* Block was modified while being written (WRITING2), so it stays DIRTY */
            TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
            entry->timeout = now + priv->dirty_timeout;     /* update for 2nd write timing conservatively */
            continue;
        }

        /* Are we supposed to stop? */
        if (priv->stopping != 0)
            break;

        /* See if there is a read-ahead block that needs to be read */
        if (priv->seq_count >= config->read_ahead_trigger && priv->ra_count < config->read_ahead) {
            while (priv->ra_count < config->read_ahead) {
                s3b_block_t ra_block;

                /* We will handle read-ahead for the next read-ahead block; claim it now */
                ra_block = priv->seq_last + ++priv->ra_count;

                /* If block already exists in the cache, nothing needs to be done */
                if (s3b_hash_get(priv->hashtable, ra_block) != NULL)
                    continue;

                /* Perform a speculative read of the block so it will get stored in the cache */
                (void)block_cache_do_read(priv, ra_block, 0, 0, NULL, 0);
                break;
            }
            continue;
        }

        /* There is nothing to do at this time; sleep until there is something to do */
        if (entry == NULL || (clean_entry != NULL && clean_entry->timeout < entry->timeout))
            entry = clean_entry;
        block_cache_worker_wait(priv, entry);
    }

    /* Decrement live worker thread count */
    priv->num_threads--;
    pthread_cond_signal(&priv->worker_exit);

done:
    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    free(buf);
    return NULL;
}

/*
 * Sleep until either the 'worker_work' condition becomes true, or the
 * entry (if any) times out.
 *
 * This assumes the mutex is held.
 */
static void
block_cache_worker_wait(struct block_cache_private *priv, struct cache_entry *entry)
{
    uint64_t wake_time_millis;
    struct timespec wake_time;

    if (entry == NULL) {
        pthread_cond_wait(&priv->worker_work, &priv->mutex);
        return;
    }
    wake_time_millis = priv->start_time + ((uint64_t)entry->timeout * TIME_UNIT_MILLIS);
    wake_time.tv_sec = wake_time_millis / 1000;
    wake_time.tv_nsec = (wake_time_millis % 1000) * 1000000;
    pthread_cond_timedwait(&priv->worker_work, &priv->mutex, &wake_time);
}

/*
 * Return current time in units of TIME_UNIT_MILLIS milliseconds since startup.
 */
static uint32_t
block_cache_get_time(struct block_cache_private *priv)
{
    uint64_t since_start;

    since_start = block_cache_get_time_millis() - priv->start_time;
    return (uint32_t)(since_start / TIME_UNIT_MILLIS);
}

/*
 * Return current time in milliseconds.
 */
static uint64_t
block_cache_get_time_millis(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

static void
block_cache_free_one(void *arg, void *value)
{
    struct block_cache_private *const priv = arg;
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *const entry = value;

    if (config->cache_file == NULL)
        free(entry->u.data);
    free(entry);
}

/*
 * Mark an entry verified and free the extra bytes we allocated for the MD5 checksum.
 */
static struct cache_entry *
block_cache_verified(struct block_cache_private *priv, struct cache_entry *entry)
{
    struct cache_entry *new_entry;

    assert(entry->verify);
    assert(!ENTRY_IN_LIST(entry));
    if ((new_entry = realloc(entry, sizeof(*entry))) == NULL)
        return entry;
    s3b_hash_put(priv->hashtable, new_entry);
    new_entry->verify = 0;
    return new_entry;
}

/*
 * Read the data from a cached block into a buffer.
 */
static int
block_cache_read_data(struct block_cache_private *priv, struct cache_entry *entry, void *dest, u_int off, u_int len)
{
    struct block_cache_conf *const config = priv->config;

    /* Sanity check */
    assert(off <= config->block_size);
    assert(len <= config->block_size);
    assert(off + len <= config->block_size);

    /* Handle easy in-memory case */
    if (config->cache_file == NULL) {
        memcpy(dest, (char *)entry->u.data + off, len);
        return 0;
    }

    /* Handle on-disk case */
    return s3b_dcache_read_block(priv->dcache, entry->u.dslot, dest, off, len);
}

/*
 * Write the data in a buffer to a cached block.
 */
static int
block_cache_write_data(struct block_cache_private *priv, struct cache_entry *entry, const void *src, u_int off, u_int len)
{
    struct block_cache_conf *const config = priv->config;

    /* Sanity check */
    assert(off <= config->block_size);
    assert(len <= config->block_size);
    assert(off + len <= config->block_size);

    /* Handle easy in-memory case */
    if (config->cache_file == NULL) {
        if (src == NULL)
            memset((char *)entry->u.data + off, 0, len);
        else
            memcpy((char *)entry->u.data + off, src, len);
        return 0;
    }

    /* Handle on-disk case */
    return s3b_dcache_write_block(priv->dcache, entry->u.dslot, src, off, len);
}

/*
 * Compute dirty ratio, i.e., percent of total cache space occupied by non-CLEAN[2] entries.
 */
static double
block_cache_dirty_ratio(struct block_cache_private *priv)
{
    struct block_cache_conf *const config = priv->config;

    return (double)(s3b_hash_size(priv->hashtable) - priv->num_cleans) / (double)config->cache_size;
}

static void
block_cache_dirty_callback(void *arg, void *value)
{
    struct cbinfo *const cbinfo = arg;
    struct cache_entry *const entry = value;

    switch (ENTRY_GET_STATE(entry)) {
    case CLEAN:
    case CLEAN2:
    case READING:
    case READING2:
        break;
    case WRITING2:
    case WRITING:
    case DIRTY:
        (*cbinfo->callback)(cbinfo->arg, entry->block_num);
        break;
    default:
        assert(0);
        break;
    }
}

#ifndef NDEBUG

/* Accounting structure */
struct check_info {
    u_int   num_clean;
    u_int   num_dirty;
    u_int   num_reading;
    u_int   num_writing;
    u_int   num_writing2;
};

static void
block_cache_check_invariants(struct block_cache_private *priv)
{
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    struct check_info info;
    int clean_len = 0;
    int dirty_len = 0;

    /* Check CLEANs and CLEAN2s */
    for (entry = TAILQ_FIRST(&priv->cleans); entry != NULL; entry = TAILQ_NEXT(entry, link)) {
        assert(ENTRY_GET_STATE(entry) == CLEAN || ENTRY_GET_STATE(entry) == CLEAN2);
        assert(s3b_hash_get(priv->hashtable, entry->block_num) == entry);
        clean_len++;
    }
    assert(clean_len == priv->num_cleans);

    /* Check DIRTYs */
    for (entry = TAILQ_FIRST(&priv->dirties); entry != NULL; entry = TAILQ_NEXT(entry, link)) {
        assert(ENTRY_GET_STATE(entry) == DIRTY);
        assert(s3b_hash_get(priv->hashtable, entry->block_num) == entry);
        dirty_len++;
    }

    /* Check hash table size */
    assert(s3b_hash_size(priv->hashtable) <= config->cache_size);

    /* Check hash table entries */
    memset(&info, 0, sizeof(info));
    s3b_hash_foreach(priv->hashtable, block_cache_check_one, &info);

    /* Check agreement */
    assert(info.num_clean == clean_len);
    assert(info.num_dirty == dirty_len);
    assert(info.num_clean + info.num_dirty + info.num_reading + info.num_writing + info.num_writing2
      == s3b_hash_size(priv->hashtable));

    /* Check read-ahead */
    assert(priv->ra_count <= config->read_ahead);
}

static void
block_cache_check_one(void *arg, void *value)
{
    struct cache_entry *const entry = value;
    struct check_info *const info = arg;

    assert(entry != NULL);
    switch (ENTRY_GET_STATE(entry)) {
    case CLEAN:
    case CLEAN2:
        info->num_clean++;
        break;
    case DIRTY:
        info->num_dirty++;
        break;
    case READING:
    case READING2:
        assert(!entry->dirty);
        info->num_reading++;
        break;
    case WRITING:
        info->num_writing++;
        break;
    case WRITING2:
        info->num_writing2++;
        break;
    default:
        assert(0);
        break;
    }
}
#endif

