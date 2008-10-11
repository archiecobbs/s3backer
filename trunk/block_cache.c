
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
#include "hash.h"

/*
 * This file implements a simple block cache that acts as a "layer" on top
 * of an underlying s3backer_store.
 *
 * Blocks in the cache are in one of these states:
 *
 *  CLEAN       Data is consistent with underlying s3backer_store
 *  DIRTY       Data is inconsistent with underlying s3backer_store (needs writing)
 *  READING     Data is being read from the underlying s3backer_store
 *  WRITING     Data is being written to underlying s3backer_store
 *  WRITING2    Same as WRITING, but a subsequent write has stored new data
 *
 * Blocks in the CLEAN state are linked in a list in order from least recently used
 * to most recently used (where 'used' means either read or written).
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
 * written, the worker threads must make a copy of the data before they send it.
 *
 * Blocks in the READING, WRITING and WRITING2 states are not in either list.
 *
 * Only CLEAN blocks are eligible to be evicted from the cache. We evict CLEAN entries
 * either when they timeout or the cache is full and we need to add a new entry to it.
 */

/* Cache entry states */
#define CLEAN           0
#define DIRTY           1
#define READING         2
#define WRITING         3
#define WRITING2        4

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
 *  State       ENTRY_IN_LIST()?    ENTRY_IS_DIRTY()?   entry->timeout == -1
 *  -----       ----------------    -----------------   --------------------
 *
 *  CLEAN       YES: priv->cleans   NO                  ?
 *  DIRTY       YES: priv->dirties  YES                 ?
 *  READING     NO                  NO                  YES
 *  WRITING     NO                  NO                  NO
 *  WRITING2    NO                  YES                 NO
 *
 * Timeouts: we trak time in units of TIME_UNIT_MILLIS milliseconds from when we start.
 * This is so we can jam them into 32 bits instead of 64. It's theoretically possible
 * for these time values to wrap, but the effect is harmless (an early write or eviction).
 */
struct cache_entry {
    s3b_block_t                     block_num;      // block number - MUST BE FIRST
    uint32_t                        timeout;        // when to evict (CLEAN) or write (DIRTY)
    TAILQ_ENTRY(cache_entry)        link;           // next in list (cleans or dirties)
    void                            *_data;         // block's data (bit zero = dirty bit)
};
#define ENTRY_IS_DIRTY(entry)               (((intptr_t)(entry)->_data & 1) != 0)
#define ENTRY_SET_DIRTY(entry)              do { (entry)->_data = (void *)((intptr_t)(entry)->_data | (intptr_t)1); } while (0)
#define ENTRY_SET_CLEAN(entry)              do { (entry)->_data = (void *)((intptr_t)(entry)->_data & ~(intptr_t)1); } while (0)
#define ENTRY_GET_DATA(entry)               ((void *)((intptr_t)(entry)->_data & ~(intptr_t)1))
#define ENTRY_SET_DATA(entry, data, state)  do { (entry)->_data = (state) == CLEAN ? (void *)(data) \
                                              : (void *)((intptr_t)(data) | (intptr_t)1); } while (0)
#define ENTRY_IN_LIST(entry)                ((entry)->link.tqe_prev != NULL)
#define ENTRY_RESET_LINK(entry)             do { (entry)->link.tqe_prev = NULL; } while (0)
#define ENTRY_GET_STATE(entry)              (ENTRY_IN_LIST(entry) ?                             \
                                                (ENTRY_IS_DIRTY(entry) ? DIRTY : CLEAN) :       \
                                                ((entry)->timeout == READING_TIMEOUT ?          \
                                                  READING :                                     \
                                                  ENTRY_IS_DIRTY(entry) ? WRITING2 : WRITING))

/* One time unit in milliseconds */
#define TIME_UNIT_MILLIS            16

/* The dirty ratio at which we want to be writing out dirty blocks immediately */
#define DIRTY_RATIO_WRITE_ASAP      0.90            // 90%

/* Special timeout value for entries in state READING */
#define READING_TIMEOUT             (~(uint32_t)0)

/* Private data */
struct block_cache_private {
    struct block_cache_conf         *config;        // configuration
    struct s3backer_store           *inner;         // underlying s3backer store
    struct block_cache_stats        stats;          // statistics
    TAILQ_HEAD(, cache_entry)       cleans;         // list of clean blocks (LRU order)
    TAILQ_HEAD(, cache_entry)       dirties;        // list of dirty blocks (write order)
    struct s3b_hash                 *hashtable;     // hashtable of all cached blocks
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
    pthread_cond_t                  end_reading;    // some entry in state READING changed state
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
static int block_cache_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5);
static int block_cache_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, const u_char *md5);
static int block_cache_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static void block_cache_destroy(struct s3backer_store *s3b);

/* Other functions */
static int block_cache_do_read_block(struct block_cache_private *priv, s3b_block_t block_num, void *dest, const u_char *expect_md5);
static void block_cache_read_from_entry(struct block_cache_private *priv, struct cache_entry *entry, void *dest);
static void *block_cache_worker_main(void *arg);
static int block_cache_get_entry(struct block_cache_private *priv, struct cache_entry **entryp, void **datap);
static void block_cache_free_one(void *arg, void *value);
static void block_cache_dirty_callback(void *arg, void *value);
static double block_cache_dirty_ratio(struct block_cache_private *priv);
static void block_cache_worker_wait(struct block_cache_private *priv, struct cache_entry *entry);
static uint32_t block_cache_get_time(struct block_cache_private *priv);
static uint64_t block_cache_get_time_millis(void);

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
    pthread_t thread;
    int r;

    /* Initialize s3backer_store structure */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }
    s3b->read_block = block_cache_read_block;
    s3b->write_block = block_cache_write_block;
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

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Create threads */
    for (priv->num_threads = 0; priv->num_threads < config->num_threads; priv->num_threads++) {
        if ((r = pthread_create(&thread, NULL, block_cache_worker_main, priv)) != 0)
            goto fail9;
    }

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    return s3b;

fail9:
    priv->stopping = 1;
    while (priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->mutex);
    }
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
 * Destructor
 */
static void
block_cache_destroy(struct s3backer_store *const s3b)
{
    struct block_cache_private *const priv = s3b->data;

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Wait for all dirty blocks to be written and all worker threads to exit */
    priv->stopping = 1;
    while (TAILQ_FIRST(&priv->dirties) != NULL || priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->mutex);
    }

    /* Free structures */
    s3b_hash_foreach(priv->hashtable, block_cache_free_one, NULL);
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

/*
 * Read a block, and trigger read-ahead if necessary.
 */
static int
block_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5)
{
    struct block_cache_private *const priv = s3b->data;
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
    r = block_cache_do_read_block(priv, block_num, dest, expect_md5);

    /* Release lock */
    pthread_mutex_unlock(&priv->mutex);
    return r;
}

/*
 * Read a block.
 *
 * Assumes the mutex is held.
 */
static int
block_cache_do_read_block(struct block_cache_private *priv, s3b_block_t block_num, void *dest, const u_char *expect_md5)
{
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    void *data;
    int r;

again:
    /* Check to see if a cache entry already exists */
    if ((entry = s3b_hash_get(priv->hashtable, block_num)) != NULL) {
        assert(entry->block_num == block_num);

        /* If READING, wait for other thread already reading this block to finish */
        if (ENTRY_GET_STATE(entry) == READING) {
            pthread_cond_wait(&priv->end_reading, &priv->mutex);
            goto again;
        }

        /* Copy data from the cache entry */
        block_cache_read_from_entry(priv, entry, dest);
        priv->stats.read_hits++;
        return 0;
    }

    /* Create a new cache entry in state READING */
    if ((r = block_cache_get_entry(priv, &entry, &data)) != 0)
        return r;
    if (entry == NULL) {
        pthread_cond_wait(&priv->space_avail, &priv->mutex);
        goto again;
    }
    entry->block_num = block_num;
    entry->timeout = READING_TIMEOUT;
    ENTRY_SET_DATA(entry, data, CLEAN);
    ENTRY_RESET_LINK(entry);
    s3b_hash_put(priv->hashtable, entry);
    assert(ENTRY_GET_STATE(entry) == READING);

    /* Update stats */
    priv->stats.read_misses++;

    /* Read the block from the underlying s3backer_store */
    pthread_mutex_unlock(&priv->mutex);
    r = (*priv->inner->read_block)(priv->inner, block_num, data, expect_md5);
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* The entry should still exist and be in state READING */
    assert(s3b_hash_get(priv->hashtable, block_num) == entry);
    assert(ENTRY_GET_STATE(entry) == READING);
    assert(ENTRY_GET_DATA(entry) == data);

    /*
     * We know two things at this point: the state is going to
     * change from READING and we will create new available space
     * in the cache. Wake up any threads waiting on those events.
     */
    pthread_cond_broadcast(&priv->end_reading);
    pthread_cond_signal(&priv->space_avail);

    /* Check for error from underlying s3backer_store */
    if (r != 0) {
        s3b_hash_remove(priv->hashtable, entry->block_num);
        free(data);
        free(entry);
        return r;
    }

    /* Copy the block data into the destination buffer */
    memcpy(dest, data, config->block_size);

    /* Change entry from READING to CLEAN */
    assert(ENTRY_GET_STATE(entry) == READING);
    entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;
    TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
    priv->num_cleans++;
    assert(ENTRY_GET_STATE(entry) == CLEAN);

    /* Done */
    return 0;
}

/*
 * Read block's data from a cache entry and update the entry.
 *
 * Assumes the entry is not in state READING and the mutex is held.
 */
static void
block_cache_read_from_entry(struct block_cache_private *priv, struct cache_entry *entry, void *dest)
{
    struct block_cache_conf *const config = priv->config;

    switch (ENTRY_GET_STATE(entry)) {
    case CLEAN:

        /* Update timestamp and move to the end of the list to maintain LRU ordering */
        TAILQ_REMOVE(&priv->cleans, entry, link);
        TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
        entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;

        // FALLTHROUGH
    case DIRTY:
    case WRITING:
    case WRITING2:

        /* Copy the cached data */
        memcpy(dest, ENTRY_GET_DATA(entry), config->block_size);
        break;

    case READING:
    default:
        assert(0);
        break;
    }
}

/*
 * Write a block.
 */
static int
block_cache_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, const u_char *md5)
{
    struct block_cache_private *const priv = s3b->data;
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    void *data;
    int r = 0;

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
            pthread_cond_wait(&priv->end_reading, &priv->mutex);
            goto again;
        case CLEAN:                 /* update data, move to state DIRTY */
            TAILQ_REMOVE(&priv->cleans, entry, link);
            priv->num_cleans--;
            TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
            entry->timeout = block_cache_get_time(priv) + priv->dirty_timeout;
            pthread_cond_signal(&priv->worker_work);
            // FALLTHROUGH
        case WRITING2:              /* update data, stay in state WRITING2 */
        case WRITING:               /* update data, move to state WRITING2 */
        case DIRTY:                 /* update data, stay in state DIRTY */
            if (src != NULL)
                memcpy(ENTRY_GET_DATA(entry), src, config->block_size);
            else
                memset(ENTRY_GET_DATA(entry), 0, config->block_size);
            ENTRY_SET_DIRTY(entry);
            priv->stats.write_hits++;
            break;
        default:
            assert(0);
            break;
        }
        goto done;
    }

    /* Get a cache entry, evicting a CLEAN entry if necessary */
    if ((r = block_cache_get_entry(priv, &entry, &data)) != 0)
        goto done;

    /* If cache is full, wait for an entry to go CLEAN so we can evict it */
    if (entry == NULL) {
        pthread_cond_wait(&priv->space_avail, &priv->mutex);
        goto again;
    }

    /* Initialize a new DIRTY cache entry */
    priv->stats.write_misses++;
    entry->block_num = block_num;
    entry->timeout = block_cache_get_time(priv) + priv->dirty_timeout;
    ENTRY_SET_DATA(entry, data, DIRTY);
    if (src != NULL)
        memcpy(data, src, config->block_size);
    else
        memset(data, 0, config->block_size);
    s3b_hash_put(priv->hashtable, entry);
    TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
    assert(ENTRY_GET_STATE(entry) == DIRTY);

    /* Wake up a worker thread to go write it */
    pthread_cond_signal(&priv->worker_work);

done:
    /* If doing synchronous writes, wait for write to complete */
    if (r == 0 && config->synchronous) {
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
            if (state == CLEAN || state == READING)
                break;

            /* Not written yet, go back to sleep */
            continue;
        }
    }

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    return r;
}

/*
 * Acquire a new cache entry. If the cache is full, and there is at least one
 * CLEAN entry, evict and return it (uninitialized). Otherwise, return NULL entry.
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
    void *data;

    /*
     * If cache is not full, allocate a new entry. We allocate the structure
     * and the data separately in hopes that the malloc() implementation will
     * put the data into its own page of virtual memory.
     */
    if (s3b_hash_size(priv->hashtable) < config->cache_size) {
        if ((entry = malloc(sizeof(*entry))) == NULL) {
            priv->stats.out_of_memory_errors++;
            return errno;
        }
        if ((data = malloc(config->block_size)) == NULL) {
            priv->stats.out_of_memory_errors++;
            free(entry);
            return ENOMEM;
        }
        goto done;
    }

    /* Is there a CLEAN entry we can evict? */
    if ((entry = TAILQ_FIRST(&priv->cleans)) != NULL) {
        TAILQ_REMOVE(&priv->cleans, entry, link);
        s3b_hash_remove(priv->hashtable, entry->block_num);
        data = ENTRY_GET_DATA(entry);
        priv->num_cleans--;
    } else
        data = NULL;

done:
    /* Return what we got (if anything) */
    *entryp = entry;
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
    const u_int block_size = config->block_size;
    struct cache_entry *entry;
    struct cache_entry *clean_entry = NULL;
    uint32_t adjusted_now;
    uint32_t now;
    u_int thread_id;
    char *buf;
    int r;

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);

    /* Assign myself a thread ID (for debugging purposes) */
    thread_id = priv->thread_id++;

    /*
     * Allocate buffer for outgoing block data. We have to copy it before
     * we send it in case another write to this block comes in and updates
     * the data associated with the cache entry. Use heap space because
     * it will likely be page-aligned.
     */
    if ((buf = malloc(block_size)) == NULL) {
        (*config->log)(LOG_ERR, "block_cache worker %u can't alloc buffer, exiting: %s", thread_id, strerror(errno));
        goto done;
    }

    /* Repeatedly do stuff until told to stop */
    while (1) {

        /* Sanity check */
        S3BCACHE_CHECK_INVARIANTS(priv);

        /* Get current time */
        now = block_cache_get_time(priv);

        /* Evict any CLEAN blocks that have timed out (if enabled) */
        if (priv->clean_timeout != 0) {
            while ((clean_entry = TAILQ_FIRST(&priv->cleans)) != NULL && now >= clean_entry->timeout) {
                s3b_hash_remove(priv->hashtable, clean_entry->block_num);
                TAILQ_REMOVE(&priv->cleans, clean_entry, link);
                free(ENTRY_GET_DATA(clean_entry));
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

            /* Move to WRITING state */
            assert(ENTRY_GET_STATE(entry) == DIRTY);
            TAILQ_REMOVE(&priv->dirties, entry, link);
            ENTRY_RESET_LINK(entry);
            ENTRY_SET_CLEAN(entry);
            entry->timeout = 0;
            assert(ENTRY_GET_STATE(entry) == WRITING);

            /* Copy data to private buffer */
            memcpy(buf, ENTRY_GET_DATA(entry), block_size);

            /* Attempt to write the block */
            pthread_mutex_unlock(&priv->mutex);
            r = (*priv->inner->write_block)(priv->inner, entry->block_num, buf, NULL);
            pthread_mutex_lock(&priv->mutex);
            S3BCACHE_CHECK_INVARIANTS(priv);

            /* Sanity check */
            assert(ENTRY_GET_STATE(entry) == WRITING || ENTRY_GET_STATE(entry) == WRITING2);

            /* If that failed, go back to the DIRTY state and try again */
            if (r != 0) {
                ENTRY_SET_DIRTY(entry);
                TAILQ_INSERT_HEAD(&priv->dirties, entry, link);
                continue;
            }

            /* If block was not modified while being written (WRITING), it is now CLEAN */
            if (!ENTRY_IS_DIRTY(entry)) {
                TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
                entry->timeout = block_cache_get_time(priv) + priv->clean_timeout;
                priv->num_cleans++;
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
                (void)block_cache_do_read_block(priv, ra_block, buf, NULL);
                break;
            }
            continue;
        }

        /* There is nothing to do at this time; sleep until there is something to do */
        if (entry == NULL || (clean_entry != NULL && clean_entry->timeout < entry->timeout))
            entry = clean_entry;
        block_cache_worker_wait(priv, entry);
    }

done:
    /* Decrement live worker thread count */
    priv->num_threads--;
    pthread_cond_signal(&priv->worker_exit);

    /* Free block data buffer */
    free(buf);

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
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
    struct cache_entry *const entry = value;

    free(ENTRY_GET_DATA(entry));
    free(entry);
}

/*
 * Compute dirty ratio, i.e., percent of total cache space occupied by
 * non-CLEAN entries.
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
    case READING:
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

    /* Check CLEANs */
    for (entry = TAILQ_FIRST(&priv->cleans); entry != NULL; entry = TAILQ_NEXT(entry, link)) {
        assert(ENTRY_GET_STATE(entry) == CLEAN);
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
        info->num_clean++;
        break;
    case DIRTY:
        info->num_dirty++;
        break;
    case READING:
        assert(!ENTRY_IS_DIRTY(entry));
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

