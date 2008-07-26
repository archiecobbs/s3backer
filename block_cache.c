
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

/*
 * This file implements a simple block cache that acts as a "layer" on top
 * of an underlying s3backer_store.
 *
 * Blocks in the cache are in one of these states:
 *
 *  CLEAN       Data is consistent with underlying s3backer_store
 *  DIRTY       Data is inconsistent with underlying s3backer_store (needs writing)
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
 * block goes back to DIRTY and to the end of the DIRTY list: the result is that failed
 * writes of DIRTY blocks will retry indefinitely. If the write is successful, the
 * block moves to CLEAN if still in state WRITING, of DIRTY if in WRITING2.
 *
 * Because we allow writes to update the data in a block while that block is being
 * written, the worker threads must make a copy of the data before they send it.
 *
 * Blocks in the WRITING and WRITING2 states are not in either list.
 *
 * Only CLEAN blocks are eligible to be evicted from the cache.
 */

/* Cache entry states */
#define CLEAN           0
#define DIRTY           1
#define WRITING         2

/*
 * One cache entry. In order to keep this structure as small as possible, we do
 * two size optimizations:
 *
 *  1. We use the low-order bit of '_data' as the dirty flag (we assume all valid
 *     pointers are aligned to an even address).
 *  2. When not linked into either list (i.e., in WRITING state), we set both members
 *     of the 'link' field to zero to indicate this. This works because at least one
 *     will always be non-zero when the structure is linked into a list.
 */
struct cache_entry {
    s3b_block_t                     block_num;      // block number
    TAILQ_ENTRY(cache_entry)        link;           // next in list (cleans or dirties)
    void                            *_data;         // block's data (bit zero = dirty bit)
};
#define ENTRY_IS_DIRTY(entry)               (((intptr_t)(entry)->_data & 1) != 0)
#define ENTRY_SET_DIRTY(entry)              do { (*((intptr_t *)&(entry)->_data)) |= (intptr_t)1; } while (0)
#define ENTRY_SET_CLEAN(entry)              do { (*((intptr_t *)&(entry)->_data)) &= ~(intptr_t)1; } while (0)
#define ENTRY_GET_DATA(entry)               ((void *)((intptr_t)(entry)->_data & ~(intptr_t)1))
#define ENTRY_SET_DATA(entry, data, state)  do { (entry)->_data = (state) == CLEAN ? (void *)(data) \
                                              : (void *)((intptr_t)(data) | (intptr_t)1); } while (0)
#define ENTRY_IN_LIST(entry)                ((entry)->link.tqe_next != NULL || (entry)->link.tqe_prev != NULL)
#define ENTRY_RESET_LINK(entry)             do { memset(&(entry)->link, 0, sizeof((entry)->link)); } while (0)
#define ENTRY_GET_STATE(entry)              (!ENTRY_IN_LIST(entry) ? WRITING : ENTRY_IS_DIRTY(entry) ? DIRTY : CLEAN)

/* Private data */
struct block_cache_private {
    struct block_cache_conf         *config;        // configuration
    struct s3backer_store           *inner;         // underlying s3backer store
    struct block_cache_stats        stats;          // statistics
    TAILQ_HEAD(, cache_entry)       cleans;         // list of clean blocks (LRU order)
    TAILQ_HEAD(, cache_entry)       dirties;        // list of dirty blocks (write order)
    GHashTable                      *hashtable;     // hashtable of all cached blocks
    u_int                           num_threads;    // number of worker threads
    int                             stopping;       // signals worker threads to exit
    pthread_mutex_t                 mutex;          // my mutex
    pthread_cond_t                  new_dirty;      // there is a new dirty cache entry
    pthread_cond_t                  new_clean;      // there is a new clean cache entry
    pthread_cond_t                  stopped;        // all worker threads have exited
};

/* s3backer_store functions */
static int block_cache_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5);
static int block_cache_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, const u_char *md5);
static int block_cache_detect_sizes(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static void block_cache_destroy(struct s3backer_store *s3b);

/* Other functions */
static void *block_cache_worker_main(void *arg);
static int block_cache_get_entry(struct block_cache_private *priv, struct cache_entry **entryp, void **datap);
static struct cache_entry *block_cache_hash_get(struct block_cache_private *priv, s3b_block_t block_num);
static void block_cache_hash_put(struct block_cache_private *priv, struct cache_entry *entry);
static void block_cache_hash_remove(struct block_cache_private *priv, s3b_block_t block_num);
static void block_cache_free_one(gpointer key, gpointer value, gpointer arg);

/* Invariants checking */
#ifndef NDEBUG
static void block_cache_check_invariants(struct block_cache_private *priv);
static void block_cache_check_one(gpointer key, gpointer value, gpointer arg);
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

    /* Sanity check: we use block numbers as g_hash_table keys */
    if (sizeof(s3b_block_t) > sizeof(gpointer)) {
        (*config->log)(LOG_ERR, "sizeof(s3b_block_t) = %d is too big!", (int)sizeof(s3b_block_t));
        r = EINVAL;
        goto fail0;
    }

    /* Initialize s3backer_store structure */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }
    s3b->read_block = block_cache_read_block;
    s3b->write_block = block_cache_write_block;
    s3b->detect_sizes = block_cache_detect_sizes;
    s3b->destroy = block_cache_destroy;

    /* Initialize block_cache_private structure */
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        goto fail1;
    }
    priv->config = config;
    priv->inner = inner;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    if ((r = pthread_cond_init(&priv->new_dirty, NULL)) != 0)
        goto fail3;
    if ((r = pthread_cond_init(&priv->new_clean, NULL)) != 0)
        goto fail4;
    if ((r = pthread_cond_init(&priv->stopped, NULL)) != 0)
        goto fail5;
    TAILQ_INIT(&priv->cleans);
    TAILQ_INIT(&priv->dirties);
    if ((priv->hashtable = g_hash_table_new(NULL, NULL)) == NULL) {
        r = errno;
        goto fail6;
    }
    s3b->data = priv;

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Create threads */
    for (priv->num_threads = 0; priv->num_threads < config->num_threads; priv->num_threads++) {
        if ((r = pthread_create(&thread, NULL, block_cache_worker_main, priv)) != 0) {
            priv->stopping = 1;
            pthread_cond_broadcast(&priv->new_dirty);
            while (priv->num_threads > 0)
                pthread_cond_wait(&priv->stopped, &priv->mutex);
        }
    }

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    return s3b;

fail6:
    pthread_cond_destroy(&priv->stopped);
fail5:
    pthread_cond_destroy(&priv->new_clean);
fail4:
    pthread_cond_destroy(&priv->new_dirty);
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

    /* Wait for all dirty blocks to be flushed and all worker threads to exit */
    priv->stopping = 1;
    while (TAILQ_FIRST(&priv->dirties) != NULL || priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->new_dirty);
        pthread_cond_wait(&priv->stopped, &priv->mutex);
    }

    /* Free structures */
    pthread_mutex_destroy(&priv->mutex);
    pthread_cond_destroy(&priv->new_clean);
    pthread_cond_destroy(&priv->new_dirty);
    pthread_cond_destroy(&priv->stopped);
    g_hash_table_foreach(priv->hashtable, block_cache_free_one, NULL);
    g_hash_table_destroy(priv->hashtable);
    free(priv);
    free(s3b);
}

void
block_cache_get_stats(struct s3backer_store *s3b, struct block_cache_stats *stats)
{
    struct block_cache_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    stats->current_size = g_hash_table_size(priv->hashtable);
    pthread_mutex_unlock(&priv->mutex);
}

static int
block_cache_detect_sizes(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    struct block_cache_private *const priv = s3b->data;

    return (*priv->inner->detect_sizes)(priv->inner, file_sizep, block_sizep);
}

static int
block_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5)
{
    struct block_cache_private *const priv = s3b->data;
    struct block_cache_conf *const config = priv->config;
    struct cache_entry *entry;
    void *data;
    int r = 0;

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* If cache entry exists, we're home free */
    if ((entry = block_cache_hash_get(priv, block_num)) != NULL) {

hit:
        /* Copy cached data */
        assert(entry->block_num == block_num);
        memcpy(dest, ENTRY_GET_DATA(entry), config->block_size);
        priv->stats.read_hits++;

        /* If CLEAN, move to the end of the list to maintain LRU ordering */
        if (ENTRY_GET_STATE(entry) == CLEAN) {
            TAILQ_REMOVE(&priv->cleans, entry, link);
            TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
        }
        goto done;
    }
    priv->stats.read_misses++;

    /* Read the block from the underlying s3backer_store */
    pthread_mutex_unlock(&priv->mutex);
    r = (*priv->inner->read_block)(priv->inner, block_num, dest, expect_md5);
    pthread_mutex_lock(&priv->mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Check the cache again; another operation on the same block could have occurred */
    if ((entry = block_cache_hash_get(priv, block_num)) != NULL)
        goto hit;

    /* Get a cache entry; if none available, no big deal */
    if ((r = block_cache_get_entry(priv, &entry, &data)) != 0 || entry == NULL)
        goto done;

    /* Initialize new CLEAN cache entry */
    entry->block_num = block_num;
    memcpy(data, dest, config->block_size);
    ENTRY_SET_DATA(entry, data, CLEAN);
    block_cache_hash_put(priv, entry);
    TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
    //pthread_cond_broadcast(&priv->new_clean);         /* this is not necessary */

done:
    /* Release lock */
    pthread_mutex_unlock(&priv->mutex);
    return r;
}

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
    /* Find cache entry */
    S3BCACHE_CHECK_INVARIANTS(priv);
    if ((entry = block_cache_hash_get(priv, block_num)) != NULL) {
        assert(entry->block_num == block_num);
        switch (ENTRY_GET_STATE(entry)) {
        case CLEAN:                 /* change to DIRTY */
            TAILQ_REMOVE(&priv->cleans, entry, link);
            TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
            pthread_cond_signal(&priv->new_dirty);
            // FALLTHROUGH
        case WRITING:               /* update dirty data and move to WRITING2 */
        case DIRTY:                 /* just update the dirty data */
            memcpy(ENTRY_GET_DATA(entry), src, config->block_size);
            ENTRY_SET_DIRTY(entry); /* if DIRTY, no change; if WRITING, -> WRITING2 */
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

    /* If none available, wait for one */
    if (entry == NULL) {
        pthread_cond_wait(&priv->new_clean, &priv->mutex);
        goto again;
    }

    /* Initialize a new DIRTY cache entry */
    priv->stats.write_misses++;
    entry->block_num = block_num;
    ENTRY_SET_DATA(entry, data, DIRTY);
    memcpy(data, src, config->block_size);
    block_cache_hash_put(priv, entry);
    TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
    pthread_cond_signal(&priv->new_dirty);

done:
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
    if (g_hash_table_size(priv->hashtable) < config->cache_size) {
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
        block_cache_hash_remove(priv, entry->block_num);
        data = ENTRY_GET_DATA(entry);
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
    char *buf;
    int r;

    /*
     * Allocate buffer for outgoing block data. We have to copy it before
     * we send it in case another write to this block comes in and updates
     * the buffer associated with the cache entry. Prefer heap space (because
     * it will likely be page-aligned), falling back to stack if that fails.
     */
    if ((buf = malloc(block_size)) == NULL) {
        (*config->log)(LOG_ERR, "block_cache worker can't alloc buffer, exiting: %s", strerror(errno));
        return NULL;
    }

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);

    /* Repeatedly do stuff until told to stop */
    while (1) {

        /* Sanity check */
        S3BCACHE_CHECK_INVARIANTS(priv);

        /* See if there is a block that needs writing */
        if ((entry = TAILQ_FIRST(&priv->dirties)) != NULL) {

            /* Move to WRITING state */
            assert(ENTRY_IS_DIRTY(entry));
            TAILQ_REMOVE(&priv->dirties, entry, link);
            ENTRY_RESET_LINK(entry);
            ENTRY_SET_CLEAN(entry);

            /* Copy data to private buffer */
            memcpy(buf, ENTRY_GET_DATA(entry), block_size);

            /* Attempt to write the block */
            pthread_mutex_unlock(&priv->mutex);
            r = (*priv->inner->write_block)(priv->inner, entry->block_num, buf, NULL);
            pthread_mutex_lock(&priv->mutex);
            S3BCACHE_CHECK_INVARIANTS(priv);

            /* If that failed, go back to the DIRTY state and try again */
            if (r != 0) {
                TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
                ENTRY_SET_DIRTY(entry);
                continue;
            }

            /* If block was not modified while being written, it is now CLEAN */
            if (!ENTRY_IS_DIRTY(entry)) {
                TAILQ_INSERT_TAIL(&priv->cleans, entry, link);
                pthread_cond_broadcast(&priv->new_clean);
                continue;
            }

            /* Block was modified while being written, so it's still DIRTY */
            TAILQ_INSERT_TAIL(&priv->dirties, entry, link);
            continue;
        }

        /* Are we supposed to stop? */
        if (priv->stopping != 0)
            break;

        /* Sleep until there is more to do */
        pthread_cond_wait(&priv->new_dirty, &priv->mutex);
    }

    /* Notify main thread that we're exiting */
    pthread_cond_signal(&priv->stopped);

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    free(buf);
    return NULL;
}

static void
block_cache_free_one(gpointer key, gpointer value, gpointer arg)
{
    struct cache_entry *const entry = value;

    free(ENTRY_GET_DATA(entry));
    free(entry);
}

/*
 * Find a 'struct cache_entry' in the hash table.
 */
static struct cache_entry *
block_cache_hash_get(struct block_cache_private *priv, s3b_block_t block_num)
{
    gconstpointer key = (gpointer)block_num;

    return (struct cache_entry *)g_hash_table_lookup(priv->hashtable, key);
}

/*
 * Add a 'struct cache_entry' to the hash table.
 */
static void
block_cache_hash_put(struct block_cache_private *priv, struct cache_entry *entry)
{
    gpointer key = (gpointer)entry->block_num;
#ifndef NDEBUG
    int size = g_hash_table_size(priv->hashtable);
#endif

    g_hash_table_replace(priv->hashtable, key, entry);
#ifndef NDEBUG
    assert(g_hash_table_size(priv->hashtable) == size + 1);
#endif
}

/*
 * Remove a 'struct cache_entry' from the hash table.
 */
static void
block_cache_hash_remove(struct block_cache_private *priv, s3b_block_t block_num)
{
    gconstpointer key = (gpointer)block_num;
#ifndef NDEBUG
    int size = g_hash_table_size(priv->hashtable);
#endif

    g_hash_table_remove(priv->hashtable, key);
#ifndef NDEBUG
    assert(g_hash_table_size(priv->hashtable) == size - 1);
#endif
}

#ifndef NDEBUG

/* Accounting structure */
struct check_info {
    u_int   num_clean;
    u_int   num_dirty;
    u_int   num_writing;
};

static void
block_cache_check_invariants(struct block_cache_private *priv)
{
    struct cache_entry *entry;
    struct check_info info;
    int clean_len = 0;
    int dirty_len = 0;

    /* Check CLEANs */
    for (entry = TAILQ_FIRST(&priv->cleans); entry != NULL; entry = TAILQ_NEXT(entry, link)) {
        assert(ENTRY_GET_STATE(entry) == CLEAN);
        assert(block_cache_hash_get(priv, entry->block_num) == entry);
        clean_len++;
    }

    /* Check DIRTYs */
    for (entry = TAILQ_FIRST(&priv->dirties); entry != NULL; entry = TAILQ_NEXT(entry, link)) {
        assert(ENTRY_GET_STATE(entry) == DIRTY);
        assert(block_cache_hash_get(priv, entry->block_num) == entry);
        dirty_len++;
    }

    /* Check hash table entries */
    memset(&info, 0, sizeof(info));
    g_hash_table_foreach(priv->hashtable, block_cache_check_one, &info);

    /* Check agreement */
    assert(info.num_clean == clean_len);
    assert(info.num_dirty == dirty_len);
    assert(info.num_clean + info.num_dirty + info.num_writing == g_hash_table_size(priv->hashtable));
}

static void
block_cache_check_one(gpointer key, gpointer value, gpointer arg)
{
    struct cache_entry *const entry = value;
    struct check_info *const info = arg;

    assert(entry != NULL);
    assert(entry->block_num == (s3b_block_t)key);
    switch (ENTRY_GET_STATE(entry)) {
    case CLEAN:                 /* change to DIRTY */
        info->num_clean++;
        break;
    case DIRTY:                 /* just update data */
        info->num_dirty++;
        break;
    case WRITING:               /* wait for previous write to complete */
        info->num_writing++;
        break;
    default:
        assert(0);
        break;
    }
}
#endif

