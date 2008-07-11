
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

#include <sys/types.h>

#include "s3backer.h"

/* HTTP definitions */
#define HTTP_GET                    "GET"
#define HTTP_PUT                    "PUT"
#define HTTP_DELETE                 "DELETE"
#define HTTP_HEAD                   "HEAD"
#define HTTP_UNAUTHORIZED           401
#define HTTP_FORBIDDEN              403
#define HTTP_NOT_FOUND              404
#define HTTP_PRECONDITION_FAILED    412
#define DATE_HEADER                 "Date"
#define AUTH_HEADER                 "Authorization"
#define CTYPE_HEADER                "Content-Type"
#define MD5_HEADER                  "Content-MD5"
#define ACL_HEADER                  "x-amz-acl"
#define FILE_SIZE_HEADER            "x-amz-meta-s3backer-filesize"
#define IF_MATCH_HEADER             "If-Match"

/* MIME type for blocks */
#define CONTENT_TYPE                "application/x-s3backer-block"

/* HTTP `Date' header format */
#define DATE_BUF_SIZE               64
#define DATE_BUF_FMT                "%a, %d %b %Y %H:%M:%S GMT"

/*
 * Written block information caching.
 *
 * The purpose of this is to minimize problems from the weak guarantees provided
 * by S3's "eventual consistency". We do this by:
 *
 *  (a) Enforcing a minimum delay between the completion of one PUT/DELETE
 *      of a block and the initiation of the next PUT/DELETE of the same block
 *  (b) Caching the MD5 checksum of every block written for some minimum time
 *      and verifying that data returned from subsequent GETs is correct.
 *
 * These are the relevant configuration parameters:
 *
 *  min_write_delay
 *      Minimum time delay after a PUT/DELETE completes before the next PUT/DELETE
 *      can be initiated.
 *  cache_time
 *      How long after writing a block we'll remember its MD5 checksum. This
 *      must be at least as long as min_write_delay.
 *  cache_size
 *      Maximum number of blocks we'll track at one time. When table
 *      is full, additional writes will block.
 *  initial_retry_pause, max_retry_pause
 *      Retry timing when a GET returns stale data.
 *
 * Blocks we are currently tracking can be in the following states:
 *
 * State    Meaning                     Hash table  List    Other invariants
 * -----    -------                     ----------  ----    ----------------
 *
 * CLEAN    initial state               No          No
 * WRITING  currently being written     Yes         No      timestamp == 0
 * WRITTEN  written and MD5 cached      Yes         Yes     timestamp != 0
 *
 * The steady state for a block is CLEAN. WRITING means the block is currently
 * being sent; concurrent attempts to write will simply sleep until the first one
 * finishes. WRITTEN is where you go after successfully writing a block. The WRITTEN
 * state will timeout (and the entry revert to CLEAN) after cache_time.
 *
 * If another attempt to write a block in the WRITTEN state occurs occurs before
 * min_write_delay has elapsed, the second attempt will sleep.
 *
 * A separate thread periodically scans the table and removes expired WRITTENs
 *
 * In the WRITING state, we have the data still so any reads are local. In the WRITTEN
 * state we don't have the data but we do know its MD5, so therefore we can verify what
 * comes back; if it doesn't verify, we use {initial,max}_retry_pause to time retries.
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
    s3b_block_t         block_num;              // block number
    uint64_t            timestamp;              // time PUT/DELETE completed (if WRITTEN)
    struct block_info   *next;                  // next entry in list (if WRITTEN)
    struct block_info   **prev;                 // previous entry in list (if WRITTEN)
    union {
        const void      *data;                  // blocks actual content (if WRITING)
        u_char          md5[MD5_DIGEST_LENGTH]; // block's content MD5 (if WRITTEN)
    } u;
};

/* Internal definitions */
struct s3backer_curl_holder {
    CURL                        *curl;
    struct s3backer_curl_holder *next;
};

/* CURL prepper function type */
typedef void (*s3b_curl_prepper_t)(CURL *curl);

/* Simple list structure */
struct s3backer_list {
    struct block_info           *head;
    struct block_info           **tail;
};
#define IS_EMPTY(list)          ((list)->head == NULL)
#define IN_LIST(list, binfo)    ((binfo)->next != NULL || (list)->tail == &(binfo)->next)

/* Internal state */
struct s3backer_private {
    struct s3backer_conf        *config;
    struct s3backer_curl_holder *curls;
    pthread_mutex_t             curls_mutex;
    GHashTable                  *hashtable;
    struct s3backer_list        list;
    pthread_mutex_t             mutex;
    pthread_cond_t              space_cond;     // signaled when cache space available
    pthread_cond_t              never_cond;     // never signaled; used for sleeping only
    char                        *zero_block;
    u_char                      *non_zero;      // used when 'assume_empty' is set
};

/* I/O state when reading/writing a block */
struct s3b_io {
    size_t      rdremain;
    size_t      wrremain;
    char        *rdbuf;
    const char  *wrbuf;
    uintmax_t   file_size;
};

/* s3backer_store functions */
static int s3backer_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest);
static int s3backer_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src);
static int s3backer_detect_sizes(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static void s3backer_destroy(struct s3backer_store *s3b);

/* Other functions */
static int s3backer_do_read_block(struct s3backer_store *const s3b, s3b_block_t block_num,
    void *dest, const u_char *expect_md5);
static int s3backer_do_write_block(struct s3backer_store *const s3b, s3b_block_t block_num,
    const void *src, const u_char *md5);

/* S3 REST API functions */
static char *s3backer_get_url(char *buf, size_t bufsiz, const char *baseURL, const char *bucket,
    const char *prefix, s3b_block_t block_num);
static void s3backer_get_auth(char *buf, size_t bufsiz, const char *accessKey, const char *method,
    const char *ctype, const char *md5, const char *date, const struct curl_slist *headers, const char *resource);

/* HTTP and curl functions */
static int s3backer_perform_io(struct s3backer_private *priv, const char *method, const char *url,
    u_int *clenp, s3b_curl_prepper_t prepper);
static size_t s3backer_curl_reader(void *ptr, size_t size, size_t nmemb, void *stream);
static size_t s3backer_curl_writer(void *ptr, size_t size, size_t nmemb, void *stream);
static size_t s3backer_curl_header(void *ptr, size_t size, size_t nmemb, void *stream);
static struct curl_slist *s3backer_add_header(struct curl_slist *headers, const char *fmt, ...);
static void s3backer_get_date(char *buf, size_t bufsiz);
static CURL *s3backer_acquire_curl(struct s3backer_private *priv);
static void s3backer_release_curl(struct s3backer_private *priv, CURL *curl, int may_cache);

/* Data structure manipulation */
static void s3backer_list_append(struct s3backer_list *list, struct block_info *binfo);
static void s3backer_list_remove(struct s3backer_list *list, struct block_info *binfo);
static struct block_info *s3backer_hash_get(struct s3backer_private *priv, s3b_block_t block_num);
static void s3backer_hash_put(struct s3backer_private *priv, struct block_info *binfo);
static void s3backer_hash_remove(struct s3backer_private *priv, s3b_block_t block_num);

/* Misc */
static int s3backer_sleep_until(struct s3backer_private *priv, uint64_t wake_time_millis);
static int s3backer_sleep_until_cond(struct s3backer_private *priv, pthread_cond_t *cond, uint64_t wake_time_millis);
static void s3backer_openssl_locker(int mode, int i, const char *file, int line);
static unsigned long s3backer_openssl_ider(void);
static void s3backer_base64_encode(char *buf, size_t bufsiz, const void *data, size_t len);
static void s3backer_scrub_expired_writtens(struct s3backer_private *priv, uint64_t current_time);
static uint64_t s3backer_get_time(void);
static void s3backer_free_one(gpointer key, gpointer value, gpointer arg);

/* Invariants checking */
#ifndef NDEBUG
static void s3backer_check_one(gpointer key, gpointer value, gpointer user_data);
static void s3backer_check_invariants(struct s3backer_private *priv);

#define S3BACKER_CHECK_INVARIANTS(priv)     s3backer_check_invariants(priv)
#else
#define S3BACKER_CHECK_INVARIANTS(priv)     do { } while (0)
#endif

/* Internal variables */
static pthread_mutex_t *openssl_locks;
static int num_openssl_locks;
static const u_char zero_md5[MD5_DIGEST_LENGTH];

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
s3backer_create(struct s3backer_conf *config)
{
    struct s3backer_store *s3b;
    struct s3backer_private *priv;
    int nlocks;
    int r;

    /* Sanity check: we can really only handle one instance */
    if (openssl_locks != NULL) {
        (*config->log)(LOG_ERR, "s3backer_create() called twice?");
        r = EALREADY;
        goto fail0;
    }

    /* Initialize structures */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }
    s3b->read_block = s3backer_read_block;
    s3b->write_block = s3backer_write_block;
    s3b->detect_sizes = s3backer_detect_sizes;
    s3b->destroy = s3backer_destroy;
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        goto fail1;
    }
    priv->config = config;
    if ((r = pthread_mutex_init(&priv->curls_mutex, NULL)) != 0)
        goto fail2;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail3;
    if ((r = pthread_cond_init(&priv->space_cond, NULL)) != 0)
        goto fail4;
    if ((r = pthread_cond_init(&priv->never_cond, NULL)) != 0)
        goto fail5;
    priv->list.tail = &priv->list.head;
    if ((priv->hashtable = g_hash_table_new(NULL, NULL)) == NULL) {
        r = errno;
        goto fail6;
    }
    s3b->data = priv;

    /* Initialize openssl */
    num_openssl_locks = CRYPTO_num_locks();
    if ((openssl_locks = malloc(num_openssl_locks * sizeof(*openssl_locks))) == NULL) {
        r = errno;
        goto fail7;
    }
    for (nlocks = 0; nlocks < num_openssl_locks; nlocks++) {
        if ((r = pthread_mutex_init(&openssl_locks[nlocks], NULL)) != 0) {
            while (nlocks > 0)
                pthread_mutex_destroy(&openssl_locks[--nlocks]);
            goto fail8;
        }
    }
    CRYPTO_set_locking_callback(s3backer_openssl_locker);
    CRYPTO_set_id_callback(s3backer_openssl_ider);

    /* Initialize cURL */
    curl_global_init(CURL_GLOBAL_ALL);

    /* Done */
    S3BACKER_CHECK_INVARIANTS(priv);
    (*config->log)(LOG_INFO, "created s3backer using %s%s", config->baseURL, config->bucket);
    return s3b;

fail8:
    free(openssl_locks);
    openssl_locks = NULL;
    num_openssl_locks = 0;
fail7:
    g_hash_table_destroy(priv->hashtable);
fail6:
    pthread_cond_destroy(&priv->never_cond);
fail5:
    pthread_cond_destroy(&priv->space_cond);
fail4:
    pthread_mutex_destroy(&priv->mutex);
fail3:
    pthread_mutex_destroy(&priv->curls_mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "s3backer creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

/*
 * Destructor
 */
static void
s3backer_destroy(struct s3backer_store *const s3b)
{
    struct s3backer_private *const priv = s3b->data;

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->mutex);
    S3BACKER_CHECK_INVARIANTS(priv);

    /* Clean up openssl */
    while (num_openssl_locks > 0)
        pthread_mutex_destroy(&openssl_locks[--num_openssl_locks]);
    free(openssl_locks);
    openssl_locks = NULL;
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);

    /* Clean up cURL */
    while (priv->curls != NULL) {
        struct s3backer_curl_holder *holder = priv->curls;

        curl_easy_cleanup(holder->curl);
        priv->curls = holder->next;
        free(holder);
    }
    curl_global_cleanup();

    /* Free structures */
    pthread_mutex_destroy(&priv->curls_mutex);
    pthread_mutex_destroy(&priv->mutex);
    pthread_cond_destroy(&priv->space_cond);
    pthread_cond_destroy(&priv->never_cond);
    g_hash_table_foreach(priv->hashtable, s3backer_free_one, NULL);
    g_hash_table_destroy(priv->hashtable);
    free(priv->zero_block);
    free(priv->non_zero);
    free(priv);
    free(s3b);
}

static int
s3backer_detect_sizes(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    auto void s3b_detect_prepper(CURL *curl);
    struct s3backer_private *const priv = s3b->data;
    struct s3backer_conf *const config = priv->config;
    struct curl_slist *headers = NULL;
    char urlbuf[strlen(config->baseURL) + strlen(config->bucket) + strlen(config->prefix) + 64];
    const char *resource;
    char authbuf[200];
    struct s3b_io s3b_io;
    char datebuf[64];
    int r;

    /* Construct URL for the first block */
    resource = s3backer_get_url(urlbuf, sizeof(urlbuf), config->baseURL, config->bucket, config->prefix, 0);

    /* Add Date header */
    s3backer_get_date(datebuf, sizeof(datebuf));
    headers = s3backer_add_header(headers, "%s: %s", DATE_HEADER, datebuf);

    /* Add Authorization header */
    if (config->accessId != NULL) {
        s3backer_get_auth(authbuf, sizeof(authbuf), config->accessKey,
          HTTP_HEAD, NULL, NULL, datebuf, headers, resource);
        headers = s3backer_add_header(headers, "%s: AWS %s:%s", AUTH_HEADER, config->accessId, authbuf);
    }

    /* Perform operation */
    r = s3backer_perform_io(priv, HTTP_HEAD, urlbuf, block_sizep, s3b_detect_prepper);

    /* If successful, extract filesystem sizing information */
    if (r == 0) {
        if (s3b_io.file_size > 0)
            *file_sizep = (off_t)s3b_io.file_size;
        else
            r = ENXIO;
    }

    /*  Clean up */
    curl_slist_free_all(headers);
    return r;

    /* CURL prepper function */
    void s3b_detect_prepper(CURL *curl) {
        memset(&s3b_io, 0, sizeof(s3b_io));
        curl_easy_setopt(curl, CURLOPT_URL, urlbuf);
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, s3backer_curl_reader);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s3b_io);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, s3backer_curl_header);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &s3b_io);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }
}

static int
s3backer_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest)
{
    struct s3backer_private *const priv = s3b->data;
    struct s3backer_conf *const config = priv->config;
    const u_char *expect_md5 = NULL;
    u_char md5[MD5_DIGEST_LENGTH];
    struct block_info *binfo;

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->mutex);
    S3BACKER_CHECK_INVARIANTS(priv);

    /* Scrub the list of WRITTENs */
    s3backer_scrub_expired_writtens(priv, s3backer_get_time());

    /* Find info for this block */
    if ((binfo = s3backer_hash_get(priv, block_num)) != NULL) {

        /* In WRITING state: we have the data already! */
        if (binfo->timestamp == 0) {
            if (binfo->u.data == NULL)
                memset(dest, 0, config->block_size);
            else
                memcpy(dest, binfo->u.data, config->block_size);
            pthread_mutex_unlock(&priv->mutex);
            return 0;
        }

        /* In WRITTEN state: special case: zero block */
        if (memcmp(binfo->u.md5, zero_md5, MD5_DIGEST_LENGTH) == 0) {
            memset(dest, 0, config->block_size);
            pthread_mutex_unlock(&priv->mutex);
            return 0;
        }

        /* In WRITTEN state: we know the expected MD5 */
        memcpy(md5, binfo->u.md5, MD5_DIGEST_LENGTH);
        expect_md5 = md5;
    }

    /* Release lock */
    pthread_mutex_unlock(&priv->mutex);

    /* Read block normally */
    return s3backer_do_read_block(s3b, block_num, dest, expect_md5);
}

static int
s3backer_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src)
{
    struct s3backer_private *const priv = s3b->data;
    struct s3backer_conf *const config = priv->config;
    u_char md5[MD5_DIGEST_LENGTH];
    struct block_info *binfo;
    uint64_t current_time;
    MD5_CTX md5ctx;
    int r;

    /* Sanity check */
    if (config->block_size == 0)
        return EINVAL;

    /* Allocate zero block if necessary */
    if (priv->zero_block == NULL
      && (priv->zero_block = calloc(1, config->block_size)) == NULL)
        return errno;

    /* Allocate empty block array if necessary */
    if (config->assume_empty && priv->non_zero == NULL
      && (priv->non_zero = calloc(1, (config->num_blocks + 7) / 8)) == NULL)
        return errno;

    /* Special case handling for all-zeroes blocks */
    if (memcmp(src, priv->zero_block, config->block_size) == 0)
        src = NULL;

    /* Compute MD5 of block */
    if (src != NULL) {
        MD5_Init(&md5ctx);
        MD5_Update(&md5ctx, src, config->block_size);
        MD5_Final(md5, &md5ctx);
    } else
        memcpy(md5, zero_md5, MD5_DIGEST_LENGTH);

    /* If cache is disabled, this is easy */
    if (config->cache_size == 0)
        return s3backer_do_write_block(s3b, block_num, src, md5);

    /* Grab lock */
    pthread_mutex_lock(&priv->mutex);

again:
    /* Sanity check */
    S3BACKER_CHECK_INVARIANTS(priv);

    /* Scrub the list of WRITTENs */
    current_time = s3backer_get_time();
    s3backer_scrub_expired_writtens(priv, current_time);

    /* Find info for this block */
    binfo = s3backer_hash_get(priv, block_num);

    /* CLEAN case: add new entry in state WRITING and write the block */
    if (binfo == NULL) {

        /* If we have reached max cache capacity, wait until there's more room */
        if (g_hash_table_size(priv->hashtable) >= config->cache_size) {
            if ((binfo = priv->list.head) != NULL)
                s3backer_sleep_until_cond(priv, &priv->space_cond, binfo->timestamp + config->cache_time);
            else
                pthread_cond_wait(&priv->space_cond, &priv->mutex);
            goto again;
        }

        /* Create new entry in WRITING state */
        if ((binfo = calloc(1, sizeof(*binfo))) == NULL) {
            pthread_mutex_unlock(&priv->mutex);
            return errno;
        }
        binfo->block_num = block_num;
        binfo->u.data = src;
        s3backer_hash_put(priv, binfo);

writeit:
        /* Write the block */
        pthread_mutex_unlock(&priv->mutex);
        r = s3backer_do_write_block(s3b, block_num, src, md5);
        pthread_mutex_lock(&priv->mutex);
        S3BACKER_CHECK_INVARIANTS(priv);

        /* If there was an error, just return it and forget */
        if (r != 0) {
            s3backer_hash_remove(priv, block_num);
            pthread_cond_signal(&priv->space_cond);
            pthread_mutex_unlock(&priv->mutex);
            free(binfo);
            return r;
        }

        /* Move to state WRITTEN */
        binfo->timestamp = s3backer_get_time();
        memcpy(binfo->u.md5, md5, MD5_DIGEST_LENGTH);
        s3backer_list_append(&priv->list, binfo);
        pthread_mutex_unlock(&priv->mutex);
        return 0;
    }

    /*
     * WRITING case: wait until current write completes (hmm, why is kernel doing overlapping writes?).
     * Since we know after current write completes we'll have to wait another 'min_write_time' milliseconds
     * anyway, we conservatively just wait exactly that long now. There may be an extra wakeup or two,
     * but that's OK.
     */
    if (binfo->timestamp == 0) {
        s3backer_sleep_until(priv, current_time + config->min_write_delay);
        goto again;
    }

    /*
     * WRITTEN case: wait until at least 'min_write_time' milliseconds has passed since previous write.
     */
    if (current_time < binfo->timestamp + config->min_write_delay) {
        s3backer_sleep_until(priv, binfo->timestamp + config->min_write_delay);
        goto again;
    }

    /*
     * WRITTEN case: if 'min_write_time' milliseconds have indeed passed, go back to WRITING.
     */
    binfo->timestamp = 0;
    binfo->u.data = src;
    s3backer_list_remove(&priv->list, binfo);
    goto writeit;
}

static int
s3backer_do_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5)
{
    auto void s3b_read_prepper(CURL *curl);
    struct s3backer_private *const priv = s3b->data;
    struct s3backer_conf *const config = priv->config;
    struct curl_slist *headers = NULL;
    char urlbuf[strlen(config->baseURL) + strlen(config->bucket) + strlen(config->prefix) + 64];
    const char *resource;
    char authbuf[200];
    struct s3b_io s3b_io;
    char datebuf[64];
    int r;

    /* Construct URL for this block */
    resource = s3backer_get_url(urlbuf, sizeof(urlbuf), config->baseURL, config->bucket, config->prefix, block_num);

    /* Add Date header */
    s3backer_get_date(datebuf, sizeof(datebuf));
    headers = s3backer_add_header(headers, "%s: %s", DATE_HEADER, datebuf);

    /* Add If-Match header */
    if (expect_md5 != NULL) {
        headers = s3backer_add_header(headers, "%s: \"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\"",
          IF_MATCH_HEADER, expect_md5[0], expect_md5[1], expect_md5[2], expect_md5[3], expect_md5[4], expect_md5[5], expect_md5[6], expect_md5[7],
          expect_md5[8], expect_md5[9], expect_md5[10], expect_md5[11], expect_md5[12], expect_md5[13], expect_md5[14], expect_md5[15]);
    }

    /* Add Authorization header */
    if (config->accessId != NULL) {
        s3backer_get_auth(authbuf, sizeof(authbuf), config->accessKey,
          HTTP_GET, NULL, NULL, datebuf, headers, resource);
        headers = s3backer_add_header(headers, "%s: AWS %s:%s", AUTH_HEADER, config->accessId, authbuf);
    }

    /* Perform operation */
    r = s3backer_perform_io(priv, HTTP_GET, urlbuf, NULL, s3b_read_prepper);

    /* Check for short read */
    if (r == 0 && s3b_io.rdremain != 0) {
        (*config->log)(LOG_WARNING, "read of block #%u returned %lu < %lu bytes",
          block_num, (u_long)(config->block_size - s3b_io.rdremain), (u_long)config->block_size);
        memset((char *)dest + config->block_size - s3b_io.rdremain, 0, s3b_io.rdremain);
    }

    /* Treat `404 Not Found' all zeroes */
    if (r == ENOENT) {
        memset(dest, 0, config->block_size);
        r = 0;
    }

    /*  Clean up */
    curl_slist_free_all(headers);
    return r;

    /* CURL prepper function */
    void s3b_read_prepper(CURL *curl) {
        memset(&s3b_io, 0, sizeof(s3b_io));
        s3b_io.rdremain = config->block_size;
        s3b_io.rdbuf = dest;
        curl_easy_setopt(curl, CURLOPT_URL, urlbuf);
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, s3backer_curl_reader);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s3b_io);
        curl_easy_setopt(curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)config->block_size);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }
}

/*
 * Write block if src != NULL, otherwise delete block.
 */
static int
s3backer_do_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *const src, const u_char *md5)
{
    auto void s3b_write_prepper(CURL *curl);
    struct s3backer_private *const priv = s3b->data;
    struct s3backer_conf *const config = priv->config;
    struct curl_slist *headers = NULL;
    char urlbuf[strlen(config->baseURL) + strlen(config->bucket) + strlen(config->prefix) + 64];
    const char *const method = src != NULL ? HTTP_PUT : HTTP_DELETE;
    const char *resource;
    char md5buf[(MD5_DIGEST_LENGTH * 4) / 3 + 4];
    char authbuf[200];
    struct s3b_io s3b_io;
    char datebuf[64];
    int r;

    /* Check for read-only configuration */
    if (config->read_only)
        return EROFS;

    /* Don't write zero blocks when 'assume_empty' until non-zero content is written */
    if (config->assume_empty) {
        const int byte = block_num / 8;
        const int bit = 1 << (block_num % 8);

        pthread_mutex_lock(&priv->mutex);
        if ((priv->non_zero[byte] & bit) == 0) {
            if (src == NULL) {
                pthread_mutex_unlock(&priv->mutex);
                return 0;
            }
            priv->non_zero[byte] |= bit;
        }
        pthread_mutex_unlock(&priv->mutex);
    }

    /* Construct URL for this block */
    resource = s3backer_get_url(urlbuf, sizeof(urlbuf), config->baseURL, config->bucket, config->prefix, block_num);

    /* Add Date header */
    s3backer_get_date(datebuf, sizeof(datebuf));
    headers = s3backer_add_header(headers, "%s: %s", DATE_HEADER, datebuf);

    /* Add PUT-only headers */
    if (src != NULL) {

        /* Add Content-Type header */
        headers = s3backer_add_header(headers, "%s: %s", CTYPE_HEADER, CONTENT_TYPE);

        /* Add ACL header */
        headers = s3backer_add_header(headers, "%s: %s", ACL_HEADER, config->accessType);

        /* Add Content-MD5 header (if provided) */
        if (md5 != NULL) {
            s3backer_base64_encode(md5buf, sizeof(md5buf), md5, MD5_DIGEST_LENGTH);
            headers = s3backer_add_header(headers, "%s: %s", MD5_HEADER, md5buf);
        }
    }

    /* Add file size meta-data to zero'th block */
    if (block_num == 0)
        headers = s3backer_add_header(headers, "%s: %ju", FILE_SIZE_HEADER, config->file_size);

    /* Add Authorization header */
    if (config->accessId != NULL) {
        s3backer_get_auth(authbuf, sizeof(authbuf), config->accessKey,
          method, src != NULL ? CONTENT_TYPE : NULL, src != NULL ? md5buf : NULL, datebuf, headers, resource);
        headers = s3backer_add_header(headers, "%s: AWS %s:%s", AUTH_HEADER, config->accessId, authbuf);
    }

    /* Perform operation */
    r = s3backer_perform_io(priv, method, urlbuf, NULL, s3b_write_prepper);

    /*  Clean up */
    curl_slist_free_all(headers);
    return r;

    /* CURL prepper function */
    void s3b_write_prepper(CURL *curl) {
        memset(&s3b_io, 0, sizeof(s3b_io));
        if (src != NULL) {
            s3b_io.wrremain = config->block_size;
            s3b_io.wrbuf = src;
        }
        curl_easy_setopt(curl, CURLOPT_URL, urlbuf);
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, s3backer_curl_writer);
        curl_easy_setopt(curl, CURLOPT_READDATA, &s3b_io);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, s3backer_curl_reader);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s3b_io);
        if (src != NULL) {
            curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
            curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)config->block_size);
        }
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }
}

/*
 * Perform HTTP operation.
 */
static int
s3backer_perform_io(struct s3backer_private *priv, const char *method, const char *url, u_int *clenp, s3b_curl_prepper_t prepper)
{
    struct s3backer_conf *const config = priv->config;
    struct timespec delay;
    CURLcode curl_code;
    u_int retry_pause = 0;
    u_int total_pause;
    long http_code;
    double clen;
    int attempt;
    CURL *curl;

    /* Debug */
    if (config->debug)
        (*config->log)(LOG_DEBUG, "%s %s", method, url);

    /* Make attempts */
    for (attempt = 0, total_pause = 0; 1; attempt++, total_pause += retry_pause) {

        /* Acquire and initialize CURL instance */
        if ((curl = s3backer_acquire_curl(priv)) == NULL)
            return EIO;
        (*prepper)(curl);

        /* Perform HTTP operation and check result */
        if (attempt > 0)
            (*config->log)(LOG_INFO, "retrying query (attempt #%d): %s %s", attempt + 1, method, url);
        curl_code = curl_easy_perform(curl);

        /* Handle success */
        if (curl_code == 0) {
            int r = 0;

#ifndef NDEBUG
            /* Extra debug logging */
            if (config->debug)
                (*config->log)(LOG_DEBUG, "success: %s %s", method, url);
#endif

            /* Extract content-length (if required) */
            if (clenp != NULL) {
                if ((curl_code = curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &clen)) == CURLE_OK)
                    *clenp = (u_int)clen;
                else {
                    (*config->log)(LOG_ERR, "can't get content-length: %s", curl_easy_strerror(curl_code));
                    r = ENXIO;
                }
            }

            /* Done */
            s3backer_release_curl(priv, curl, r == 0);
            return r;
        }

        /* Handle errors */
        switch (curl_code) {
        case CURLE_OPERATION_TIMEDOUT:
            (*config->log)(LOG_NOTICE, "HTTP operation timeout: %s %s", method, url);
            s3backer_release_curl(priv, curl, 0);
            break;
        case CURLE_HTTP_RETURNED_ERROR:

            /* Get the HTTP return code */
            if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != 0) {
                (*config->log)(LOG_ERR, "unknown HTTP error: %s %s", method, url);
                s3backer_release_curl(priv, curl, 0);
                return EIO;
            }
            s3backer_release_curl(priv, curl, 0);

            /* Special handling for some specific HTTP codes */
            switch (http_code) {
            case HTTP_NOT_FOUND:
#ifndef NDEBUG
                if (config->debug)
                    (*config->log)(LOG_DEBUG, "rec'd %ld response: %s %s", http_code, method, url);
#endif
                return ENOENT;
            case HTTP_UNAUTHORIZED:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, method, url);
                return EACCES;
            case HTTP_FORBIDDEN:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, method, url);
                return EPERM;
            case HTTP_PRECONDITION_FAILED:
                (*config->log)(LOG_INFO, "rec'd stale content: %s %s", method, url);
                break;
            default:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, method, url);
                break;
            }
            break;
        default:
            (*config->log)(LOG_ERR, "curl error: %s", curl_easy_strerror(curl_code));
            break;
        }

        /* Retry with exponential backoff up to max total pause limit */
        if (total_pause >= config->max_retry_pause)
            break;
        retry_pause = retry_pause > 0 ? retry_pause * 2 : config->initial_retry_pause;
        if (total_pause + retry_pause > config->max_retry_pause)
            retry_pause = config->max_retry_pause - total_pause;
        delay.tv_sec = retry_pause / 1000;
        delay.tv_nsec = (retry_pause % 1000) * 1000000;
        nanosleep(&delay, NULL);
    }

    /* Give up */
    (*config->log)(LOG_ERR, "giving up: %s %s", method, url);
    return EIO;
}

/*
 * Compute S3 authorization hash using secret access key.
 */
static void
s3backer_get_auth(char *buf, size_t bufsiz, const char *accessKey, const char *method,
    const char *ctype, const char *md5, const char *date, const struct curl_slist *headers, const char *resource)
{
    const EVP_MD *sha1_md = EVP_sha1();
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len;
    char tosign[1024];

    /* Build string to sign */
    snprintf(tosign, sizeof(tosign), "%s\n%s\n%s\n%s\n",
      method, md5 != NULL ? md5 : "", ctype != NULL ? ctype : "", date);
    for ( ; headers != NULL; headers = headers->next) {
        const char *colon;
        const char *value;

        if (strncmp(headers->data, "x-amz", 5) != 0)
            continue;
        if ((colon = strchr(headers->data, ':')) == NULL)
            continue;
        for (value = colon + 1; isspace(*value); value++)
            ;
        snprintf(tosign + strlen(tosign), sizeof(tosign) - strlen(tosign), "%.*s:%s\n", colon - headers->data, headers->data, value);
    }
    snprintf(tosign + strlen(tosign), sizeof(tosign) - strlen(tosign), "%s", resource);

    /* Compute hash */
    HMAC(sha1_md, accessKey, strlen(accessKey), (unsigned char *)tosign, strlen(tosign), digest, &digest_len);

    /* Write it out base64 encoded */
    s3backer_base64_encode(buf, bufsiz, digest, digest_len);
}

/*
 * Create URL for a block, and return pointer to the URL's path.
 */
static char *
s3backer_get_url(char *buf, size_t bufsiz, const char *baseURL, const char *bucket, const char *prefix, s3b_block_t block_num)
{
    snprintf(buf, bufsiz, "%s%s/%s%08x", baseURL, bucket, prefix, block_num);
    return buf + strlen(baseURL) - 1;
}

/*
 * Get HTTP Date header value based on current time.
 */
static void
s3backer_get_date(char *buf, size_t bufsiz)
{
    time_t now = time(NULL);

    strftime(buf, bufsiz, DATE_BUF_FMT, gmtime(&now));
}

/*
 * Return current time in milliseconds.
 */
static uint64_t
s3backer_get_time(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

static struct curl_slist *
s3backer_add_header(struct curl_slist *headers, const char *fmt, ...)
{
    char buf[1024];
    va_list args;

    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    headers = curl_slist_append(headers, buf);
    va_end(args);
    return headers;
}

/*
 * Remove expired WRITTEN entries from the list.
 * This assumes the mutex is held.
 */
static void
s3backer_scrub_expired_writtens(struct s3backer_private *priv, uint64_t current_time)
{
    struct s3backer_conf *const config = priv->config;
    struct block_info *binfo;
    int num_removed = 0;

    while ((binfo = priv->list.head) != NULL && current_time >= binfo->timestamp + config->cache_time) {
        s3backer_list_remove(&priv->list, binfo);
        s3backer_hash_remove(priv, binfo->block_num);
        free(binfo);
        num_removed++;
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

static int
s3backer_sleep_until(struct s3backer_private *priv, uint64_t wake_time_millis)
{
    return s3backer_sleep_until_cond(priv, &priv->never_cond, wake_time_millis);
}

static int
s3backer_sleep_until_cond(struct s3backer_private *priv, pthread_cond_t *cond, uint64_t wake_time_millis)
{
    struct timespec wake_time;

    wake_time.tv_sec = wake_time_millis / 1000;
    wake_time.tv_nsec = (wake_time_millis % 1000) * 1000000;
    return pthread_cond_timedwait(cond, &priv->mutex, &wake_time);
}

static void
s3backer_free_one(gpointer key, gpointer value, gpointer arg)
{
    struct block_info *const binfo = value;

    free(binfo);
}

#ifndef NDEBUG

/* Accounting structure */
struct check_info {
    u_int   num_in_list;
    u_int   written;
    u_int   writing;
};

static void
s3backer_check_one(gpointer key, gpointer value, gpointer arg)
{
    struct block_info *const binfo = value;
    struct check_info *const info = arg;

    if (binfo->timestamp == 0)
        info->writing++;
    else
        info->written++;
}

static void
s3backer_check_invariants(struct s3backer_private *priv)
{
    struct block_info *binfo;
    struct check_info info;

    memset(&info, 0, sizeof(info));
    for (binfo = priv->list.head; binfo != NULL; binfo = binfo->next) {
        assert(binfo->timestamp != 0);
        assert(s3backer_hash_get(priv, binfo->block_num) == binfo);
        info.num_in_list++;
    }
    g_hash_table_foreach(priv->hashtable, s3backer_check_one, &info);
    assert(info.written == info.num_in_list);
    assert(info.written + info.writing == g_hash_table_size(priv->hashtable));
}
#endif

/*
 * Append a 'struct block_info' to the tail of the list.
 */
static void
s3backer_list_append(struct s3backer_list *list, struct block_info *binfo)
{
    assert(binfo != NULL);
    assert(binfo->next == NULL);
    assert(binfo->prev == NULL);
    assert(*list->tail == NULL);
    assert(!IN_LIST(list, binfo));
    binfo->prev = list->tail;
    *list->tail = binfo;
    list->tail = &binfo->next;
}

/*
 * Remove the 'struct block_info' pointed to by binfo from the list.
 */
static void
s3backer_list_remove(struct s3backer_list *list, struct block_info *binfo)
{
    assert(binfo != NULL);
    assert(*list->tail == NULL);
    assert(IN_LIST(list, binfo));
    *binfo->prev = binfo->next;
    if (binfo->next != NULL)
        binfo->next->prev = binfo->prev;
    else
        list->tail = binfo->prev;
    binfo->next = NULL;
    binfo->prev = NULL;
}

/*
 * Find a 'struct block_info' in the hash table.
 */
static struct block_info *
s3backer_hash_get(struct s3backer_private *priv, s3b_block_t block_num)
{
    gconstpointer key = (gpointer)block_num;

    return (struct block_info *)g_hash_table_lookup(priv->hashtable, key);
}

/*
 * Add a 'struct block_info' to the hash table.
 */
static void
s3backer_hash_put(struct s3backer_private *priv, struct block_info *binfo)
{
    gpointer key = (gpointer)binfo->block_num;
#ifndef NDEBUG
    int size = g_hash_table_size(priv->hashtable);
#endif

    g_hash_table_replace(priv->hashtable, key, binfo);
#ifndef NDEBUG
    assert(g_hash_table_size(priv->hashtable) == size + 1);
#endif
}

/*
 * Remove a 'struct block_info' from the hash table.
 */
static void
s3backer_hash_remove(struct s3backer_private *priv, s3b_block_t block_num)
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

static CURL *
s3backer_acquire_curl(struct s3backer_private *priv)
{
    struct s3backer_conf *const config = priv->config;
    struct s3backer_curl_holder *holder;
    CURL *curl;

    pthread_mutex_lock(&priv->curls_mutex);
    holder = priv->curls;
    if (holder != NULL) {
        curl = holder->curl;
        priv->curls = holder->next;
        pthread_mutex_unlock(&priv->curls_mutex);
        free(holder);
        curl_easy_reset(curl);
    } else {
        pthread_mutex_unlock(&priv->curls_mutex);
        if ((curl = curl_easy_init()) == NULL) {
            (*config->log)(LOG_ERR, "curl_easy_init() failed");
            return NULL;
        }
    }
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, (long)1);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long)config->connect_timeout);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config->io_timeout);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, config->user_agent);
    //curl_easy_setopt(curl, CURLOPT_VERBOSE);
    return curl;
}

static size_t
s3backer_curl_reader(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct s3b_io *const io = (struct s3b_io *)stream;
    size_t total = size * nmemb;

    if (total > io->rdremain)     /* should never happen */
        total = io->rdremain;
    memcpy(io->rdbuf, ptr, total);
    io->rdbuf += total;
    io->rdremain -= total;
    return total;
}

static size_t
s3backer_curl_writer(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct s3b_io *const io = (struct s3b_io *)stream;
    size_t total = size * nmemb;

    if (total > io->wrremain)     /* should never happen */
        total = io->wrremain;
    memcpy(ptr, io->wrbuf, total);
    io->wrbuf += total;
    io->wrremain -= total;
    return total;
}

static size_t
s3backer_curl_header(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct s3b_io *const io = (struct s3b_io *)stream;
    const size_t total = size * nmemb;
    char buf[1024];

    /* Null-terminate header */
    if (total > sizeof(buf) - 1)
        return total;
    memcpy(buf, ptr, total);
    buf[total] = '\0';

    /* Check for interesting headers */
    (void)sscanf(buf, FILE_SIZE_HEADER ": %ju", &io->file_size);
    return total;
}

static void
s3backer_release_curl(struct s3backer_private *priv, CURL *curl, int may_cache)
{
    struct s3backer_curl_holder *holder;

    if (!may_cache || (holder = calloc(1, sizeof(*holder))) == NULL) {
        curl_easy_cleanup(curl);
        return;
    }
    holder->curl = curl;
    pthread_mutex_lock(&priv->curls_mutex);
    holder->next = priv->curls;
    priv->curls = holder;
    pthread_mutex_unlock(&priv->curls_mutex);
}

static void
s3backer_openssl_locker(int mode, int i, const char *file, int line)
{
    if ((mode & CRYPTO_LOCK) != 0)
        pthread_mutex_lock(&openssl_locks[i]);
    else
        pthread_mutex_unlock(&openssl_locks[i]);
}

static unsigned long
s3backer_openssl_ider(void)
{
    return (unsigned long)pthread_self();
}

static void
s3backer_base64_encode(char *buf, size_t bufsiz, const void *data, size_t len)
{
    BUF_MEM *bptr;
    BIO* bmem;
    BIO* b64;

    b64 = BIO_new(BIO_f_base64());
    bmem = BIO_new(BIO_s_mem());
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, data, len);
    BIO_flush(b64);
    BIO_get_mem_ptr(b64, &bptr);
    snprintf(buf, bufsiz, "%.*s", bptr->length - 1, (char *)bptr->data);
    BIO_free_all(b64);
}

