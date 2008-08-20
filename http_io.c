
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
#include "http_io.h"

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
 * HTTP-based implementation of s3backer_store.
 *
 * This implementation does no caching or consistency checking.
 */

/* Internal definitions */
struct curl_holder {
    CURL                        *curl;
    LIST_ENTRY(curl_holder)     link;
};

/* Internal state */
struct http_io_private {
    struct http_io_conf         *config;
    struct http_io_stats        stats;
    LIST_HEAD(, curl_holder)    curls;
    pthread_mutex_t             mutex;
    u_int                       *non_zero;      // used when 'assume_empty' is set
};

/* I/O buffers */
struct http_io_bufs {
    size_t      rdremain;
    size_t      wrremain;
    char        *rddata;
    const char  *wrdata;
};

/* I/O state when reading/writing a block */
struct http_io {

    // I/O buffers
    struct http_io_bufs bufs;

    // Other info that needs to be passed around
    const char          *method;                // HTTP method
    const char          *url;                   // HTTP URL
    struct curl_slist   *headers;               // HTTP headers
    void                *dest;                  // Block data (when reading)
    const void          *src;                   // Block data (when writing)
    u_int               block_size;             // Block's size
    u_int               *content_lengthp;       // Returned Content-Length
    uintmax_t           file_size;              // file size from "x-amz-meta-s3backer-filesize"
};

/* CURL prepper function type */
typedef void (*http_io_curl_prepper_t)(CURL *curl, struct http_io *io);

/* s3backer_store functions */
static int http_io_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5);
static int http_io_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, const u_char *md5);
static void http_io_destroy(struct s3backer_store *s3b);

/* Other functions */
static void http_io_detect_prepper(CURL *curl, struct http_io *io);
static void http_io_read_prepper(CURL *curl, struct http_io *io);
static void http_io_write_prepper(CURL *curl, struct http_io *io);

/* S3 REST API functions */
static char *http_io_get_url(char *buf, size_t bufsiz, const char *baseURL, const char *bucket,
    const char *prefix, s3b_block_t block_num);
static void http_io_get_auth(char *buf, size_t bufsiz, const char *accessKey, const char *method,
    const char *ctype, const char *md5, const char *date, const struct curl_slist *headers, const char *resource);

/* HTTP and curl functions */
static int http_io_perform_io(struct http_io_private *priv, struct http_io *io, http_io_curl_prepper_t prepper);
static size_t http_io_curl_reader(void *ptr, size_t size, size_t nmemb, void *stream);
static size_t http_io_curl_writer(void *ptr, size_t size, size_t nmemb, void *stream);
static size_t http_io_curl_header(void *ptr, size_t size, size_t nmemb, void *stream);
static struct curl_slist *http_io_add_header(struct curl_slist *headers, const char *fmt, ...)
    __attribute__ ((__format__ (__printf__, 2, 3)));
static void http_io_get_date(char *buf, size_t bufsiz);
static CURL *http_io_acquire_curl(struct http_io_private *priv, struct http_io *io);
static void http_io_release_curl(struct http_io_private *priv, CURL *curl, int may_cache);

/* Misc */
static void http_io_openssl_locker(int mode, int i, const char *file, int line);
static unsigned long http_io_openssl_ider(void);
static void http_io_base64_encode(char *buf, size_t bufsiz, const void *data, size_t len);

/* Internal variables */
static pthread_mutex_t *openssl_locks;
static int num_openssl_locks;

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
http_io_create(struct http_io_conf *config)
{
    struct s3backer_store *s3b;
    struct http_io_private *priv;
    int nlocks;
    int r;

    /* Sanity check: we can really only handle one instance */
    if (openssl_locks != NULL) {
        (*config->log)(LOG_ERR, "http_io_create() called twice");
        r = EALREADY;
        goto fail0;
    }

    /* Initialize structures */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }
    s3b->read_block = http_io_read_block;
    s3b->write_block = http_io_write_block;
    s3b->destroy = http_io_destroy;
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        goto fail1;
    }
    priv->config = config;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    LIST_INIT(&priv->curls);
    s3b->data = priv;

    /* Initialize openssl */
    num_openssl_locks = CRYPTO_num_locks();
    if ((openssl_locks = malloc(num_openssl_locks * sizeof(*openssl_locks))) == NULL) {
        r = errno;
        goto fail3;
    }
    for (nlocks = 0; nlocks < num_openssl_locks; nlocks++) {
        if ((r = pthread_mutex_init(&openssl_locks[nlocks], NULL)) != 0) {
            while (nlocks > 0)
                pthread_mutex_destroy(&openssl_locks[--nlocks]);
            goto fail4;
        }
    }
    CRYPTO_set_locking_callback(http_io_openssl_locker);
    CRYPTO_set_id_callback(http_io_openssl_ider);

    /* Initialize cURL */
    curl_global_init(CURL_GLOBAL_ALL);

    /* Done */
    return s3b;

fail4:
    free(openssl_locks);
    openssl_locks = NULL;
    num_openssl_locks = 0;
fail3:
    pthread_mutex_destroy(&priv->mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "http_io creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

/*
 * Destructor
 */
static void
http_io_destroy(struct s3backer_store *const s3b)
{
    struct http_io_private *const priv = s3b->data;
    struct curl_holder *holder;

    /* Clean up openssl */
    while (num_openssl_locks > 0)
        pthread_mutex_destroy(&openssl_locks[--num_openssl_locks]);
    free(openssl_locks);
    openssl_locks = NULL;
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);

    /* Clean up cURL */
    while ((holder = LIST_FIRST(&priv->curls)) != NULL) {
        curl_easy_cleanup(holder->curl);
        LIST_REMOVE(holder, link);
        free(holder);
    }
    curl_global_cleanup();

    /* Free structures */
    pthread_mutex_destroy(&priv->mutex);
    free(priv->non_zero);
    free(priv);
    free(s3b);
}

void
http_io_get_stats(struct s3backer_store *s3b, struct http_io_stats *stats)
{
    struct http_io_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    pthread_mutex_unlock(&priv->mutex);
}

/*
 * Auto-detect block size and total size based on the first block.
 *
 * Returns:
 *
 *  0       Success
 *  ENOENT  Block not found
 *  ENXIO   Response was missing one of the two required headers
 *  Other   Other error
 */
int
http_io_detect_sizes(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[strlen(config->baseURL) + strlen(config->bucket) + strlen(config->prefix) + 64];
    const char *resource;
    char authbuf[200];
    struct http_io io;
    char datebuf[64];
    int r;

    /* Initialize I/O info */
    memset(&io, 0, sizeof(io));
    io.url = urlbuf;
    io.method = HTTP_HEAD;
    io.block_size = config->block_size;
    io.content_lengthp = block_sizep;

    /* Construct URL for the first block */
    resource = http_io_get_url(urlbuf, sizeof(urlbuf), config->baseURL, config->bucket, config->prefix, 0);

    /* Add Date header */
    http_io_get_date(datebuf, sizeof(datebuf));
    io.headers = http_io_add_header(io.headers, "%s: %s", DATE_HEADER, datebuf);

    /* Add Authorization header */
    if (config->accessId != NULL) {
        http_io_get_auth(authbuf, sizeof(authbuf), config->accessKey,
          io.method, NULL, NULL, datebuf, io.headers, resource);
        io.headers = http_io_add_header(io.headers, "%s: AWS %s:%s", AUTH_HEADER, config->accessId, authbuf);
    }

    /* Perform operation */
    r = http_io_perform_io(priv, &io, http_io_detect_prepper);

    /* If successful, extract filesystem sizing information */
    if (r == 0) {
        if (io.file_size > 0)
            *file_sizep = (off_t)io.file_size;
        else
            r = ENXIO;
    }

    /*  Clean up */
    curl_slist_free_all(io.headers);
    return r;
}

static void
http_io_detect_prepper(CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &io->bufs);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, http_io_curl_header);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, io);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, io->headers);
}

static int
http_io_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest, const u_char *expect_md5)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[strlen(config->baseURL) + strlen(config->bucket) + strlen(config->prefix) + 64];
    const char *resource;
    char authbuf[200];
    struct http_io io;
    char datebuf[64];
    int r;

    /* Sanity check */
    if (config->block_size == 0 || block_num >= config->num_blocks)
        return EINVAL;

    /* Read zero blocks when 'assume_empty' until non-zero content is written */
    if (config->assume_empty) {
        const int bits_per_word = sizeof(*priv->non_zero) * 8;
        const int word = block_num / bits_per_word;
        const int bit = 1 << (block_num % bits_per_word);

        pthread_mutex_lock(&priv->mutex);
        if (priv->non_zero == NULL || (priv->non_zero[word] & bit) == 0) {
            priv->stats.empty_blocks_read++;
            pthread_mutex_unlock(&priv->mutex);
            memset(dest, 0, config->block_size);
            return 0;
        }
        pthread_mutex_unlock(&priv->mutex);
    }

    /* Initialize I/O info */
    memset(&io, 0, sizeof(io));
    io.url = urlbuf;
    io.method = HTTP_GET;
    io.dest = dest;
    io.block_size = config->block_size;

    /* Construct URL for this block */
    resource = http_io_get_url(urlbuf, sizeof(urlbuf), config->baseURL, config->bucket, config->prefix, block_num);

    /* Add Date header */
    http_io_get_date(datebuf, sizeof(datebuf));
    io.headers = http_io_add_header(io.headers, "%s: %s", DATE_HEADER, datebuf);

    /* Add If-Match header */
    if (expect_md5 != NULL) {
        io.headers = http_io_add_header(io.headers, "%s: \"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\"",
          IF_MATCH_HEADER, expect_md5[0], expect_md5[1], expect_md5[2], expect_md5[3], expect_md5[4], expect_md5[5], expect_md5[6], expect_md5[7],
          expect_md5[8], expect_md5[9], expect_md5[10], expect_md5[11], expect_md5[12], expect_md5[13], expect_md5[14], expect_md5[15]);
    }

    /* Add Authorization header */
    if (config->accessId != NULL) {
        http_io_get_auth(authbuf, sizeof(authbuf), config->accessKey,
          io.method, NULL, NULL, datebuf, io.headers, resource);
        io.headers = http_io_add_header(io.headers, "%s: AWS %s:%s", AUTH_HEADER, config->accessId, authbuf);
    }

    /* Perform operation */
    r = http_io_perform_io(priv, &io, http_io_read_prepper);

    /* Check for short read */
    if (r == 0 && io.bufs.rdremain != 0) {
        (*config->log)(LOG_WARNING, "read of block #%u returned %lu < %lu bytes",
          block_num, (u_long)(config->block_size - io.bufs.rdremain), (u_long)config->block_size);
        memset((char *)dest + config->block_size - io.bufs.rdremain, 0, io.bufs.rdremain);
    }

    /* Update stats */
    pthread_mutex_lock(&priv->mutex);
    switch (r) {
    case 0:
        priv->stats.normal_blocks_read++;
        break;
    case ENOENT:
        priv->stats.zero_blocks_read++;
        break;
    default:
        break;
    }
    pthread_mutex_unlock(&priv->mutex);

    /* Treat `404 Not Found' all zeroes */
    if (r == ENOENT) {
        memset(dest, 0, config->block_size);
        r = 0;
    }

    /*  Clean up */
    curl_slist_free_all(io.headers);
    return r;
}

static void
http_io_read_prepper(CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    io->bufs.rdremain = io->block_size;
    io->bufs.rddata = io->dest;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &io->bufs);
    curl_easy_setopt(curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)io->block_size);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, io->headers);
}

/*
 * Write block if src != NULL, otherwise delete block.
 */
static int
http_io_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *const src, const u_char *md5)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[strlen(config->baseURL) + strlen(config->bucket) + strlen(config->prefix) + 64];
    const char *resource;
    char md5buf[(MD5_DIGEST_LENGTH * 4) / 3 + 4];
    char authbuf[200];
    struct http_io io;
    char datebuf[64];
    int r;

    /* Sanity check */
    if (config->block_size == 0 || block_num >= config->num_blocks)
        return EINVAL;

    /* Don't write zero blocks when 'assume_empty' until non-zero content is written */
    if (config->assume_empty) {
        const int bits_per_word = sizeof(*priv->non_zero) * 8;
        const int word = block_num / bits_per_word;
        const int bit = 1 << (block_num % bits_per_word);

        pthread_mutex_lock(&priv->mutex);
        if (src == NULL) {
            if (priv->non_zero == NULL || (priv->non_zero[word] & bit) == 0) {
                priv->stats.empty_blocks_written++;
                pthread_mutex_unlock(&priv->mutex);
                return 0;
            }
        } else {
            if (priv->non_zero == NULL && (priv->non_zero = calloc(sizeof(*priv->non_zero),
              (config->num_blocks + bits_per_word - 1) / bits_per_word)) == NULL) {
                priv->stats.out_of_memory_errors++;
                pthread_mutex_unlock(&priv->mutex);
                return ENOMEM;
            }
            priv->non_zero[word] |= bit;
        }
        pthread_mutex_unlock(&priv->mutex);
    }

    /* Initialize I/O info */
    memset(&io, 0, sizeof(io));
    io.url = urlbuf;
    io.method = src != NULL ? HTTP_PUT : HTTP_DELETE;
    io.src = src;
    io.block_size = config->block_size;

    /* Construct URL for this block */
    resource = http_io_get_url(urlbuf, sizeof(urlbuf), config->baseURL, config->bucket, config->prefix, block_num);

    /* Add Date header */
    http_io_get_date(datebuf, sizeof(datebuf));
    io.headers = http_io_add_header(io.headers, "%s: %s", DATE_HEADER, datebuf);

    /* Add PUT-only headers */
    if (src != NULL) {

        /* Add Content-Type header */
        io.headers = http_io_add_header(io.headers, "%s: %s", CTYPE_HEADER, CONTENT_TYPE);

        /* Add ACL header */
        io.headers = http_io_add_header(io.headers, "%s: %s", ACL_HEADER, config->accessType);

        /* Add Content-MD5 header (if provided) */
        if (md5 != NULL) {
            http_io_base64_encode(md5buf, sizeof(md5buf), md5, MD5_DIGEST_LENGTH);
            io.headers = http_io_add_header(io.headers, "%s: %s", MD5_HEADER, md5buf);
        }
    }

    /* Add file size meta-data to zero'th block */
    if (block_num == 0) {
        io.headers = http_io_add_header(io.headers, "%s: %ju",
          FILE_SIZE_HEADER, (uintmax_t)(config->block_size * config->num_blocks));
    }

    /* Add Authorization header */
    if (config->accessId != NULL) {
        http_io_get_auth(authbuf, sizeof(authbuf), config->accessKey, io.method,
          src != NULL ? CONTENT_TYPE : NULL, src != NULL && md5 != NULL ? md5buf : NULL,
          datebuf, io.headers, resource);
        io.headers = http_io_add_header(io.headers, "%s: AWS %s:%s", AUTH_HEADER, config->accessId, authbuf);
    }

    /* Perform operation */
    r = http_io_perform_io(priv, &io, http_io_write_prepper);

    /* Update stats */
    if (r == 0) {
        pthread_mutex_lock(&priv->mutex);
        if (src == NULL)
            priv->stats.zero_blocks_written++;
        else
            priv->stats.normal_blocks_written++;
        pthread_mutex_unlock(&priv->mutex);
    }

    /*  Clean up */
    curl_slist_free_all(io.headers);
    return r;
}

static void
http_io_write_prepper(CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    if (io->src != NULL) {
        io->bufs.wrremain = io->block_size;
        io->bufs.wrdata = io->src;
    }
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, http_io_curl_writer);
    curl_easy_setopt(curl, CURLOPT_READDATA, &io->bufs);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &io->bufs);
    if (io->src != NULL) {
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)io->block_size);
    }
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, io->method);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, io->headers);
}

/*
 * Perform HTTP operation.
 */
static int
http_io_perform_io(struct http_io_private *priv, struct http_io *io, http_io_curl_prepper_t prepper)
{
    struct http_io_conf *const config = priv->config;
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
        (*config->log)(LOG_DEBUG, "%s %s", io->method, io->url);

    /* Make attempts */
    for (attempt = 0, total_pause = 0; 1; attempt++, total_pause += retry_pause) {

        /* Acquire and initialize CURL instance */
        if ((curl = http_io_acquire_curl(priv, io)) == NULL)
            return EIO;
        (*prepper)(curl, io);

        /* Perform HTTP operation and check result */
        if (attempt > 0)
            (*config->log)(LOG_INFO, "retrying query (attempt #%d): %s %s", attempt + 1, io->method, io->url);
        curl_code = curl_easy_perform(curl);

        /* Handle success */
        if (curl_code == 0) {
            double curl_time;
            int r = 0;

            /* Extra debug logging */
            if (config->debug)
                (*config->log)(LOG_DEBUG, "success: %s %s", io->method, io->url);

            /* Extract timing info */
            if ((curl_code = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &curl_time)) != CURLE_OK) {
                (*config->log)(LOG_ERR, "can't get cURL timing: %s", curl_easy_strerror(curl_code));
                curl_time = 0.0;
            }

            /* Extract content-length (if required) */
            if (io->content_lengthp != NULL) {
                if ((curl_code = curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &clen)) == CURLE_OK)
                    *io->content_lengthp = (u_int)clen;
                else {
                    (*config->log)(LOG_ERR, "can't get content-length: %s", curl_easy_strerror(curl_code));
                    r = ENXIO;
                }
            }

            /* Update stats */
            pthread_mutex_lock(&priv->mutex);
            if (strcmp(io->method, HTTP_GET) == 0) {
                priv->stats.http_gets.count++;
                priv->stats.http_gets.time += curl_time;
            } else if (strcmp(io->method, HTTP_PUT) == 0) {
                priv->stats.http_puts.count++;
                priv->stats.http_puts.time += curl_time;
            } else if (strcmp(io->method, HTTP_DELETE) == 0) {
                priv->stats.http_deletes.count++;
                priv->stats.http_deletes.time += curl_time;
            } else if (strcmp(io->method, HTTP_HEAD) == 0) {
                priv->stats.http_heads.count++;
                priv->stats.http_heads.time += curl_time;
            }
            pthread_mutex_unlock(&priv->mutex);

            /* Done */
            http_io_release_curl(priv, curl, r == 0);
            return r;
        }

        /* Handle errors */
        switch (curl_code) {
        case CURLE_OPERATION_TIMEDOUT:
            (*config->log)(LOG_NOTICE, "operation timeout: %s %s", io->method, io->url);
            pthread_mutex_lock(&priv->mutex);
            priv->stats.curl_timeouts++;
            pthread_mutex_unlock(&priv->mutex);
            http_io_release_curl(priv, curl, 0);
            break;
        case CURLE_HTTP_RETURNED_ERROR:

            /* Get the HTTP return code */
            if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != 0) {
                (*config->log)(LOG_ERR, "unknown HTTP error: %s %s", io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_other_error++;
                pthread_mutex_unlock(&priv->mutex);
                http_io_release_curl(priv, curl, 0);
                return EIO;
            }
            http_io_release_curl(priv, curl, 0);

            /* Special handling for some specific HTTP codes */
            switch (http_code) {
            case HTTP_NOT_FOUND:
                if (config->debug)
                    (*config->log)(LOG_DEBUG, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                return ENOENT;
            case HTTP_UNAUTHORIZED:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_unauthorized++;
                pthread_mutex_unlock(&priv->mutex);
                return EACCES;
            case HTTP_FORBIDDEN:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_forbidden++;
                pthread_mutex_unlock(&priv->mutex);
                return EPERM;
            case HTTP_PRECONDITION_FAILED:
                (*config->log)(LOG_INFO, "rec'd stale content: %s %s", io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_stale++;
                pthread_mutex_unlock(&priv->mutex);
                break;
            default:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                switch (http_code / 100) {
                case 4:
                    priv->stats.http_4xx_error++;
                    break;
                case 5:
                    priv->stats.http_5xx_error++;
                    break;
                default:
                    priv->stats.http_other_error++;
                    break;
                }
                pthread_mutex_unlock(&priv->mutex);
                break;
            }
            break;
        default:
            (*config->log)(LOG_ERR, "operation failed: %s (%s)", curl_easy_strerror(curl_code),
              total_pause >= config->max_retry_pause ? "final attempt" : "will retry");
            pthread_mutex_lock(&priv->mutex);
            switch (curl_code) {
            case CURLE_OUT_OF_MEMORY:
                priv->stats.curl_out_of_memory++;
                break;
            case CURLE_COULDNT_CONNECT:
                priv->stats.curl_connect_failed++;
                break;
            case CURLE_COULDNT_RESOLVE_HOST:
                priv->stats.curl_host_unknown++;
                break;
            default:
                priv->stats.curl_other_error++;
                break;
            }
            pthread_mutex_unlock(&priv->mutex);
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
        nanosleep(&delay, NULL);            // TODO: check for EINTR

        /* Update retry stats */
        pthread_mutex_lock(&priv->mutex);
        priv->stats.num_retries++;
        priv->stats.retry_delay += retry_pause;
        pthread_mutex_unlock(&priv->mutex);
    }

    /* Give up */
    (*config->log)(LOG_ERR, "giving up on: %s %s", io->method, io->url);
    return EIO;
}

/*
 * Compute S3 authorization hash using secret access key.
 */
static void
http_io_get_auth(char *buf, size_t bufsiz, const char *accessKey, const char *method,
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
    http_io_base64_encode(buf, bufsiz, digest, digest_len);
}

/*
 * Create URL for a block, and return pointer to the URL's path.
 */
static char *
http_io_get_url(char *buf, size_t bufsiz, const char *baseURL, const char *bucket, const char *prefix, s3b_block_t block_num)
{
    snprintf(buf, bufsiz, "%s%s/%s%0*x", baseURL, bucket, prefix, S3B_BLOCK_NUM_DIGITS, block_num);
    return buf + strlen(baseURL) - 1;
}

/*
 * Get HTTP Date header value based on current time.
 */
static void
http_io_get_date(char *buf, size_t bufsiz)
{
    time_t now = time(NULL);

    strftime(buf, bufsiz, DATE_BUF_FMT, gmtime(&now));
}

static struct curl_slist *
http_io_add_header(struct curl_slist *headers, const char *fmt, ...)
{
    char buf[1024];
    va_list args;

    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    headers = curl_slist_append(headers, buf);
    va_end(args);
    return headers;
}

static CURL *
http_io_acquire_curl(struct http_io_private *priv, struct http_io *io)
{
    struct http_io_conf *const config = priv->config;
    struct curl_holder *holder;
    CURL *curl;

    pthread_mutex_lock(&priv->mutex);
    if ((holder = LIST_FIRST(&priv->curls)) != NULL) {
        curl = holder->curl;
        LIST_REMOVE(holder, link);
        priv->stats.curl_handles_reused++;
        pthread_mutex_unlock(&priv->mutex);
        free(holder);
        curl_easy_reset(curl);
    } else {
        priv->stats.curl_handles_created++;             // optimistic
        pthread_mutex_unlock(&priv->mutex);
        if ((curl = curl_easy_init()) == NULL) {
            pthread_mutex_lock(&priv->mutex);
            priv->stats.curl_handles_created--;         // undo optimistic
            priv->stats.curl_other_error++;
            pthread_mutex_unlock(&priv->mutex);
            (*config->log)(LOG_ERR, "curl_easy_init() failed");
            return NULL;
        }
    }
    curl_easy_setopt(curl, CURLOPT_URL, io->url);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, (long)1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config->timeout);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, config->user_agent);
    if (strncmp(io->url, "https", 5) == 0) {
        if (config->insecure)
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, (long)0);
        if (config->cacert != NULL)
            curl_easy_setopt(curl, CURLOPT_CAINFO, config->cacert);
    }
    //curl_easy_setopt(curl, CURLOPT_VERBOSE);
    return curl;
}

static size_t
http_io_curl_reader(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io_bufs *const bufs = (struct http_io_bufs *)stream;
    size_t total = size * nmemb;

    if (total > bufs->rdremain)     /* should never happen */
        total = bufs->rdremain;
    memcpy(bufs->rddata, ptr, total);
    bufs->rddata += total;
    bufs->rdremain -= total;
    return total;
}

static size_t
http_io_curl_writer(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io_bufs *const bufs = (struct http_io_bufs *)stream;
    size_t total = size * nmemb;

    if (total > bufs->wrremain)     /* should never happen */
        total = bufs->wrremain;
    memcpy(ptr, bufs->wrdata, total);
    bufs->wrdata += total;
    bufs->wrremain -= total;
    return total;
}

static size_t
http_io_curl_header(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io *const io = (struct http_io *)stream;
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
http_io_release_curl(struct http_io_private *priv, CURL *curl, int may_cache)
{
    struct curl_holder *holder;

    if (!may_cache) {
        curl_easy_cleanup(curl);
        return;
    }
    if ((holder = calloc(1, sizeof(*holder))) == NULL) {
        curl_easy_cleanup(curl);
        pthread_mutex_lock(&priv->mutex);
        priv->stats.out_of_memory_errors++;
        pthread_mutex_unlock(&priv->mutex);
        return;
    }
    holder->curl = curl;
    pthread_mutex_lock(&priv->mutex);
    LIST_INSERT_HEAD(&priv->curls, holder, link);
    pthread_mutex_unlock(&priv->mutex);
}

static void
http_io_openssl_locker(int mode, int i, const char *file, int line)
{
    if ((mode & CRYPTO_LOCK) != 0)
        pthread_mutex_lock(&openssl_locks[i]);
    else
        pthread_mutex_unlock(&openssl_locks[i]);
}

static unsigned long
http_io_openssl_ider(void)
{
    return (unsigned long)pthread_self();
}

static void
http_io_base64_encode(char *buf, size_t bufsiz, const void *data, size_t len)
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

