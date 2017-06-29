
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 * 
 * Copyright 2008-2011 Archie L. Cobbs <archie@dellroad.org>
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
#include "block_cache.h"
#include "ec_protect.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"

/****************************************************************************
 *                          DEFINITIONS                                     *
 ****************************************************************************/

/* S3 URL */
#define S3_DOMAIN                                   "amazonaws.com"

/* S3 access permission strings */
#define S3_ACCESS_PRIVATE                           "private"
#define S3_ACCESS_PUBLIC_READ                       "public-read"
#define S3_ACCESS_PUBLIC_READ_WRITE                 "public-read-write"
#define S3_ACCESS_AUTHENTICATED_READ                "authenticated-read"

/* Default values for some configuration parameters */
#define S3BACKER_DEFAULT_ACCESS_TYPE                S3_ACCESS_PRIVATE
#define S3BACKER_DEFAULT_AUTH_VERSION               AUTH_VERSION_AWS4
#define S3BACKER_DEFAULT_REGION                     "us-east-1"
#define S3BACKER_DEFAULT_PWD_FILE                   ".s3backer_passwd"
#define S3BACKER_DEFAULT_PREFIX                     ""
#define S3BACKER_DEFAULT_FILENAME                   "file"
#define S3BACKER_DEFAULT_STATS_FILENAME             "stats"
#define S3BACKER_DEFAULT_BLOCKSIZE                  4096
#define S3BACKER_DEFAULT_TIMEOUT                    30              // 30s
#define S3BACKER_DEFAULT_FILE_MODE                  0600
#define S3BACKER_DEFAULT_FILE_MODE_READ_ONLY        0400
#define S3BACKER_DEFAULT_INITIAL_RETRY_PAUSE        200             // 200ms
#define S3BACKER_DEFAULT_MAX_RETRY_PAUSE            30000           // 30s
#define S3BACKER_DEFAULT_MIN_WRITE_DELAY            500             // 500ms
#define S3BACKER_DEFAULT_MD5_CACHE_TIME             10000           // 10s
#define S3BACKER_DEFAULT_MD5_CACHE_SIZE             10000
#define S3BACKER_DEFAULT_BLOCK_CACHE_SIZE           1000
#define S3BACKER_DEFAULT_BLOCK_CACHE_NUM_THREADS    20
#define S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY    250             // 250ms
#define S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT        0
#define S3BACKER_DEFAULT_BLOCK_CACHE_MAX_DIRTY      0
#define S3BACKER_DEFAULT_READ_AHEAD                 4
#define S3BACKER_DEFAULT_READ_AHEAD_TRIGGER         2
#define S3BACKER_DEFAULT_COMPRESSION                Z_NO_COMPRESSION
#define S3BACKER_DEFAULT_ENCRYPTION                 "AES-128-CBC"

#define S3BACKER_SSE_HEADER                         "x-amz-server-side-encryption"
#define S3BACKER_DEFAULT_SSE_VALUE                  "AES256"

/* MacFUSE setting for kernel daemon timeout */
#ifdef __APPLE__
#ifndef FUSE_MAX_DAEMON_TIMEOUT
#define FUSE_MAX_DAEMON_TIMEOUT         600
#endif
#define s3bquote0(x)                    #x
#define s3bquote(x)                     s3bquote0(x)
#define FUSE_MAX_DAEMON_TIMEOUT_STRING  s3bquote(FUSE_MAX_DAEMON_TIMEOUT)
#endif  /* __APPLE__ */

/* Block counting info */
struct list_blocks {
    u_int       *bitmap;
    int         print_dots;
    uintmax_t   count;
};
#define BLOCKS_PER_DOT                  0x100

/****************************************************************************
 *                          FUNCTION DECLARATIONS                           *
 ****************************************************************************/

static print_stats_t s3b_config_print_stats;

static int parse_size_string(const char *s, uintmax_t *valp);
static void unparse_size_string(char *buf, size_t bmax, uintmax_t value);
static int search_access_for(const char *file, const char *accessId, char **idptr, char **pwptr);
static int handle_unknown_option(void *data, const char *arg, int key, struct fuse_args *outargs);
static void syslog_logger(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
static void stderr_logger(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
static int validate_config(void);
static void list_blocks_callback(void *arg, s3b_block_t block_num);
static void dump_config(void);
static void usage(void);

/****************************************************************************
 *                          VARIABLE DEFINITIONS                            *
 ****************************************************************************/

/* Upload/download strings */
static const char *const upload_download_names[] = { "download", "upload" };

/* Valid S3 access values */
static const char *const s3_acls[] = {
    S3_ACCESS_PRIVATE,
    S3_ACCESS_PUBLIC_READ,
    S3_ACCESS_PUBLIC_READ_WRITE,
    S3_ACCESS_AUTHENTICATED_READ
};

/* Valid S3 authentication types */
static const char *const s3_auth_types[] = {
    AUTH_VERSION_AWS2,
    AUTH_VERSION_AWS4,
};

/* Configuration structure */
static char user_agent_buf[64];
static struct s3b_config config = {

    /* HTTP config */
    .http_io= {
        .accessId=              NULL,
        .accessKey=             NULL,
        .baseURL=               NULL,
        .region=                NULL,
        .bucket=                NULL,
        .prefix=                S3BACKER_DEFAULT_PREFIX,
        .accessType=            S3BACKER_DEFAULT_ACCESS_TYPE,
        .authVersion=           S3BACKER_DEFAULT_AUTH_VERSION,
        .user_agent=            user_agent_buf,
        .compress=              S3BACKER_DEFAULT_COMPRESSION,
        .timeout=               S3BACKER_DEFAULT_TIMEOUT,
        .initial_retry_pause=   S3BACKER_DEFAULT_INITIAL_RETRY_PAUSE,
        .max_retry_pause=       S3BACKER_DEFAULT_MAX_RETRY_PAUSE,
        .sse =                  NULL
    },

    /* "Eventual consistency" protection config */
    .ec_protect= {
        .min_write_delay=       S3BACKER_DEFAULT_MIN_WRITE_DELAY,
        .cache_time=            S3BACKER_DEFAULT_MD5_CACHE_TIME,
        .cache_size=            S3BACKER_DEFAULT_MD5_CACHE_SIZE,
    },

    /* Block cache config */
    .block_cache= {
        .cache_size=            S3BACKER_DEFAULT_BLOCK_CACHE_SIZE,
        .num_threads=           S3BACKER_DEFAULT_BLOCK_CACHE_NUM_THREADS,
        .write_delay=           S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY,
        .max_dirty=             S3BACKER_DEFAULT_BLOCK_CACHE_MAX_DIRTY,
        .timeout=               S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT,
        .read_ahead=            S3BACKER_DEFAULT_READ_AHEAD,
        .read_ahead_trigger=    S3BACKER_DEFAULT_READ_AHEAD_TRIGGER,
    },

    /* FUSE operations config */
    .fuse_ops= {
        .filename=              S3BACKER_DEFAULT_FILENAME,
        .stats_filename=        S3BACKER_DEFAULT_STATS_FILENAME,
        .file_mode=             -1,             /* default depends on 'read_only' */
    },

    /* Common stuff */
    .block_size=            0,
    .file_size=             0,
    .quiet=                 0,
    .erase=                 0,
    .no_auto_detect=        0,
    .reset=                 0,
    .log=                   syslog_logger
};

/*
 * Command line flags
 *
 * Note: each entry here is listed twice, so both version "--foo=X" and "-o foo=X" work.
 * See http://code.google.com/p/s3backer/issues/detail?id=7
 */
static const struct fuse_opt option_list[] = {
    {
        .templ=     "--accessFile=%s",
        .offset=    offsetof(struct s3b_config, accessFile),
    },
    {
        .templ=     "--accessId=%s",
        .offset=    offsetof(struct s3b_config, http_io.accessId),
    },
    {
        .templ=     "--accessKey=%s",
        .offset=    offsetof(struct s3b_config, http_io.accessKey),
    },
    {
        .templ=     "--accessType=%s",
        .offset=    offsetof(struct s3b_config, http_io.accessType),
    },
    {
        .templ=     "--accessEC2IAM=%s",
        .offset=    offsetof(struct s3b_config, http_io.ec2iam_role),
    },
    {
        .templ=     "--authVersion=%s",
        .offset=    offsetof(struct s3b_config, http_io.authVersion),
    },
    {
        .templ=     "--listBlocks",
        .offset=    offsetof(struct s3b_config, list_blocks),
        .value=     1
    },
    {
        .templ=     "--baseURL=%s",
        .offset=    offsetof(struct s3b_config, http_io.baseURL),
    },
    {
        .templ=     "--region=%s",
        .offset=    offsetof(struct s3b_config, http_io.region),
    },
    {
            .templ=     "--sse=%s",
            .offset=    offsetof(struct s3b_config, http_io.sse),
    },
    {
        .templ=     "--blockCacheSize=%u",
        .offset=    offsetof(struct s3b_config, block_cache.cache_size),
    },
    {
        .templ=     "--blockCacheSync",
        .offset=    offsetof(struct s3b_config, block_cache.synchronous),
        .value=     1
    },
    {
        .templ=     "--blockCacheThreads=%u",
        .offset=    offsetof(struct s3b_config, block_cache.num_threads),
    },
    {
        .templ=     "--blockCacheTimeout=%u",
        .offset=    offsetof(struct s3b_config, block_cache.timeout),
    },
    {
        .templ=     "--blockCacheWriteDelay=%u",
        .offset=    offsetof(struct s3b_config, block_cache.write_delay),
    },
    {
        .templ=     "--blockCacheMaxDirty=%u",
        .offset=    offsetof(struct s3b_config, block_cache.max_dirty),
    },
    {
        .templ=     "--readAhead=%u",
        .offset=    offsetof(struct s3b_config, block_cache.read_ahead),
    },
    {
        .templ=     "--readAheadTrigger=%u",
        .offset=    offsetof(struct s3b_config, block_cache.read_ahead_trigger),
    },
    {
        .templ=     "--blockCacheFile=%s",
        .offset=    offsetof(struct s3b_config, block_cache.cache_file),
    },
    {
        .templ=     "--blockCacheNoVerify",
        .offset=    offsetof(struct s3b_config, block_cache.no_verify),
        .value=     1
    },
    {
        .templ=     "--blockSize=%s",
        .offset=    offsetof(struct s3b_config, block_size_str),
    },
    {
        .templ=     "--maxUploadSpeed=%s",
        .offset=    offsetof(struct s3b_config, max_speed_str[HTTP_UPLOAD]),
    },
    {
        .templ=     "--maxDownloadSpeed=%s",
        .offset=    offsetof(struct s3b_config, max_speed_str[HTTP_DOWNLOAD]),
    },
    {
        .templ=     "--md5CacheSize=%u",
        .offset=    offsetof(struct s3b_config, ec_protect.cache_size),
    },
    {
        .templ=     "--md5CacheTime=%u",
        .offset=    offsetof(struct s3b_config, ec_protect.cache_time),
    },
    {
        .templ=     "--debug",
        .offset=    offsetof(struct s3b_config, debug),
        .value=     1
    },
    {
        .templ=     "--debug-http",
        .offset=    offsetof(struct s3b_config, http_io.debug_http),
        .value=     1
    },
    {
        .templ=     "--quiet",
        .offset=    offsetof(struct s3b_config, quiet),
        .value=     1
    },
    {
        .templ=     "--erase",
        .offset=    offsetof(struct s3b_config, erase),
        .value=     1
    },
    {
        .templ=     "--reset-mounted-flag",
        .offset=    offsetof(struct s3b_config, reset),
        .value=     1
    },
    {
        .templ=     "--vhost",
        .offset=    offsetof(struct s3b_config, http_io.vhost),
        .value=     1
    },
    {
        .templ=     "--fileMode=%o",
        .offset=    offsetof(struct s3b_config, fuse_ops.file_mode),
    },
    {
        .templ=     "--filename=%s",
        .offset=    offsetof(struct s3b_config, fuse_ops.filename),
    },
    {
        .templ=     "--force",
        .offset=    offsetof(struct s3b_config, force),
        .value=     1
    },
    {
        .templ=     "--noAutoDetect",
        .offset=    offsetof(struct s3b_config, no_auto_detect),
        .value=     1
    },
    {
        .templ=     "--initialRetryPause=%u",
        .offset=    offsetof(struct s3b_config, http_io.initial_retry_pause),
    },
    {
        .templ=     "--maxRetryPause=%u",
        .offset=    offsetof(struct s3b_config, http_io.max_retry_pause),
    },
    {
        .templ=     "--minWriteDelay=%u",
        .offset=    offsetof(struct s3b_config, ec_protect.min_write_delay),
    },
    {
        .templ=     "--prefix=%s",
        .offset=    offsetof(struct s3b_config, http_io.prefix),
    },
    {
        .templ=     "--defaultContentEncoding=%s",
        .offset=    offsetof(struct s3b_config, http_io.default_ce),
    },
    {
        .templ=     "--readOnly",
        .offset=    offsetof(struct s3b_config, fuse_ops.read_only),
        .value=     1
    },
    {
        .templ=     "--size=%s",
        .offset=    offsetof(struct s3b_config, file_size_str),
    },
    {
        .templ=     "--statsFilename=%s",
        .offset=    offsetof(struct s3b_config, fuse_ops.stats_filename),
    },
    {
        .templ=     "--rrs",
        .offset=    offsetof(struct s3b_config, http_io.rrs),
        .value=     1
    },
    {
        .templ=     "--storageClass=%s",
        .offset=    offsetof(struct s3b_config, http_io.storage_class),
    },
    {
        .templ=     "--ssl",
        .offset=    offsetof(struct s3b_config, ssl),
        .value=     1
    },
    {
        .templ=     "--cacert=%s",
        .offset=    offsetof(struct s3b_config, http_io.cacert),
    },
    {
        .templ=     "--insecure",
        .offset=    offsetof(struct s3b_config, http_io.insecure),
        .value=     1
    },
    {
        .templ=     "--compress",
        .offset=    offsetof(struct s3b_config, http_io.compress),
        .value=     Z_DEFAULT_COMPRESSION
    },
    {
        .templ=     "--compress=%d",
        .offset=    offsetof(struct s3b_config, http_io.compress),
    },
    {
        .templ=     "--encrypt",
        .offset=    offsetof(struct s3b_config, encrypt),
        .value=     1
    },
    {
        .templ=     "--encrypt=%s",
        .offset=    offsetof(struct s3b_config, http_io.encryption),
    },
    {
        .templ=     "--keyLength=%u",
        .offset=    offsetof(struct s3b_config, http_io.key_length),
    },
    {
        .templ=     "--password=%s",
        .offset=    offsetof(struct s3b_config, http_io.password),
    },
    {
        .templ=     "--passwordFile=%s",
        .offset=    offsetof(struct s3b_config, password_file),
    },
    {
        .templ=     "--test",
        .offset=    offsetof(struct s3b_config, test),
        .value=     1
    },
    {
        .templ=     "--timeout=%u",
        .offset=    offsetof(struct s3b_config, http_io.timeout),
    },
    {
        .templ=     "--directIO",
        .offset=    offsetof(struct s3b_config, fuse_ops.direct_io),
        .value=     1
    },
};

/* Default flags we send to FUSE */
static const char *const s3backer_fuse_defaults[] = {
    "-okernel_cache",
    "-oallow_other",
    "-ouse_ino",
    "-omax_readahead=0",
    "-osubtype=s3backer",
    "-oentry_timeout=31536000",
    "-onegative_timeout=31536000",
    "-oattr_timeout=0",             // because statistics file length changes
    "-odefault_permissions",
#ifndef __FreeBSD__
    "-onodev",
#endif
    "-onosuid",
#ifdef __APPLE__
    "-odaemon_timeout=" FUSE_MAX_DAEMON_TIMEOUT_STRING,
#endif
/*  "-ointr", */
};

/* Size suffixes */
struct size_suffix {
    const char  *suffix;
    int         bits;
};
static const struct size_suffix size_suffixes[] = {
    {
        .suffix=    "k",
        .bits=      10
    },
    {
        .suffix=    "m",
        .bits=      20
    },
    {
        .suffix=    "g",
        .bits=      30
    },
    {
        .suffix=    "t",
        .bits=      40
    },
    {
        .suffix=    "p",
        .bits=      50
    },
    {
        .suffix=    "e",
        .bits=      60
    },
    {
        .suffix=    "z",
        .bits=      70
    },
    {
        .suffix=    "y",
        .bits=      80
    },
};

/* s3backer_store layers */
struct s3backer_store *block_cache_store;
struct s3backer_store *ec_protect_store;
struct s3backer_store *http_io_store;
struct s3backer_store *test_io_store;

/****************************************************************************
 *                      PUBLIC FUNCTION DEFINITIONS                         *
 ****************************************************************************/

struct s3b_config *
s3backer_get_config(int argc, char **argv)
{
    const int num_options = sizeof(option_list) / sizeof(*option_list);
    struct fuse_opt dup_option_list[2 * sizeof(option_list) + 1];
    char buf[1024];
    int i;

    /* Remember user creds */
    config.fuse_ops.uid = getuid();
    config.fuse_ops.gid = getgid();

    /* Set user-agent */
    snprintf(user_agent_buf, sizeof(user_agent_buf), "%s/%s/%s", PACKAGE, VERSION, s3backer_version);

    /* Copy passed args */
    memset(&config.fuse_args, 0, sizeof(config.fuse_args));
    for (i = 0; i < argc; i++) {
        if (fuse_opt_insert_arg(&config.fuse_args, i, argv[i]) != 0)
            err(1, "fuse_opt_insert_arg");
    }

    /* Insert our default FUSE options */
    for (i = 0; i < sizeof(s3backer_fuse_defaults) / sizeof(*s3backer_fuse_defaults); i++) {
        if (fuse_opt_insert_arg(&config.fuse_args, i + 1, s3backer_fuse_defaults[i]) != 0)
            err(1, "fuse_opt_insert_arg");
    }

    /* Create the equivalent fstab options (without the "--") for each option in the option list */
    memcpy(dup_option_list, option_list, sizeof(option_list));
    memcpy(dup_option_list + num_options, option_list, sizeof(option_list));
    for (i = num_options; i < 2 * num_options; i++)
        dup_option_list[i].templ += 2;
    dup_option_list[2 * num_options].templ = NULL;

    /* Parse command line flags */
    if (fuse_opt_parse(&config.fuse_args, &config, dup_option_list, handle_unknown_option) != 0)
        return NULL;

    /* Validate configuration */
    if (validate_config() != 0)
        return NULL;

    /* Set fsname based on configuration */
    snprintf(buf, sizeof(buf), "-ofsname=%s", config.description);
    if (fuse_opt_insert_arg(&config.fuse_args, 1, buf) != 0)
        err(1, "fuse_opt_insert_arg");

    /* Set up fuse_ops callbacks */
    config.fuse_ops.print_stats = s3b_config_print_stats;
    config.fuse_ops.s3bconf = &config;

    /* Debug */
    if (config.debug)
        dump_config();

    /* Done */
    return &config;
}

/*
 * Create the s3backer_store used at runtime. This method is invoked by fuse_op_init().
 */
struct s3backer_store *
s3backer_create_store(struct s3b_config *conf)
{
    struct s3backer_store *store;
    int mounted;
    int r;

    /* Sanity check */
    if (http_io_store != NULL || test_io_store != NULL) {
        errno = EINVAL;
        return NULL;
    }

    /* Create HTTP (or test) layer */
    if (conf->test) {
        if ((test_io_store = test_io_create(&conf->http_io)) == NULL)
            return NULL;
        store = test_io_store;
    } else {
        if ((http_io_store = http_io_create(&conf->http_io)) == NULL)
            return NULL;
        store = http_io_store;
    }

    /* Create eventual consistency protection layer (if desired) */
    if (conf->ec_protect.cache_size > 0) {
        if ((ec_protect_store = ec_protect_create(&conf->ec_protect, store)) == NULL) 
            goto fail_with_errno;
        store = ec_protect_store;
    }

    /* Create block cache layer (if desired) */
    if (conf->block_cache.cache_size > 0) {
        if ((block_cache_store = block_cache_create(&conf->block_cache, store)) == NULL)
            goto fail_with_errno;
        store = block_cache_store;
    }

    /* Set mounted flag and check previous value one last time */
    r = (*store->set_mounted)(store, &mounted, conf->fuse_ops.read_only ? -1 : 1);
    if (r != 0) {
        (*conf->log)(LOG_ERR, "error reading mounted flag on %s: %s", conf->description, strerror(r));
        goto fail;
    }
    if (mounted) {
        if (!conf->force) {
            (*conf->log)(LOG_ERR, "%s appears to be mounted by another s3backer process", config.description);
            r = EBUSY;
            goto fail;
        }
    }

    /* Done */
    return store;

fail_with_errno:
    r = errno;
fail:
    if (store != NULL)
        (*store->destroy)(store);
    block_cache_store = NULL;
    ec_protect_store = NULL;
    http_io_store = NULL;
    test_io_store = NULL;
    errno = r;
    return NULL;
}

/****************************************************************************
 *                    INTERNAL FUNCTION DEFINITIONS                         *
 ****************************************************************************/

static void
s3b_config_print_stats(void *prarg, printer_t *printer)
{
    struct http_io_stats http_io_stats;
    struct ec_protect_stats ec_protect_stats;
    struct block_cache_stats block_cache_stats;
    double curl_reuse_ratio = 0.0;
    u_int total_oom = 0;
    u_int total_curls;

    /* Get HTTP stats */
    if (http_io_store != NULL)
        http_io_get_stats(http_io_store, &http_io_stats);

    /* Get EC protection stats */
    if (ec_protect_store != NULL)
        ec_protect_get_stats(ec_protect_store, &ec_protect_stats);

    /* Get block cache stats */
    if (block_cache_store != NULL)
        block_cache_get_stats(block_cache_store, &block_cache_stats);

    /* Print stats in human-readable form */
    if (http_io_store != NULL) {
        (*printer)(prarg, "%-28s %u\n", "http_normal_blocks_read", http_io_stats.normal_blocks_read);
        (*printer)(prarg, "%-28s %u\n", "http_normal_blocks_written", http_io_stats.normal_blocks_written);
        (*printer)(prarg, "%-28s %u\n", "http_zero_blocks_read", http_io_stats.zero_blocks_read);
        (*printer)(prarg, "%-28s %u\n", "http_zero_blocks_written", http_io_stats.zero_blocks_written);
        if (config.list_blocks) {
            (*printer)(prarg, "%-28s %u\n", "http_empty_blocks_read", http_io_stats.empty_blocks_read);
            (*printer)(prarg, "%-28s %u\n", "http_empty_blocks_written", http_io_stats.empty_blocks_written);
        }
        (*printer)(prarg, "%-28s %u\n", "http_gets", http_io_stats.http_gets.count);
        (*printer)(prarg, "%-28s %u\n", "http_puts", http_io_stats.http_puts.count);
        (*printer)(prarg, "%-28s %u\n", "http_deletes", http_io_stats.http_deletes.count);
        (*printer)(prarg, "%-28s %.3f sec\n", "http_avg_get_time", http_io_stats.http_gets.count > 0 ?
          http_io_stats.http_gets.time / http_io_stats.http_gets.count : 0.0);
        (*printer)(prarg, "%-28s %.3f sec\n", "http_avg_put_time", http_io_stats.http_puts.count > 0 ?
          http_io_stats.http_puts.time / http_io_stats.http_puts.count : 0.0);
        (*printer)(prarg, "%-28s %.3f sec\n", "http_avg_delete_time", http_io_stats.http_deletes.count > 0 ?
          http_io_stats.http_deletes.time / http_io_stats.http_deletes.count : 0.0);
        (*printer)(prarg, "%-28s %u\n", "http_unauthorized", http_io_stats.http_unauthorized);
        (*printer)(prarg, "%-28s %u\n", "http_forbidden", http_io_stats.http_forbidden);
        (*printer)(prarg, "%-28s %u\n", "http_stale", http_io_stats.http_stale);
        (*printer)(prarg, "%-28s %u\n", "http_verified", http_io_stats.http_verified);
        (*printer)(prarg, "%-28s %u\n", "http_mismatch", http_io_stats.http_mismatch);
        (*printer)(prarg, "%-28s %u\n", "http_5xx_error", http_io_stats.http_5xx_error);
        (*printer)(prarg, "%-28s %u\n", "http_4xx_error", http_io_stats.http_4xx_error);
        (*printer)(prarg, "%-28s %u\n", "http_other_error", http_io_stats.http_other_error);
        (*printer)(prarg, "%-28s %u\n", "http_canceled_writes", http_io_stats.http_canceled_writes);
        (*printer)(prarg, "%-28s %u\n", "http_num_retries", http_io_stats.num_retries);
        (*printer)(prarg, "%-28s %ju.%03u sec\n", "http_total_retry_delay",
          (uintmax_t)(http_io_stats.retry_delay / 1000), (u_int)(http_io_stats.retry_delay % 1000));
        total_curls = http_io_stats.curl_handles_created + http_io_stats.curl_handles_reused;
        if (total_curls > 0)
            curl_reuse_ratio = (double)http_io_stats.curl_handles_reused / (double)total_curls;
        (*printer)(prarg, "%-28s %.4f\n", "curl_handle_reuse_ratio", curl_reuse_ratio);
        (*printer)(prarg, "%-28s %u\n", "curl_timeouts", http_io_stats.curl_timeouts);
        (*printer)(prarg, "%-28s %u\n", "curl_connect_failed", http_io_stats.curl_connect_failed);
        (*printer)(prarg, "%-28s %u\n", "curl_host_unknown", http_io_stats.curl_host_unknown);
        (*printer)(prarg, "%-28s %u\n", "curl_out_of_memory", http_io_stats.curl_out_of_memory);
        (*printer)(prarg, "%-28s %u\n", "curl_other_error", http_io_stats.curl_other_error);
        total_oom += http_io_stats.out_of_memory_errors;
    }
    if (block_cache_store != NULL) {
        double read_hit_ratio = 0.0;
        double write_hit_ratio = 0.0;
        u_int total_reads;
        u_int total_writes;

        total_reads = block_cache_stats.read_hits + block_cache_stats.read_misses;
        if (total_reads != 0)
            read_hit_ratio = (double)block_cache_stats.read_hits / (double)total_reads;
        total_writes = block_cache_stats.write_hits + block_cache_stats.write_misses;
        if (total_writes != 0)
            write_hit_ratio = (double)block_cache_stats.write_hits / (double)total_writes;
        (*printer)(prarg, "%-28s %u blocks\n", "block_cache_current_size", block_cache_stats.current_size);
        (*printer)(prarg, "%-28s %u blocks\n", "block_cache_initial_size", block_cache_stats.initial_size);
        (*printer)(prarg, "%-28s %.4f\n", "block_cache_dirty_ratio", block_cache_stats.dirty_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_read_hits", block_cache_stats.read_hits);
        (*printer)(prarg, "%-28s %u\n", "block_cache_read_misses", block_cache_stats.read_misses);
        (*printer)(prarg, "%-28s %.4f\n", "block_cache_read_hit_ratio", read_hit_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_write_hits", block_cache_stats.write_hits);
        (*printer)(prarg, "%-28s %u\n", "block_cache_write_misses", block_cache_stats.write_misses);
        (*printer)(prarg, "%-28s %.4f\n", "block_cache_write_hit_ratio", write_hit_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_verified", block_cache_stats.verified);
        (*printer)(prarg, "%-28s %u\n", "block_cache_mismatch", block_cache_stats.mismatch);
        total_oom += block_cache_stats.out_of_memory_errors;
    }
    if (ec_protect_store != NULL) {
        (*printer)(prarg, "%-28s %u blocks\n", "md5_cache_current_size", ec_protect_stats.current_cache_size);
        (*printer)(prarg, "%-28s %u\n", "md5_cache_data_hits", ec_protect_stats.cache_data_hits);
        (*printer)(prarg, "%-28s %ju.%03u sec\n", "md5_cache_full_delays",
          (uintmax_t)(ec_protect_stats.cache_full_delay / 1000), (u_int)(ec_protect_stats.cache_full_delay % 1000));
        (*printer)(prarg, "%-28s %ju.%03u sec\n", "md5_cache_write_delays",
          (uintmax_t)(ec_protect_stats.repeated_write_delay / 1000), (u_int)(ec_protect_stats.repeated_write_delay % 1000));
        total_oom += ec_protect_stats.out_of_memory_errors;
    }
    (*printer)(prarg, "%-28s %u\n", "out_of_memory_errors", total_oom);
}

static int
parse_size_string(const char *s, uintmax_t *valp)
{
    char suffix[3] = { '\0' };
    int nconv;

    nconv = sscanf(s, "%ju%2s", valp, suffix);
    if (nconv < 1)
        return -1;
    if (nconv >= 2) {
        int found = 0;
        int i;

        for (i = 0; i < sizeof(size_suffixes) / sizeof(*size_suffixes); i++) {
            const struct size_suffix *const ss = &size_suffixes[i];

            if (ss->bits >= sizeof(off_t) * 8)
                break;
            if (strcasecmp(suffix, ss->suffix) == 0) {
                *valp <<= ss->bits;
                found = 1;
                break;
            }
        }
        if (!found)
            return -1;
    }
    return 0;
}

static void
unparse_size_string(char *buf, size_t bmax, uintmax_t value)
{
    uintmax_t unit;
    int i;

    if (value == 0) {
        snprintf(buf, bmax, "0");
        return;
    }
    for (i = sizeof(size_suffixes) / sizeof(*size_suffixes); i-- > 0; ) {
        const struct size_suffix *const ss = &size_suffixes[i];

        if (ss->bits >= sizeof(off_t) * 8)
            continue;
        unit = (uintmax_t)1 << ss->bits;
        if (value % unit == 0) {
            snprintf(buf, bmax, "%ju%s", value / unit, ss->suffix);
            return;
        }
    }
    snprintf(buf, bmax, "%ju", value);
}

/**
 * Handle command-line flag.
 */
static int
handle_unknown_option(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    /* Check options */
    if (key == FUSE_OPT_KEY_OPT) {

        /* Debug flags */
        if (strcmp(arg, "-d") == 0)
            config.debug = 1;
        if (strcmp(arg, "-d") == 0 || strcmp(arg, "-f") == 0)
            config.log = stderr_logger;

        /* Version */
        if (strcmp(arg, "--version") == 0 || strcmp(arg, "-v") == 0) {
            fprintf(stderr, "%s version %s (%s)\n", PACKAGE, VERSION, s3backer_version);
            fprintf(stderr, "Copyright (C) 2008-2011 Archie L. Cobbs.\n");
            fprintf(stderr, "This is free software; see the source for copying conditions.  There is NO\n");
            fprintf(stderr, "warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n");
            exit(0);
        }

        /* Help */
        if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0 || strcmp(arg, "-?") == 0) {
            usage();
            exit(0);
        }

        /* Unknown; pass it through to fuse_main() */
        return 1;
    }

    /* Get bucket parameter */
    if (config.http_io.bucket == NULL) {
        if ((config.http_io.bucket = strdup(arg)) == NULL)
            err(1, "strdup");
        return 0;
    }

    /* Copy mount point */
    if (config.mount == NULL) {
        if ((config.mount = strdup(arg)) == NULL)
            err(1, "strdup");
        return 1;
    }

    /* Pass subsequent paramters on to fuse_main() */
    return 1;
}

static int
search_access_for(const char *file, const char *accessId, char **idptr, char **pwptr)
{
    char buf[1024];
    FILE *fp;

    if (idptr != NULL)
        *idptr = NULL;
    if (pwptr != NULL)
        *pwptr = NULL;
    if ((fp = fopen(file, "r")) == NULL)
        return 0;
    while (fgets(buf, sizeof(buf), fp) != NULL) {
        char *colon;

        if (*buf == '#' || *buf == '\0' || isspace(*buf) || (colon = strchr(buf, ':')) == NULL)
            continue;
        while (*buf != '\0' && isspace(buf[strlen(buf) - 1]))
            buf[strlen(buf) - 1] = '\0';
        *colon = '\0';
        if (accessId != NULL && strcmp(buf, accessId) != 0)
            continue;
        if (idptr != NULL && (*idptr = strdup(buf)) == NULL)
            err(1, "strdup");
        if (pwptr != NULL && (*pwptr = strdup(colon + 1)) == NULL)
            err(1, "strdup");
        fclose(fp);
        return 1;
    }
    fclose(fp);
    return 0;
}

static int
validate_config(void)
{
    struct s3backer_store *s3b;
    const int customBaseURL = config.http_io.baseURL != NULL;
    const int customRegion = config.http_io.region != NULL;
    off_t auto_file_size;
    u_int auto_block_size;
    uintmax_t value;
    const char *s;
    char blockSizeBuf[64];
    char fileSizeBuf[64];
    struct stat sb;
    char urlbuf[512];
    int i;
    int r;

    /* Default to $HOME/.s3backer for accessFile */
    if (config.http_io.ec2iam_role == NULL && config.accessFile == NULL) {
        const char *home = getenv("HOME");
        char buf[PATH_MAX];

        if (home != NULL) {
            snprintf(buf, sizeof(buf), "%s/%s", home, S3BACKER_DEFAULT_PWD_FILE);
            if ((config.accessFile = strdup(buf)) == NULL)
                err(1, "strdup");
        }
    }

    /* Auto-set file mode in read_only if not explicitly set */
    if (config.fuse_ops.file_mode == -1) {
        config.fuse_ops.file_mode = config.fuse_ops.read_only ?
          S3BACKER_DEFAULT_FILE_MODE_READ_ONLY : S3BACKER_DEFAULT_FILE_MODE;
    }

    /* If no accessId specified, default to first in accessFile */
    if (config.http_io.accessId == NULL && config.accessFile != NULL)
        search_access_for(config.accessFile, NULL, &config.http_io.accessId, NULL);
    if (config.http_io.accessId != NULL && *config.http_io.accessId == '\0')
        config.http_io.accessId = NULL;

    /* If no accessId, only read operations will succeed */
    if (config.http_io.accessId == NULL && !config.fuse_ops.read_only && !customBaseURL && config.http_io.ec2iam_role == NULL) {
        warnx("warning: no `accessId' specified; only read operations will succeed");
        warnx("you can eliminate this warning by providing the `--readOnly' flag");
    }

    /* Find key in file if not specified explicitly */
    if (config.http_io.accessId == NULL && config.http_io.accessKey != NULL) {
        warnx("an `accessKey' was specified but no `accessId' was specified");
        return -1;
    }
    if (config.http_io.accessId != NULL) {
        if (config.http_io.accessKey == NULL && config.accessFile != NULL)
            search_access_for(config.accessFile, config.http_io.accessId, NULL, &config.http_io.accessKey);
        if (config.http_io.accessKey == NULL) {
            warnx("no `accessKey' specified");
            return -1;
        }
    }

    /* Check for conflict between explicit accessId and EC2 IAM role */
    if (config.http_io.accessId != NULL && config.http_io.ec2iam_role != NULL) {
        warnx("an `accessKey' must not be specified when an `accessEC2IAM' role is specified");
        return -1;
    }

    /* Check auth version */
    for (i = 0; i < sizeof(s3_auth_types) / sizeof(*s3_auth_types); i++) {
        if (strcmp(config.http_io.authVersion, s3_auth_types[i]) == 0)
            break;
    }
    if (i == sizeof(s3_auth_types) / sizeof(*s3_auth_types)) {
        warnx("illegal authentication version `%s'", config.http_io.authVersion);
        return -1;
    }

    /* Check bucket/testdir */
    if (!config.test) {
        if (config.http_io.bucket == NULL) {
            warnx("no S3 bucket specified");
            return -1;
        }
        if (*config.http_io.bucket == '\0' || *config.http_io.bucket == '/' || strchr(config.http_io.bucket, '/') != 0) {
            warnx("invalid S3 bucket `%s'", config.http_io.bucket);
            return -1;
        }
    } else {
        if (config.http_io.bucket == NULL) {
            warnx("no test directory specified");
            return -1;
        }
        if (stat(config.http_io.bucket, &sb) == -1) {
            warn("%s", config.http_io.bucket);
            return -1;
        }
        if (!S_ISDIR(sb.st_mode)) {
            errno = ENOTDIR;
            warn("%s", config.http_io.bucket);
            return -1;
        }
    }

    /* Check storage class */
    if (config.http_io.storage_class != NULL
      && strcmp(config.http_io.storage_class, STORAGE_CLASS_STANDARD) != 0
      && strcmp(config.http_io.storage_class, STORAGE_CLASS_STANDARD_IA) != 0
      && strcmp(config.http_io.storage_class, STORAGE_CLASS_REDUCED_REDUNDANCY) != 0) {
        warnx("invalid storage class `%s'", config.http_io.storage_class);
        return -1;
    }

    /* Check storage class */
    if (config.http_io.sse != NULL
        && strcmp(config.http_io.sse, S3BACKER_DEFAULT_SSE_VALUE) != 0) {
        warnx("invalid sse type `%s'", config.http_io.sse);
        config.http_io.sse = S3BACKER_DEFAULT_SSE_VALUE;
    }

    /* Set default or custom region */
    if (config.http_io.region == NULL)
        config.http_io.region = S3BACKER_DEFAULT_REGION;
    if (customRegion)
        config.http_io.vhost = 1;

    /* Set default base URL */
    if (config.http_io.baseURL == NULL) {
        if (customRegion && strcmp(config.http_io.region, S3BACKER_DEFAULT_REGION) != 0)
            snprintf(urlbuf, sizeof(urlbuf), "http%s://s3-%s.%s/", config.ssl ? "s" : "", config.http_io.region, S3_DOMAIN);
        else
            snprintf(urlbuf, sizeof(urlbuf), "http%s://s3.%s/", config.ssl ? "s" : "", S3_DOMAIN);
        if ((config.http_io.baseURL = strdup(urlbuf)) == NULL) {
            warn("malloc");
            return -1;
        }
    }

    /* Check base URL */
    s = NULL;
    if (strncmp(config.http_io.baseURL, "http://", 7) == 0)
        s = config.http_io.baseURL + 7;
    else if (strncmp(config.http_io.baseURL, "https://", 8) == 0)
        s = config.http_io.baseURL + 8;
    if (s != NULL && (*s == '/' || *s == '\0'))
        s = NULL;
    if (s != NULL && (s = strrchr(s, '/')) == NULL) {
        warnx("base URL must end with a '/'");
        s = NULL;
    }
    if (s != NULL && s[1] != '\0') {
        warnx("base URL must end with a '/' not '%c'", s[1]);
        s = NULL;
    }
    if (s == NULL) {
        warnx("invalid base URL `%s'", config.http_io.baseURL);
        return -1;
    }
    if (config.ssl && customBaseURL && strncmp(config.http_io.baseURL, "https", 5) != 0) {
        warnx("non-SSL `--baseURL' conflicts with `--ssl'");
        return -1;
    }

    /* Handle virtual host style URL (prefix hostname with bucket name) */
    if (config.http_io.vhost) {
        size_t buflen;
        int schemelen;
        char *buf;

        schemelen = strchr(config.http_io.baseURL, ':') - config.http_io.baseURL + 3;
        buflen = strlen(config.http_io.bucket) + 1 + strlen(config.http_io.baseURL) + 1;
        if ((buf = malloc(buflen)) == NULL)
            err(1, "malloc(%u)", (u_int)buflen);
        snprintf(buf, buflen, "%.*s%s.%s", schemelen, config.http_io.baseURL,
          config.http_io.bucket, config.http_io.baseURL + schemelen);
        config.http_io.baseURL = buf;
    }

    /* Check S3 access privilege */
    for (i = 0; i < sizeof(s3_acls) / sizeof(*s3_acls); i++) {
        if (strcmp(config.http_io.accessType, s3_acls[i]) == 0)
            break;
    }
    if (i == sizeof(s3_acls) / sizeof(*s3_acls)) {
        warnx("illegal access type `%s'", config.http_io.accessType);
        return -1;
    }

    /* Check filenames */
    if (strchr(config.fuse_ops.filename, '/') != NULL || *config.fuse_ops.filename == '\0') {
        warnx("illegal filename `%s'", config.fuse_ops.filename);
        return -1;
    }
    if (strchr(config.fuse_ops.stats_filename, '/') != NULL) {
        warnx("illegal stats filename `%s'", config.fuse_ops.stats_filename);
        return -1;
    }

    /* Apply default encryption */
    if (config.http_io.encryption == NULL && config.encrypt)
        config.http_io.encryption = strdup(S3BACKER_DEFAULT_ENCRYPTION);

    /* Uppercase encryption name for consistency */
    if (config.http_io.encryption != NULL) {
        char *t;

        if ((t = strdup(config.http_io.encryption)) == NULL)
            err(1, "strdup()");
        for (i = 0; t[i] != '\0'; i++)
            t[i] = toupper(t[i]);
        config.http_io.encryption = t;
    }

    /* Check encryption and get key */
    if (config.http_io.encryption != NULL) {
        char pwbuf[1024];
        FILE *fp;

        if (config.password_file != NULL && config.http_io.password != NULL) {
            warnx("specify only one of `--password' or `--passwordFile'");
            return -1;
        }
        if (config.password_file == NULL && config.http_io.password == NULL) {
            if ((s = getpass("Password: ")) == NULL)
                err(1, "getpass()");
        }
        if (config.password_file != NULL) {
            assert(config.http_io.password == NULL);
            if ((fp = fopen(config.password_file, "r")) == NULL) {
                warn("can't open encryption key file `%s'", config.password_file);
                return -1;
            }
            if (fgets(pwbuf, sizeof(pwbuf), fp) == NULL || *pwbuf == '\0') {
                warnx("can't read encryption key from file `%s'", config.password_file);
                fclose(fp);
                return -1;
            }
            if (pwbuf[strlen(pwbuf) - 1] == '\n')
                pwbuf[strlen(pwbuf) - 1] = '\0';
            fclose(fp);
            s = pwbuf;
        }
        if (config.http_io.password == NULL && (config.http_io.password = strdup(s)) == NULL)
            err(1, "strdup()");
        if (config.http_io.key_length > EVP_MAX_KEY_LENGTH) {
            warnx("`--keyLength' value must be positive and at most %u", EVP_MAX_KEY_LENGTH);
            return -1;
        }
    } else {
        if (config.http_io.password != NULL)
            warnx("unexpected flag `%s' (`--encrypt' was not specified)", "--password");
        else if (config.password_file != NULL)
            warnx("unexpected flag `%s' (`--encrypt' was not specified)", "--passwordFile");
        if (config.http_io.key_length != 0)
            warnx("unexpected flag `%s' (`--encrypt' was not specified)", "--keyLength");
    }

    /* We always want to compress if we are encrypting */
    if (config.http_io.encryption != NULL && config.http_io.compress == Z_NO_COMPRESSION)
        config.http_io.compress = Z_DEFAULT_COMPRESSION;

    /* Check compression level */
    switch (config.http_io.compress) {
    case Z_DEFAULT_COMPRESSION:
    case Z_NO_COMPRESSION:
        break;
    default:
        if (config.http_io.compress < Z_BEST_SPEED || config.http_io.compress > Z_BEST_COMPRESSION) {
            warnx("illegal compression level `%d'", config.http_io.compress);
            return -1;
        }
        break;
    }

    /* Disable md5 cache when in read only mode */
    if (config.fuse_ops.read_only) {
        config.ec_protect.cache_size = 0;
        config.ec_protect.cache_time = 0;
        config.ec_protect.min_write_delay = 0;
    }

    /* Check time/cache values */
    if (config.ec_protect.cache_size == 0 && config.ec_protect.cache_time > 0) {
        warnx("`md5CacheTime' must zero when MD5 cache is disabled");
        return -1;
    }
    if (config.ec_protect.cache_size == 0 && config.ec_protect.min_write_delay > 0) {
        warnx("`minWriteDelay' must zero when MD5 cache is disabled");
        return -1;
    }
    if (config.ec_protect.cache_time > 0
      && config.ec_protect.cache_time < config.ec_protect.min_write_delay) {
        warnx("`md5CacheTime' must be at least `minWriteDelay'");
        return -1;
    }
    if (config.http_io.initial_retry_pause > config.http_io.max_retry_pause) {
        warnx("`maxRetryPause' must be at least `initialRetryPause'");
        return -1;
    }

    /* Parse block and file sizes */
    if (config.block_size_str != NULL) {
        if (parse_size_string(config.block_size_str, &value) == -1 || value == 0) {
            warnx("invalid block size `%s'", config.block_size_str);
            return -1;
        }
        if ((u_int)value != value) {
            warnx("block size `%s' is too big", config.block_size_str);
            return -1;
        }
        config.block_size = value;
    }
    if (config.file_size_str != NULL) {
        if (parse_size_string(config.file_size_str, &value) == -1 || value == 0) {
            warnx("invalid file size `%s'", config.block_size_str);
            return -1;
        }
        config.file_size = value;
    }

    /* Parse upload/download speeds */
    for (i = 0; i < 2; i++) {
        if (config.max_speed_str[i] != NULL) {
            if (parse_size_string(config.max_speed_str[i], &value) == -1 || value == 0) {
                warnx("invalid max %s speed `%s'", upload_download_names[i], config.max_speed_str[i]);
                return -1;
            }
            if ((curl_off_t)(value / 8) != (value / 8)) {
                warnx("max %s speed `%s' is too big", upload_download_names[i], config.max_speed_str[i]);
                return -1;
            }
            config.http_io.max_speed[i] = value;
        }
        if (config.http_io.max_speed[i] != 0 && config.block_size / (config.http_io.max_speed[i] / 8) >= config.http_io.timeout) {
            warnx("configured timeout of %us is too short for block size of %u bytes and max %s speed %s bps",
              config.http_io.timeout, config.block_size, upload_download_names[i], config.max_speed_str[i]);
            return -1;
        }
    }

    /* Check block cache config */
    if (config.block_cache.cache_size > 0 && config.block_cache.num_threads <= 0) {
        warnx("invalid block cache thread pool size %u", config.block_cache.num_threads);
        return -1;
    }
    if (config.block_cache.write_delay > 0 && config.block_cache.synchronous) {
        warnx("`--blockCacheSync' requires setting `--blockCacheWriteDelay=0'");
        return -1;
    }
    if (config.block_cache.cache_size > 0 && config.block_cache.cache_file != NULL) {
        int bs_bits = ffs(config.block_size) - 1;
        int cs_bits = ffs(config.block_cache.cache_size);

        if (bs_bits + cs_bits >= sizeof(off_t) * 8 - 1) {
            warnx("the block cache is too big to fit within a single file (%u blocks x %u bytes)",
              config.block_cache.cache_size, config.block_size);
            return -1;
        }
    }

    /* Check mount point */
    if (config.erase || config.reset) {
        if (config.mount != NULL) {
            warnx("no mount point should be specified with `--erase' or `--reset-mounted-flag'");
            return -1;
        }
    } else {
        if (config.mount == NULL) {
            warnx("no mount point specified");
            return -1;
        }
    }

    /* Format descriptive string of what we're mounting */
    if (config.test) {
        snprintf(config.description, sizeof(config.description), "%s%s/%s",
          "file://", config.http_io.bucket, config.http_io.prefix);
    } else if (config.http_io.vhost)
        snprintf(config.description, sizeof(config.description), "%s%s", config.http_io.baseURL, config.http_io.prefix);
    else {
        snprintf(config.description, sizeof(config.description), "%s%s/%s",
          config.http_io.baseURL, config.http_io.bucket, config.http_io.prefix);
    }

    /*
     * Read the first block (if any) to determine existing file and block size,
     * and compare with configured sizes (if given).
     */
    if (config.test)
        config.no_auto_detect = 1;
    if (config.no_auto_detect)
        r = ENOENT;
    else {
        config.http_io.debug = config.debug;
        config.http_io.quiet = config.quiet;
        config.http_io.log = config.log;
        if ((s3b = http_io_create(&config.http_io)) == NULL)
            err(1, "http_io_create");
        if (!config.quiet)
            warnx("auto-detecting block size and total file size...");
        r = (*s3b->meta_data)(s3b, &auto_file_size, &auto_block_size);
        (*s3b->destroy)(s3b);
    }

    /* Check result */
    switch (r) {
    case 0:
        unparse_size_string(blockSizeBuf, sizeof(blockSizeBuf), (uintmax_t)auto_block_size);
        unparse_size_string(fileSizeBuf, sizeof(fileSizeBuf), (uintmax_t)auto_file_size);
        if (!config.quiet)
            warnx("auto-detected block size=%s and total size=%s", blockSizeBuf, fileSizeBuf);
        if (config.block_size == 0)
            config.block_size = auto_block_size;
        else if (auto_block_size != config.block_size) {
            char buf[64];

            unparse_size_string(buf, sizeof(buf), (uintmax_t)config.block_size);
            if (config.force) {
                if (!config.quiet) {
                    warnx("warning: configured block size %s != filesystem block size %s,\n"
                      "but you said `--force' so I'll proceed anyway even though your data will\n"
                      "probably not read back correctly.", buf, blockSizeBuf);
                }
            } else
                errx(1, "error: configured block size %s != filesystem block size %s", buf, blockSizeBuf);
        }
        if (config.file_size == 0)
            config.file_size = auto_file_size;
        else if (auto_file_size != config.file_size) {
            char buf[64];

            unparse_size_string(buf, sizeof(buf), (uintmax_t)config.file_size);
            if (config.force) {
                if (!config.quiet) {
                    warnx("warning: configured file size %s != filesystem file size %s,\n"
                      "but you said `--force' so I'll proceed anyway even though your data will\n"
                      "probably not read back correctly.", buf, fileSizeBuf);
                }
            } else
                errx(1, "error: configured file size %s != filesystem file size %s", buf, fileSizeBuf);
        }
        break;
    case ENOENT:
    {
        const char *why = config.no_auto_detect ? "disabled" : "failed";
        int config_block_size = config.block_size;

        if (config.file_size == 0)
            errx(1, "error: auto-detection of filesystem size %s; please specify `--size'", why);
        if (config.block_size == 0)
            config.block_size = S3BACKER_DEFAULT_BLOCKSIZE;
        unparse_size_string(blockSizeBuf, sizeof(blockSizeBuf), (uintmax_t)config.block_size);
        unparse_size_string(fileSizeBuf, sizeof(fileSizeBuf), (uintmax_t)config.file_size);
        if (!config.quiet) {
            warnx("auto-detection %s; using %s block size %s and file size %s", why,
              config_block_size == 0 ? "default" : "configured", blockSizeBuf, fileSizeBuf);
        }
        break;
    }
    default:
        errno = r;
        err(1, "can't read data store meta-data");
        break;
    }

    /* Check whether already mounted */
    if (!config.test && !config.erase && !config.reset) {
        int mounted;

        config.http_io.debug = config.debug;
        config.http_io.quiet = config.quiet;
        config.http_io.log = config.log;
        if ((s3b = http_io_create(&config.http_io)) == NULL)
            err(1, "http_io_create");
        r = (*s3b->set_mounted)(s3b, &mounted, -1);
        (*s3b->destroy)(s3b);
        if (r != 0) {
            errno = r;
            err(1, "error reading mounted flag");
        }
        if (mounted) {
            if (!config.force)
                errx(1, "error: %s appears to be already mounted", config.description);
            if (!config.quiet) {
                warnx("warning: filesystem appears already mounted but you said `--force'\n"
                  " so I'll proceed anyway even though your data may get corrupted.\n");
            }
        }
    }

    /* Check computed block and file sizes */
    if (config.block_size != (1 << (ffs(config.block_size) - 1))) {
        warnx("block size must be a power of 2");
        return -1;
    }
    if (config.file_size % config.block_size != 0) {
        warnx("file size must be a multiple of block size");
        return -1;
    }
    config.num_blocks = config.file_size / config.block_size;
    if (sizeof(s3b_block_t) < sizeof(config.num_blocks)
      && config.num_blocks > ((off_t)1 << (sizeof(s3b_block_t) * 8))) {
        warnx("more than 2^%d blocks: decrease file size or increase block size", (int)(sizeof(s3b_block_t) * 8));
        return -1;
    }

    /* Check block size vs. encryption block size */
    if (config.http_io.encryption != NULL && config.block_size % EVP_MAX_IV_LENGTH != 0) {
        warnx("block size must be at least %u when encryption is enabled", EVP_MAX_IV_LENGTH);
        return -1;
    }

    /* Check that MD5 cache won't eventually deadlock */
    if (config.ec_protect.cache_size > 0
      && config.ec_protect.cache_time == 0
      && config.ec_protect.cache_size < config.num_blocks) {
        warnx("`md5CacheTime' is infinite but `md5CacheSize' is less than the number of blocks, so eventual deadlock will result");
        return -1;
    }

    /* No point in the caches being bigger than necessary */
    if (config.ec_protect.cache_size > config.num_blocks) {
        warnx("MD5 cache size (%ju) is greater that the total number of blocks (%ju); automatically reducing",
          (uintmax_t)config.ec_protect.cache_size, (uintmax_t)config.num_blocks);
        config.ec_protect.cache_size = config.num_blocks;
    }
    if (config.block_cache.cache_size > config.num_blocks) {
        warnx("block cache size (%ju) is greater that the total number of blocks (%ju); automatically reducing",
          (uintmax_t)config.block_cache.cache_size, (uintmax_t)config.num_blocks);
        config.block_cache.cache_size = config.num_blocks;
    }

#ifdef __APPLE__
    /* On MacOS, warn if kernel timeouts can happen prior to our own timeout */
    {
        u_int total_time = 0;
        u_int retry_pause = 0;
        u_int total_pause;

        /*
         * Determine how much total time an operation can take including retries.
         * We have to use the same exponential backoff algorithm.
         */
        for (total_pause = 0; 1; total_pause += retry_pause) {
            total_time += config.http_io.timeout * 1000;
            if (total_pause >= config.http_io.max_retry_pause)
                break;
            retry_pause = retry_pause > 0 ? retry_pause * 2 : config.http_io.initial_retry_pause;
            if (total_pause + retry_pause > config.http_io.max_retry_pause)
                retry_pause = config.http_io.max_retry_pause - total_pause;
            total_time += retry_pause;
        }

        /* Convert from milliseconds to seconds */
        total_time = (total_time + 999) / 1000;

        /* Warn if exceeding MacFUSE limit */
        if (total_time >= FUSE_MAX_DAEMON_TIMEOUT && !config.quiet) {
            warnx("warning: maximum possible I/O delay (%us) >= MacFUSE limit (%us);", total_time, FUSE_MAX_DAEMON_TIMEOUT);
            warnx("consider lower settings for `--maxRetryPause' and/or `--timeout'.");
        }
    }
#endif  /* __APPLE__ */

    /* Copy common stuff into sub-module configs */
    config.block_cache.block_size = config.block_size;
    config.block_cache.log = config.log;
    config.http_io.debug = config.debug;
    config.http_io.quiet = config.quiet;
    config.http_io.block_size = config.block_size;
    config.http_io.num_blocks = config.num_blocks;
    config.http_io.log = config.log;
    config.ec_protect.block_size = config.block_size;
    config.ec_protect.log = config.log;
    config.fuse_ops.block_size = config.block_size;
    config.fuse_ops.num_blocks = config.num_blocks;
    config.fuse_ops.log = config.log;

    /* If `--listBlocks' was given, build non-empty block bitmap */
    if (config.erase || config.reset)
        config.list_blocks = 0;
    if (config.list_blocks) {
        struct s3backer_store *temp_store;
        struct list_blocks lb;
        size_t nwords;

        /* Logging */
        if (!config.quiet) {
            fprintf(stderr, "s3backer: listing non-zero blocks...");
            fflush(stderr);
        }

        /* Create temporary lower layer */
        if ((temp_store = config.test ? test_io_create(&config.http_io) : http_io_create(&config.http_io)) == NULL)
            err(1, config.test ? "test_io_create" : "http_io_create");

        /* Initialize bitmap */
        nwords = (config.num_blocks + (sizeof(*lb.bitmap) * 8) - 1) / (sizeof(*lb.bitmap) * 8);
        if ((lb.bitmap = calloc(nwords, sizeof(*lb.bitmap))) == NULL)
            err(1, "calloc");
        lb.print_dots = !config.quiet;
        lb.count = 0;

        /* Generate non-zero block bitmap */
        assert(config.http_io.nonzero_bitmap == NULL);
        if ((r = (*temp_store->list_blocks)(temp_store, list_blocks_callback, &lb)) != 0)
            errx(1, "can't list blocks: %s", strerror(r));

        /* Close temporary store */
        (*temp_store->destroy)(temp_store);

        /* Save generated bitmap */
        config.http_io.nonzero_bitmap = lb.bitmap;

        /* Logging */
        if (!config.quiet) {
            fprintf(stderr, "done\n");
            warnx("found %ju non-zero blocks", lb.count);
        }
    }

    /* Done */
    return 0;
}

static void
list_blocks_callback(void *arg, s3b_block_t block_num)
{
    struct list_blocks *const lb = arg;
    const int bits_per_word = sizeof(*lb->bitmap) * 8;

    lb->bitmap[block_num / bits_per_word] |= 1 << (block_num % bits_per_word);
    lb->count++;
    if (lb->print_dots && (lb->count % BLOCKS_PER_DOT) == 0) {
        fprintf(stderr, ".");
        fflush(stderr);
    }
}

static void
dump_config(void)
{
    int i;

    (*config.log)(LOG_DEBUG, "s3backer config:");
    (*config.log)(LOG_DEBUG, "%24s: %s", "test mode", config.test ? "true" : "false");
    (*config.log)(LOG_DEBUG, "%24s: %s", "directIO", config.fuse_ops.direct_io ? "true" : "false");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "accessId", config.http_io.accessId != NULL ? config.http_io.accessId : "");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "accessKey", config.http_io.accessKey != NULL ? "****" : "");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "accessFile", config.accessFile);
    (*config.log)(LOG_DEBUG, "%24s: %s", "accessType", config.http_io.accessType);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "ec2iam_role", config.http_io.ec2iam_role != NULL ? config.http_io.ec2iam_role : "");
    (*config.log)(LOG_DEBUG, "%24s: %s", "authVersion", config.http_io.authVersion);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "baseURL", config.http_io.baseURL);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "region", config.http_io.region);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", config.test ? "testdir" : "bucket", config.http_io.bucket);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "prefix", config.http_io.prefix);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "defaultContentEncoding",
      config.http_io.default_ce != NULL ? config.http_io.default_ce : "(none)");
    (*config.log)(LOG_DEBUG, "%24s: %s", "list_blocks", config.list_blocks ? "true" : "false");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "mount", config.mount);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "filename", config.fuse_ops.filename);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "stats_filename", config.fuse_ops.stats_filename);
    (*config.log)(LOG_DEBUG, "%24s: %s (%u)", "block_size",
      config.block_size_str != NULL ? config.block_size_str : "-", config.block_size);
    (*config.log)(LOG_DEBUG, "%24s: %s (%jd)", "file_size",
      config.file_size_str != NULL ? config.file_size_str : "-", (intmax_t)config.file_size);
    (*config.log)(LOG_DEBUG, "%24s: %jd", "num_blocks", (intmax_t)config.num_blocks);
    (*config.log)(LOG_DEBUG, "%24s: 0%o", "file_mode", config.fuse_ops.file_mode);
    (*config.log)(LOG_DEBUG, "%24s: %s", "read_only", config.fuse_ops.read_only ? "true" : "false");
    (*config.log)(LOG_DEBUG, "%24s: %d", "compress", config.http_io.compress);
    (*config.log)(LOG_DEBUG, "%24s: %s", "encryption", config.http_io.encryption != NULL ? config.http_io.encryption : "(none)");
    (*config.log)(LOG_DEBUG, "%24s: %u", "key_length", config.http_io.key_length);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "password", config.http_io.password != NULL ? "****" : "");
    (*config.log)(LOG_DEBUG, "%24s: %s bps (%ju)", "max_upload",
      config.max_speed_str[HTTP_UPLOAD] != NULL ? config.max_speed_str[HTTP_UPLOAD] : "-",
      config.http_io.max_speed[HTTP_UPLOAD]);
    (*config.log)(LOG_DEBUG, "%24s: %s bps (%ju)", "max_download",
      config.max_speed_str[HTTP_DOWNLOAD] != NULL ? config.max_speed_str[HTTP_DOWNLOAD] : "-",
      config.http_io.max_speed[HTTP_DOWNLOAD]);
    (*config.log)(LOG_DEBUG, "%24s: %us", "timeout", config.http_io.timeout);
    (*config.log)(LOG_DEBUG, "%24s: %ums", "initial_retry_pause", config.http_io.initial_retry_pause);
    (*config.log)(LOG_DEBUG, "%24s: %ums", "max_retry_pause", config.http_io.max_retry_pause);
    (*config.log)(LOG_DEBUG, "%24s: %ums", "min_write_delay", config.ec_protect.min_write_delay);
    (*config.log)(LOG_DEBUG, "%24s: %ums", "md5_cache_time", config.ec_protect.cache_time);
    (*config.log)(LOG_DEBUG, "%24s: %u entries", "md5_cache_size", config.ec_protect.cache_size);
    (*config.log)(LOG_DEBUG, "%24s: %u entries", "block_cache_size", config.block_cache.cache_size);
    (*config.log)(LOG_DEBUG, "%24s: %u threads", "block_cache_threads", config.block_cache.num_threads);
    (*config.log)(LOG_DEBUG, "%24s: %ums", "block_cache_timeout", config.block_cache.timeout);
    (*config.log)(LOG_DEBUG, "%24s: %ums", "block_cache_write_delay", config.block_cache.write_delay);
    (*config.log)(LOG_DEBUG, "%24s: %u blocks", "block_cache_max_dirty", config.block_cache.max_dirty);
    (*config.log)(LOG_DEBUG, "%24s: %s", "block_cache_sync", config.block_cache.synchronous ? "true" : "false");
    (*config.log)(LOG_DEBUG, "%24s: %u blocks", "read_ahead", config.block_cache.read_ahead);
    (*config.log)(LOG_DEBUG, "%24s: %u blocks", "read_ahead_trigger", config.block_cache.read_ahead_trigger);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "block_cache_cache_file",
      config.block_cache.cache_file != NULL ? config.block_cache.cache_file : "");
    (*config.log)(LOG_DEBUG, "%24s: %s", "block_cache_no_verify", config.block_cache.no_verify ? "true" : "false");
    (*config.log)(LOG_DEBUG, "fuse_main arguments:");
    for (i = 0; i < config.fuse_args.argc; i++)
        (*config.log)(LOG_DEBUG, "  [%d] = \"%s\"", i, config.fuse_args.argv[i]);
}

static void
syslog_logger(int level, const char *fmt, ...)
{
    va_list args;

    /* Filter debug messages */
    if (!config.debug && level == LOG_DEBUG)
        return;

    /* Send message to syslog */
    va_start(args, fmt);
    vsyslog(level, fmt, args);
    va_end(args);
}

static void
stderr_logger(int level, const char *fmt, ...)
{
    const char *levelstr;
    char timebuf[32];
    va_list args;
    struct tm tm;
    time_t now;

    /* Filter debug messages */
    if (!config.debug && level == LOG_DEBUG)
        return;

    /* Get level descriptor */
    switch (level) {
    case LOG_ERR:
        levelstr = "ERROR";
        break;
    case LOG_WARNING:
        levelstr = "WARNING";
        break;
    case LOG_NOTICE:
        levelstr = "NOTICE";
        break;
    case LOG_INFO:
        levelstr = "INFO";
        break;
    case LOG_DEBUG:
        levelstr = "DEBUG";
        break;
    default:
        levelstr = "<?>";
        break;
    }

    /* Format and print log message */
    time(&now);
    strftime(timebuf, sizeof(timebuf), "%F %T", localtime_r(&now, &tm));
    va_start(args, fmt);
    fprintf(stderr, "%s %s: ", timebuf, levelstr);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
    va_end(args);
}

static void
usage(void)
{
    int i;

    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "\ts3backer [options] bucket /mount/point\n");
    fprintf(stderr, "\ts3backer --test [options] directory /mount/point\n");
    fprintf(stderr, "\ts3backer --erase [options] bucket\n");
    fprintf(stderr, "\ts3backer --reset-mounted-flag [options] bucket\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "\t--%-27s %s\n", "accessFile=FILE", "File containing `accessID:accessKey' pairs");
    fprintf(stderr, "\t--%-27s %s\n", "accessId=ID", "S3 access key ID");
    fprintf(stderr, "\t--%-27s %s\n", "accessKey=KEY", "S3 secret access key");
    fprintf(stderr, "\t--%-27s %s\n", "accessType=TYPE", "S3 ACL used when creating new items; one of:");
    fprintf(stderr, "\t  %-27s ", "");
    for (i = 0; i < sizeof(s3_acls) / sizeof(*s3_acls); i++)
        fprintf(stderr, "%s%s", i > 0 ? ", " : "  ", s3_acls[i]);
    fprintf(stderr, "\n");
    fprintf(stderr, "\t--%-27s %s\n", "authVersion=TYPE", "Specify S3 authentication style; one of:");
    fprintf(stderr, "\t  %-27s ", "");
    for (i = 0; i < sizeof(s3_auth_types) / sizeof(*s3_auth_types); i++)
        fprintf(stderr, "%s%s", i > 0 ? ", " : "  ", s3_auth_types[i]);
    fprintf(stderr, "\n");
    fprintf(stderr, "\t--%-27s %s\n", "accessEC2IAM=ROLE", "Acquire S3 credentials from EC2 machine via IAM role");
    fprintf(stderr, "\t--%-27s %s\n", "baseURL=URL", "Base URL for all requests");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheFile=FILE", "Block cache persistent file");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheMaxDirty=NUM", "Block cache maximum number of dirty blocks");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheNoVerify", "Disable verification of data loaded from cache file");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheSize=NUM", "Block cache size (in number of blocks)");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheSync", "Block cache performs all writes synchronously");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheThreads=NUM", "Block cache write-back thread pool size");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheTimeout=MILLIS", "Block cache entry timeout (zero = infinite)");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheWriteDelay=MILLIS", "Block cache maximum write-back delay");
    fprintf(stderr, "\t--%-27s %s\n", "blockSize=SIZE", "Block size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-27s %s\n", "cacert=FILE", "Specify SSL certificate authority file");
    fprintf(stderr, "\t--%-27s %s\n", "compress[=LEVEL]", "Enable block compression, with 1=fast up to 9=small");
    fprintf(stderr, "\t--%-27s %s\n", "debug", "Enable logging of debug messages");
    fprintf(stderr, "\t--%-27s %s\n", "debug-http", "Print HTTP headers to standard output");
    fprintf(stderr, "\t--%-27s %s\n", "directIO", "Disable kernel caching of the backed file");
    fprintf(stderr, "\t--%-27s %s\n", "encrypt[=CIPHER]", "Enable encryption (implies `--compress')");
    fprintf(stderr, "\t--%-27s %s\n", "erase", "Erase all blocks in the filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "fileMode=MODE", "Permissions of backed file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "filename=NAME", "Name of backed file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "force", "Ignore different auto-detected block and file sizes");
    fprintf(stderr, "\t--%-27s %s\n", "help", "Show this information and exit");
    fprintf(stderr, "\t--%-27s %s\n", "initialRetryPause=MILLIS", "Inital retry pause after stale data or server error");
    fprintf(stderr, "\t--%-27s %s\n", "insecure", "Don't verify SSL server identity");
    fprintf(stderr, "\t--%-27s %s\n", "keyLength", "Override generated cipher key length");
    fprintf(stderr, "\t--%-27s %s\n", "listBlocks", "Auto-detect non-empty blocks at startup");
    fprintf(stderr, "\t--%-27s %s\n", "maxDownloadSpeed=BITSPERSEC", "Max download bandwidth for a single read");
    fprintf(stderr, "\t--%-27s %s\n", "maxRetryPause=MILLIS", "Max total pause after stale data or server error");
    fprintf(stderr, "\t--%-27s %s\n", "maxUploadSpeed=BITSPERSEC", "Max upload bandwidth for a single write");
    fprintf(stderr, "\t--%-27s %s\n", "md5CacheSize=NUM", "Max size of MD5 cache (zero = disabled)");
    fprintf(stderr, "\t--%-27s %s\n", "md5CacheTime=MILLIS", "Expire time for MD5 cache (zero = infinite)");
    fprintf(stderr, "\t--%-27s %s\n", "minWriteDelay=MILLIS", "Minimum time between same block writes");
    fprintf(stderr, "\t--%-27s %s\n", "password=PASSWORD", "Encrypt using PASSWORD");
    fprintf(stderr, "\t--%-27s %s\n", "passwordFile=FILE", "Encrypt using password read from FILE");
    fprintf(stderr, "\t--%-27s %s\n", "prefix=STRING", "Prefix for resource names within bucket");
    fprintf(stderr, "\t--%-27s %s\n", "defaultContentEncoding=STRING", "Default HTTP Content-Encoding if none given");
    fprintf(stderr, "\t--%-27s %s\n", "quiet", "Omit progress output at startup");
    fprintf(stderr, "\t--%-27s %s\n", "readAhead=NUM", "Number of blocks to read-ahead");
    fprintf(stderr, "\t--%-27s %s\n", "readAheadTrigger=NUM", "# of sequentially read blocks to trigger read-ahead");
    fprintf(stderr, "\t--%-27s %s\n", "readOnly", "Return `Read-only file system' error for write attempts");
    fprintf(stderr, "\t--%-27s %s\n", "region=region", "Specify AWS region");
    fprintf(stderr, "\t--%-27s %s\n", "reset-mounted-flag", "Reset `already mounted' flag in the filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "rrs", "Target written blocks for Reduced Redundancy Storage (deprecated)");
    fprintf(stderr, "\t--%-27s %s\n", "size=SIZE", "File size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-27s %s\n", "ssl", "Enable SSL");
    fprintf(stderr, "\t--%-27s %s\n", "statsFilename=NAME", "Name of statistics file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "storageClass=TYPE", "Specify storage class for written blocks");
    fprintf(stderr, "\t--%-27s %s\n", "test", "Run in local test mode (bucket is a directory)");
    fprintf(stderr, "\t--%-27s %s\n", "timeout=SECONDS", "Max time allowed for one HTTP operation");
    fprintf(stderr, "\t--%-27s %s\n", "timeout=SECONDS", "Specify HTTP operation timeout");
    fprintf(stderr, "\t--%-27s %s\n", "version", "Show version information and exit");
    fprintf(stderr, "\t--%-27s %s\n", "vhost", "Use virtual host bucket style URL for all requests");
    fprintf(stderr, "Default values:\n");
    fprintf(stderr, "\t--%-27s \"%s\"\n", "accessFile", "$HOME/" S3BACKER_DEFAULT_PWD_FILE);
    fprintf(stderr, "\t--%-27s %s\n", "accessId", "The first one listed in `accessFile'");
    fprintf(stderr, "\t--%-27s \"%s\"\n", "accessType", S3BACKER_DEFAULT_ACCESS_TYPE);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "authVersion", S3BACKER_DEFAULT_AUTH_VERSION);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "baseURL", "http://s3." S3_DOMAIN "/");
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheSize", S3BACKER_DEFAULT_BLOCK_CACHE_SIZE);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheThreads", S3BACKER_DEFAULT_BLOCK_CACHE_NUM_THREADS);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheTimeout", S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheWriteDelay", S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY);
    fprintf(stderr, "\t--%-27s %d\n", "blockSize", S3BACKER_DEFAULT_BLOCKSIZE);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "filename", S3BACKER_DEFAULT_FILENAME);
    fprintf(stderr, "\t--%-27s %u\n", "initialRetryPause", S3BACKER_DEFAULT_INITIAL_RETRY_PAUSE);
    fprintf(stderr, "\t--%-27s %u\n", "md5CacheSize", S3BACKER_DEFAULT_MD5_CACHE_SIZE);
    fprintf(stderr, "\t--%-27s %u\n", "md5CacheTime", S3BACKER_DEFAULT_MD5_CACHE_TIME);
    fprintf(stderr, "\t--%-27s 0%03o (0%03o if `--readOnly')\n", "fileMode",
      S3BACKER_DEFAULT_FILE_MODE, S3BACKER_DEFAULT_FILE_MODE_READ_ONLY);
    fprintf(stderr, "\t--%-27s %u\n", "maxRetryPause", S3BACKER_DEFAULT_MAX_RETRY_PAUSE);
    fprintf(stderr, "\t--%-27s %u\n", "minWriteDelay", S3BACKER_DEFAULT_MIN_WRITE_DELAY);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "prefix", S3BACKER_DEFAULT_PREFIX);
    fprintf(stderr, "\t--%-27s %u\n", "readAhead", S3BACKER_DEFAULT_READ_AHEAD);
    fprintf(stderr, "\t--%-27s %u\n", "readAheadTrigger", S3BACKER_DEFAULT_READ_AHEAD_TRIGGER);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "region", S3BACKER_DEFAULT_REGION);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "statsFilename", S3BACKER_DEFAULT_STATS_FILENAME);
    fprintf(stderr, "\t--%-27s %u\n", "timeout", S3BACKER_DEFAULT_TIMEOUT);
    fprintf(stderr, "FUSE options (partial list):\n");
    fprintf(stderr, "\t%-29s %s\n", "-o nonempty", "Allows mount over a non-empty directory");
    fprintf(stderr, "\t%-29s %s\n", "-o uid=UID", "Set user ID");
    fprintf(stderr, "\t%-29s %s\n", "-o gid=GID", "Set group ID");
    fprintf(stderr, "\t%-29s %s\n", "-o sync_read", "Do synchronous reads");
    fprintf(stderr, "\t%-29s %s\n", "-o max_readahead=NUM", "Set maximum read-ahead (bytes)");
    fprintf(stderr, "\t%-29s %s\n", "-f", "Run in the foreground (do not fork)");
    fprintf(stderr, "\t%-29s %s\n", "-d", "Debug mode (implies -f)");
    fprintf(stderr, "\t%-29s %s\n", "-s", "Run in single-threaded mode");
}

