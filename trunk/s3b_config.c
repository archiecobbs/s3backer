
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
#include "ec_protect.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"

/****************************************************************************
 *                          DEFINITIONS                                     *
 ****************************************************************************/

/* S3 URL */
#define S3_BASE_URL                                 "http://s3.amazonaws.com/"
#define S3_BASE_URL_HTTPS                           "https://s3.amazonaws.com/"

/* S3 access permission strings */
#define S3_ACCESS_PRIVATE                           "private"
#define S3_ACCESS_PUBLIC_READ                       "public-read"
#define S3_ACCESS_PUBLIC_READ_WRITE                 "public-read-write"
#define S3_ACCESS_AUTHENTICATED_READ                "authenticated-read"

/* Default values for some configuration parameters */
#define S3BACKER_DEFAULT_ACCESS_TYPE                S3_ACCESS_PRIVATE
#define S3BACKER_DEFAULT_BASE_URL                   S3_BASE_URL
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
#define S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY    0
#define S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT        0
#define S3BACKER_DEFAULT_READ_AHEAD                 4
#define S3BACKER_DEFAULT_READ_AHEAD_TRIGGER         2

/* MacFUSE setting for kernel daemon timeout */
#ifdef __APPLE__
#ifndef FUSE_MAX_DAEMON_TIMEOUT
#define FUSE_MAX_DAEMON_TIMEOUT         600
#endif
#define s3bquote0(x)                    #x
#define s3bquote(x)                     s3bquote0(x)
#define FUSE_MAX_DAEMON_TIMEOUT_STRING  s3bquote(FUSE_MAX_DAEMON_TIMEOUT)
#endif  /* __APPLE__ */

/****************************************************************************
 *                          FUNCTION DECLARATIONS                           *
 ****************************************************************************/

static create_s3b_t s3b_config_create_s3b;
static print_stats_t s3b_config_print_stats;

static int parse_size_string(const char *s, uintmax_t *valp);
static void unparse_size_string(char *buf, size_t bmax, uintmax_t value);
static int search_access_for(const char *file, const char *accessId, const char **idptr, const char **pwptr);
static int handle_unknown_option(void *data, const char *arg, int key, struct fuse_args *outargs);
static void syslog_logger(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
static void stderr_logger(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
static int validate_config(void);
static void dump_config(void);
static void usage(void);

/****************************************************************************
 *                          VARIABLE DEFINITIONS                            *
 ****************************************************************************/

/* Valid S3 access values */
static const char *const s3_acls[] = {
    S3_ACCESS_PRIVATE,
    S3_ACCESS_PUBLIC_READ,
    S3_ACCESS_PUBLIC_READ_WRITE,
    S3_ACCESS_AUTHENTICATED_READ
};

/* Configuration structure */
static char user_agent_buf[64];
static struct s3b_config config = {

    /* HTTP config */
    .http_io= {
        .accessId=              NULL,
        .accessKey=             NULL,
        .baseURL=               S3BACKER_DEFAULT_BASE_URL,
        .bucket=                NULL,
        .prefix=                S3BACKER_DEFAULT_PREFIX,
        .accessType=            S3BACKER_DEFAULT_ACCESS_TYPE,
        .user_agent=            user_agent_buf,
        .timeout=               S3BACKER_DEFAULT_TIMEOUT,
        .initial_retry_pause=   S3BACKER_DEFAULT_INITIAL_RETRY_PAUSE,
        .max_retry_pause=       S3BACKER_DEFAULT_MAX_RETRY_PAUSE,
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
    .no_auto_detect=        0,
    .log=                   syslog_logger
};

/* Command line flags */
static const struct fuse_opt option_list[] = {
    {
        .templ=     "--accessFile=%s",
        .offset=    offsetof(struct s3b_config, accessFile),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--accessId=%s",
        .offset=    offsetof(struct s3b_config, http_io.accessId),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--accessKey=%s",
        .offset=    offsetof(struct s3b_config, http_io.accessKey),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--accessType=%s",
        .offset=    offsetof(struct s3b_config, http_io.accessType),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--assumeEmpty",                    /* deprecated */
        .offset=    offsetof(struct s3b_config, list_blocks),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--listBlocks",
        .offset=    offsetof(struct s3b_config, list_blocks),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--baseURL=%s",
        .offset=    offsetof(struct s3b_config, http_io.baseURL),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--blockCacheSize=%u",
        .offset=    offsetof(struct s3b_config, block_cache.cache_size),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--blockCacheThreads=%u",
        .offset=    offsetof(struct s3b_config, block_cache.num_threads),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--blockCacheTimeout=%u",
        .offset=    offsetof(struct s3b_config, block_cache.timeout),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--blockCacheWriteDelay=%u",
        .offset=    offsetof(struct s3b_config, block_cache.write_delay),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--readAhead=%u",
        .offset=    offsetof(struct s3b_config, block_cache.read_ahead),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--readAheadTrigger=%u",
        .offset=    offsetof(struct s3b_config, block_cache.read_ahead_trigger),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--blockSize=%s",
        .offset=    offsetof(struct s3b_config, block_size_str),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--md5CacheSize=%u",
        .offset=    offsetof(struct s3b_config, ec_protect.cache_size),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--md5CacheTime=%u",
        .offset=    offsetof(struct s3b_config, ec_protect.cache_time),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--debug",
        .offset=    offsetof(struct s3b_config, debug),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--fileMode=%o",
        .offset=    offsetof(struct s3b_config, fuse_ops.file_mode),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--filename=%s",
        .offset=    offsetof(struct s3b_config, fuse_ops.filename),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--force",
        .offset=    offsetof(struct s3b_config, force),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--noAutoDetect",
        .offset=    offsetof(struct s3b_config, no_auto_detect),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--initialRetryPause=%u",
        .offset=    offsetof(struct s3b_config, http_io.initial_retry_pause),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--maxRetryPause=%u",
        .offset=    offsetof(struct s3b_config, http_io.max_retry_pause),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--minWriteDelay=%u",
        .offset=    offsetof(struct s3b_config, ec_protect.min_write_delay),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--prefix=%s",
        .offset=    offsetof(struct s3b_config, http_io.prefix),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--readOnly",
        .offset=    offsetof(struct s3b_config, fuse_ops.read_only),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--size=%s",
        .offset=    offsetof(struct s3b_config, file_size_str),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--statsFilename=%s",
        .offset=    offsetof(struct s3b_config, fuse_ops.stats_filename),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--ssl",
        .offset=    offsetof(struct s3b_config, ssl),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--cacert=%s",
        .offset=    offsetof(struct s3b_config, http_io.cacert),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--insecure",
        .offset=    offsetof(struct s3b_config, http_io.insecure),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--test",
        .offset=    offsetof(struct s3b_config, test),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--timeout=%u",
        .offset=    offsetof(struct s3b_config, http_io.timeout),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    FUSE_OPT_END
};

/* Default flags we send to FUSE */
static const char *const s3backer_fuse_defaults[] = {
    "-okernel_cache",
    "-ouse_ino",
    "-omax_readahead=0",
    "-osubtype=s3backer",
    "-oentry_timeout=31536000",
    "-onegative_timeout=31536000",
    "-oattr_timeout=0",             // because statistics file length changes
    "-odefault_permissions",
    "-onodev",
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
    char buf[1024];
    int i;

    /* Remember user creds */
    config.fuse_ops.uid = getuid();
    config.fuse_ops.gid = getgid();

    /* Set user-agent */
    snprintf(user_agent_buf, sizeof(user_agent_buf), "%s/%s/r%d", PACKAGE, VERSION, s3backer_svnrev);

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

    /* Parse command line flags */
    if (fuse_opt_parse(&config.fuse_args, &config, option_list, handle_unknown_option) != 0)
        return NULL;

    /* Set fsname based on configuration */
    snprintf(buf, sizeof(buf), "-ofsname=%s%s/%s",
      config.test ? "" : config.http_io.baseURL, config.http_io.bucket, config.http_io.prefix);
    if (fuse_opt_insert_arg(&config.fuse_args, 1, buf) != 0)
        err(1, "fuse_opt_insert_arg");

    /* Validate configuration */
    if (validate_config() != 0)
        return NULL;

    /* Set up fuse_ops callbacks */
    config.fuse_ops.create_s3b = s3b_config_create_s3b;
    config.fuse_ops.print_stats = s3b_config_print_stats;
    config.fuse_ops.arg = &config;

    /* Debug */
    if (config.debug)
        dump_config();

    /* Done */
    return &config;
}

struct s3backer_store *
s3backer_create_store(struct s3b_config *conf)
{
    struct s3backer_store *store;

    /* Sanity check */
    if (http_io_store != NULL || test_io_store != NULL)
        return NULL;

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
        if ((ec_protect_store = ec_protect_create(&conf->ec_protect, store)) == NULL) {
            (*store->destroy)(store);
            http_io_store = NULL;
            test_io_store = NULL;
            return NULL;
        }
        store = ec_protect_store;
    }

    /* Create block cache layer (if desired) */
    if (conf->block_cache.cache_size > 0) {
        if ((block_cache_store = block_cache_create(&conf->block_cache, store)) == NULL) {
            (*store->destroy)(store);
            ec_protect_store = NULL;
            http_io_store = NULL;
            test_io_store = NULL;
            return NULL;
        }
        store = block_cache_store;
    }

    /* Done */
    return store;
}

/****************************************************************************
 *                    INTERNAL FUNCTION DEFINITIONS                         *
 ****************************************************************************/

static struct s3backer_store *
s3b_config_create_s3b(void *arg)
{
    struct s3b_config *const conf = arg;

    return s3backer_create_store(conf);
}

static void
s3b_config_print_stats(void *arg, void *prarg, printer_t *printer)
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
        (*printer)(prarg, "%-28s %u\n", "http_5xx_error", http_io_stats.http_5xx_error);
        (*printer)(prarg, "%-28s %u\n", "http_4xx_error", http_io_stats.http_4xx_error);
        (*printer)(prarg, "%-28s %u\n", "http_other_error", http_io_stats.http_other_error);
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
        (*printer)(prarg, "%-28s %.4f\n", "block_cache_dirty_ratio", block_cache_stats.dirty_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_read_hits", block_cache_stats.read_hits);
        (*printer)(prarg, "%-28s %u\n", "block_cache_read_misses", block_cache_stats.read_misses);
        (*printer)(prarg, "%-28s %.4f\n", "block_cache_read_hit_ratio", read_hit_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_write_hits", block_cache_stats.write_hits);
        (*printer)(prarg, "%-28s %u\n", "block_cache_write_misses", block_cache_stats.write_misses);
        (*printer)(prarg, "%-28s %.4f\n", "block_cache_write_hit_ratio", write_hit_ratio);
        total_oom += ec_protect_stats.out_of_memory_errors;
    }
    if (ec_protect_store != NULL) {
        (*printer)(prarg, "%-28s %u blocks\n", "md5_cache_current_size", ec_protect_stats.current_cache_size);
        (*printer)(prarg, "%-28s %u\n", "md5_cache_data_hits", ec_protect_stats.cache_data_hits);
        (*printer)(prarg, "%-28s %ju.%03u sec\n", "md5_cache_full_delays",
          (uintmax_t)(ec_protect_stats.cache_full_delay / 1000), (u_int)(ec_protect_stats.cache_full_delay % 1000));
        (*printer)(prarg, "%-28s %ju.%03u sec\n", "md5_cache_write_delays",
          (uintmax_t)(ec_protect_stats.repeated_write_delay / 1000), (u_int)(ec_protect_stats.repeated_write_delay % 1000));
        total_oom += block_cache_stats.out_of_memory_errors;
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
        int i;

        for (i = 0; i < sizeof(size_suffixes) / sizeof(*size_suffixes); i++) {
            const struct size_suffix *const ss = &size_suffixes[i];

            if (ss->bits >= sizeof(off_t) * 8)
                break;
            if (strcasecmp(suffix, ss->suffix) == 0)
                *valp <<= ss->bits;
        }
    }
    return 0;
}

static void
unparse_size_string(char *buf, size_t bmax, uintmax_t value)
{
    uintmax_t unit;
    int i;

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
        if (strcmp(arg, "-d") == 0 || strcmp(arg, "-f") == 0) {
            config.log = stderr_logger;
            config.debug = 1;
        }

        /* Version */
        if (strcmp(arg, "--version") == 0 || strcmp(arg, "-v") == 0) {
            fprintf(stderr, "%s version %s (r%d)\n", PACKAGE, VERSION, s3backer_svnrev);
            fprintf(stderr, "Copyright (C) 2008 Archie L. Cobbs.\n");
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
search_access_for(const char *file, const char *accessId, const char **idptr, const char **pwptr)
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
    off_t auto_file_size;
    u_int auto_block_size;
    uintmax_t value;
    const char *s;
    char blockSizeBuf[64];
    char fileSizeBuf[64];
    struct stat sb;
    int i;
    int r;

    /* Default to $HOME/.s3backer for accessFile */
    if (config.accessFile == NULL) {
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
    if (config.http_io.accessId == NULL
      && !config.fuse_ops.read_only
      && (strcmp(config.http_io.baseURL, S3_BASE_URL) == 0 || strcmp(config.http_io.baseURL, S3_BASE_URL_HTTPS) == 0)) {
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

    /* Check base URL */
    s = NULL;
    if (strncmp(config.http_io.baseURL, "http://", 7) == 0)
        s = config.http_io.baseURL + 7;
    else if (strncmp(config.http_io.baseURL, "https://", 8) == 0)
        s = config.http_io.baseURL + 8;
    if (s != NULL && (*s == '/' || *s == '\0'))
        s = NULL;
    if (s != NULL && (s = strchr(s, '/')) == NULL)
        s = NULL;
    if (s != NULL && s[1] != '\0') {
        warnx("base URL must end with a '/'");
        s = NULL;
    }
    if (s == NULL) {
        warnx("invalid base URL `%s'", config.http_io.baseURL);
        return -1;
    }
    if (config.ssl) {
        if (strcmp(config.http_io.baseURL, S3BACKER_DEFAULT_BASE_URL) != 0
          && strcmp(config.http_io.baseURL, S3_BASE_URL_HTTPS) != 0) {
            warnx("specify only one of `--baseURL' and `--ssl'");
            return -1;
        }
        config.http_io.baseURL = S3_BASE_URL_HTTPS;
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

    /* Check time/cache values */
    if (config.ec_protect.cache_size == 0 && config.ec_protect.cache_time > 0) {
        warnx("`md5CacheTime' must zero when MD5 cache is disabled");
        return -1;
    }
    if (config.ec_protect.cache_size == 0 && config.ec_protect.min_write_delay > 0) {
        warnx("`minWriteDelay' must zero when MD5 cache is disabled");
        return -1;
    }
    if (config.ec_protect.cache_time < config.ec_protect.min_write_delay) {
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
        config.block_size = value;
    }
    if (config.file_size_str != NULL) {
        if (parse_size_string(config.file_size_str, &value) == -1 || value == 0) {
            warnx("invalid file size `%s'", config.block_size_str);
            return -1;
        }
        config.file_size = value;
    }

    /* Check block cache config */
    if (config.block_cache.cache_size > 0 && config.block_cache.num_threads <= 0) {
        warnx("invalid block cache thread pool size %u", config.block_cache.num_threads);
        return -1;
    }

    /* Check bucket and mount point provided */
    if (config.http_io.bucket == NULL) {
        warnx("no S3 bucket specified");
        return -1;
    }
    if (config.mount == NULL) {
        warnx("no mount point specified");
        return -1;
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
        config.http_io.log = config.log;
        if ((s3b = http_io_create(&config.http_io)) == NULL)
            err(1, "http_io_create");
        warnx("auto-detecting block size and total file size...");
        r = http_io_detect_sizes(s3b, &auto_file_size, &auto_block_size);
        (*s3b->destroy)(s3b);
    }

    /* Check result */
    switch (r) {
    case 0:
        unparse_size_string(blockSizeBuf, sizeof(blockSizeBuf), (uintmax_t)auto_block_size);
        unparse_size_string(fileSizeBuf, sizeof(fileSizeBuf), (uintmax_t)auto_file_size);
        warnx("auto-detected block size=%s and total size=%s", blockSizeBuf, fileSizeBuf);
        if (config.block_size == 0)
            config.block_size = auto_block_size;
        else if (auto_block_size != config.block_size) {
            char buf[64];

            unparse_size_string(buf, sizeof(buf), (uintmax_t)config.block_size);
            if (config.force) {
                warnx("warning: configured block size %s != filesystem block size %s,\n"
                  "but you said `--force' so I'll proceed anyway even though your data will\n"
                  "probably not read back correctly.", buf, blockSizeBuf);
            } else
                errx(1, "error: configured block size %s != filesystem block size %s", buf, blockSizeBuf);
        }
        if (config.file_size == 0)
            config.file_size = auto_file_size;
        else if (auto_file_size != config.file_size) {
            char buf[64];

            unparse_size_string(buf, sizeof(buf), (uintmax_t)config.file_size);
            if (config.force) {
                warnx("warning: configured file size %s != filesystem file size %s,\n"
                  "but you said `--force' so I'll proceed anyway even though your data will\n"
                  "probably not read back correctly.", buf, fileSizeBuf);
            } else
                errx(1, "error: configured file size %s != filesystem file size %s", buf, fileSizeBuf);
        }
        break;
    case ENOENT:
    case ENXIO:
    {
        const char *why = config.no_auto_detect ? "disabled" : "failed";
        int config_block_size = config.block_size;

        if (config.file_size == 0)
            errx(1, "error: auto-detection of filesystem size %s; please specify `--size'", why);
        if (config.block_size == 0)
            config.block_size = S3BACKER_DEFAULT_BLOCKSIZE;
        unparse_size_string(blockSizeBuf, sizeof(blockSizeBuf), (uintmax_t)config.block_size);
        unparse_size_string(fileSizeBuf, sizeof(fileSizeBuf), (uintmax_t)config.file_size);
        warnx("auto-detection %s; using %s block size %s and file size %s", why,
          config_block_size == 0 ? "default" : "configured", blockSizeBuf, fileSizeBuf);
        break;
    }
    default:
        errno = r;
        err(1, "can't read block zero meta-data");
        break;
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
    if (config.num_blocks >= ((off_t)1 << (sizeof(s3b_block_t) * 8))) {    // cf. struct defer_info.block_num
        warnx("more than 2^%d blocks: decrease file size or increase block size", sizeof(s3b_block_t) * 8);
        return -1;
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
        if (total_time >= FUSE_MAX_DAEMON_TIMEOUT) {
            warnx("warning: maximum possible I/O delay (%us) >= MacFUSE limit (%us);", total_time, FUSE_MAX_DAEMON_TIMEOUT);
            warnx("consider lower settings for `--maxRetryPause' and/or `--timeout'.");
        }
    }
#endif  /* __APPLE__ */

    /* Copy common stuff into sub-module configs */
    config.block_cache.block_size = config.block_size;
    config.block_cache.log = config.log;
    config.http_io.debug = config.debug;
    config.http_io.block_size = config.block_size;
    config.http_io.num_blocks = config.num_blocks;
    config.http_io.log = config.log;
    config.ec_protect.block_size = config.block_size;
    config.ec_protect.log = config.log;
    config.fuse_ops.block_size = config.block_size;
    config.fuse_ops.num_blocks = config.num_blocks;
    config.fuse_ops.log = config.log;

    /* If `--listBlocks' was given, build non-empty block bitmap */
    if (config.list_blocks) {
        struct s3backer_store *temp_store;
        uintmax_t num_found;
        u_int *bitmap;

        /* Logging */
        warnx("listing non-zero blocks...");

        /* Create temporary lower layer */
        if ((temp_store = config.test ? test_io_create(&config.http_io) : http_io_create(&config.http_io)) == NULL)
            err(1, config.test ? "test_io_create" : "http_io_create");

        /* Generate non-zero block bitmap */
        assert(config.http_io.nonzero_bitmap == NULL);
        if ((r = (*temp_store->list_blocks)(temp_store, &bitmap, &num_found)) != 0)
            errx(1, "can't list blocks: %s", strerror(r));

        /* Close temporary store */
        (*temp_store->destroy)(temp_store);

        /* Save bitmap */
        config.http_io.nonzero_bitmap = bitmap;
        warnx("found %ju non-zero blocks", num_found);
    }

    /* Done */
    return 0;
}

static void
dump_config(void)
{
    int i;

    (*config.log)(LOG_DEBUG, "s3backer config:");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "test mode", config.test ? "true" : "false");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "accessId", config.http_io.accessId != NULL ? config.http_io.accessId : "");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "accessKey", config.http_io.accessKey != NULL ? "****" : "");
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "accessFile", config.accessFile);
    (*config.log)(LOG_DEBUG, "%24s: %s", "accessType", config.http_io.accessType);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "baseURL", config.http_io.baseURL);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", config.test ? "testdir" : "bucket", config.http_io.bucket);
    (*config.log)(LOG_DEBUG, "%24s: \"%s\"", "prefix", config.http_io.prefix);
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
    (*config.log)(LOG_DEBUG, "%24s: %u blocks", "read_ahead", config.block_cache.read_ahead);
    (*config.log)(LOG_DEBUG, "%24s: %u blocks", "read_ahead_trigger", config.block_cache.read_ahead_trigger);
    (*config.log)(LOG_DEBUG, "fuse_main arguments:");
    for (i = 0; i < config.fuse_args.argc; i++)
        (*config.log)(LOG_DEBUG, "  [%d] = \"%s\"", i, config.fuse_args.argv[i]);
}

static void
syslog_logger(int level, const char *fmt, ...)
{
    va_list args;

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
    time_t now;

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
    time(&now);
    strftime(timebuf, sizeof(timebuf), "%F %T", localtime(&now));
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

    fprintf(stderr, "Usage: s3backer [options] bucket /mount/point\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "\t--%-27s %s\n", "accessFile=FILE", "File containing `accessID:accessKey' pairs");
    fprintf(stderr, "\t--%-27s %s\n", "accessId=ID", "S3 access key ID");
    fprintf(stderr, "\t--%-27s %s\n", "accessKey=KEY", "S3 secret access key");
    fprintf(stderr, "\t--%-27s %s\n", "accessType=TYPE", "S3 ACL used when creating new items; one of:");
    fprintf(stderr, "\t  %-27s ", "");
    for (i = 0; i < sizeof(s3_acls) / sizeof(*s3_acls); i++)
        fprintf(stderr, "%s%s", i > 0 ? ", " : "  ", s3_acls[i]);
    fprintf(stderr, "\n");
    fprintf(stderr, "\t--%-27s %s\n", "baseURL=URL", "Base URL for all requests");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheSize=NUM", "Block cache size");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheThreads=NUM", "Block cache write-back thread pool size");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheTimeout=MILLIS", "Block cache entry timeout (zero = infinite)");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheWriteDelay=MILLIS", "Block cache maximum write-back delay");
    fprintf(stderr, "\t--%-27s %s\n", "blockSize=SIZE", "Block size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-27s %s\n", "cacert=FILE", "Specify SSL certificate authority file");
    fprintf(stderr, "\t--%-27s %s\n", "md5CacheSize=NUM", "Max size of MD5 cache (zero = disabled)");
    fprintf(stderr, "\t--%-27s %s\n", "md5CacheTime=MILLIS", "Expire time for MD5 cache (zero = infinite)");
    fprintf(stderr, "\t--%-27s %s\n", "timeout=SECONDS", "Max time allowed for one HTTP operation");
    fprintf(stderr, "\t--%-27s %s\n", "debug", "Enable logging of debug messages");
    fprintf(stderr, "\t--%-27s %s\n", "filename=NAME", "Name of backed file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "fileMode=MODE", "Permissions of backed file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "force", "Ignore different auto-detected block and file sizes");
    fprintf(stderr, "\t--%-27s %s\n", "initialRetryPause=MILLIS", "Inital retry pause after stale data or server error");
    fprintf(stderr, "\t--%-27s %s\n", "insecure", "Don't verify SSL server identity");
    fprintf(stderr, "\t--%-27s %s\n", "listBlocks", "Auto-detect non-empty blocks at startup");
    fprintf(stderr, "\t--%-27s %s\n", "maxRetryPause=MILLIS", "Max total pause after stale data or server error");
    fprintf(stderr, "\t--%-27s %s\n", "minWriteDelay=MILLIS", "Minimum time between same block writes");
    fprintf(stderr, "\t--%-27s %s\n", "prefix=STRING", "Prefix for resource names within bucket");
    fprintf(stderr, "\t--%-27s %s\n", "readAhead=NUM", "Number of blocks to read-ahead");
    fprintf(stderr, "\t--%-27s %s\n", "readAheadTrigger=NUM", "# of sequentially read blocks to trigger read-ahead");
    fprintf(stderr, "\t--%-27s %s\n", "readOnly", "Return `Read-only file system' error for write attempts");
    fprintf(stderr, "\t--%-27s %s\n", "size=SIZE", "File size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-27s %s\n", "ssl", "Same as --baseURL " S3_BASE_URL_HTTPS);
    fprintf(stderr, "\t--%-27s %s\n", "statsFilename=NAME", "Name of statistics file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "test", "Run in local test mode (bucket is a directory)");
    fprintf(stderr, "\t--%-27s %s\n", "timeout=SECONDS", "Specify HTTP operation timeout");
    fprintf(stderr, "\t--%-27s %s\n", "version", "Show version information and exit");
    fprintf(stderr, "\t--%-27s %s\n", "help", "Show this information and exit");
    fprintf(stderr, "Default values:\n");
    fprintf(stderr, "\t--%-27s \"%s\"\n", "accessFile", "$HOME/" S3BACKER_DEFAULT_PWD_FILE);
    fprintf(stderr, "\t--%-27s %s\n", "accessId", "The first one listed in `accessFile'");
    fprintf(stderr, "\t--%-27s \"%s\"\n", "accessType", S3BACKER_DEFAULT_ACCESS_TYPE);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "baseURL", S3BACKER_DEFAULT_BASE_URL);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheSize", S3BACKER_DEFAULT_BLOCK_CACHE_SIZE);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheThreads", S3BACKER_DEFAULT_BLOCK_CACHE_NUM_THREADS);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheTimeout", S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT);
    fprintf(stderr, "\t--%-27s %u\n", "blockCacheWriteDelay", S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY);
    fprintf(stderr, "\t--%-27s %d\n", "blockSize", S3BACKER_DEFAULT_BLOCKSIZE);
    fprintf(stderr, "\t--%-27s %u\n", "md5CacheSize", S3BACKER_DEFAULT_MD5_CACHE_SIZE);
    fprintf(stderr, "\t--%-27s %u\n", "md5CacheTime", S3BACKER_DEFAULT_MD5_CACHE_TIME);
    fprintf(stderr, "\t--%-27s %u\n", "timeout", S3BACKER_DEFAULT_TIMEOUT);
    fprintf(stderr, "\t--%-27s 0%03o (0%03o if `--readOnly')\n", "fileMode", S3BACKER_DEFAULT_FILE_MODE, S3BACKER_DEFAULT_FILE_MODE_READ_ONLY);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "filename", S3BACKER_DEFAULT_FILENAME);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "statsFilename", S3BACKER_DEFAULT_STATS_FILENAME);
    fprintf(stderr, "\t--%-27s %u\n", "initialRetryPause", S3BACKER_DEFAULT_INITIAL_RETRY_PAUSE);
    fprintf(stderr, "\t--%-27s %u\n", "maxRetryPause", S3BACKER_DEFAULT_MAX_RETRY_PAUSE);
    fprintf(stderr, "\t--%-27s %u\n", "minWriteDelay", S3BACKER_DEFAULT_MIN_WRITE_DELAY);
    fprintf(stderr, "\t--%-27s \"%s\"\n", "prefix", S3BACKER_DEFAULT_PREFIX);
    fprintf(stderr, "\t--%-27s %u\n", "readAhead", S3BACKER_DEFAULT_READ_AHEAD);
    fprintf(stderr, "\t--%-27s %u\n", "readAheadTrigger", S3BACKER_DEFAULT_READ_AHEAD_TRIGGER);
    fprintf(stderr, "FUSE options (partial list):\n");
    fprintf(stderr, "\t%-29s %s\n", "-o allow_root", "Allow root (only) to view backed file");
    fprintf(stderr, "\t%-29s %s\n", "-o allow_other", "Allow all users to view backed file");
    fprintf(stderr, "\t%-29s %s\n", "-o nonempty", "Allows mount over a non-empty directory");
    fprintf(stderr, "\t%-29s %s\n", "-o uid=UID", "Set user ID");
    fprintf(stderr, "\t%-29s %s\n", "-o gid=GID", "Set group ID");
    fprintf(stderr, "\t%-29s %s\n", "-o sync_read", "Do synchronous reads");
    fprintf(stderr, "\t%-29s %s\n", "-o max_readahead=NUM", "Set maximum read-ahead (bytes)");
    fprintf(stderr, "\t%-29s %s\n", "-f", "Run in the foreground (do not fork)");
    fprintf(stderr, "\t%-29s %s\n", "-d", "Debug mode (implies -f)");
    fprintf(stderr, "\t%-29s %s\n", "-s", "Run in single-threaded mode");
}

