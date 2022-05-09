
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
#include "block_cache.h"
#include "zero_cache.h"
#include "ec_protect.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"
#include "dcache.h"
#include "compress.h"
#include "util.h"

/****************************************************************************
 *                          DEFINITIONS                                     *
 ****************************************************************************/

// S3 URL
#define S3_DOMAIN                                   "amazonaws.com"

// S3 access permission strings
#define S3_ACCESS_PRIVATE                           "private"
#define S3_ACCESS_PUBLIC_READ                       "public-read"
#define S3_ACCESS_PUBLIC_READ_WRITE                 "public-read-write"
#define S3_ACCESS_AUTHENTICATED_READ                "authenticated-read"

// Default values for some configuration parameters
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
#define S3BACKER_DEFAULT_MIN_WRITE_DELAY            0               // disabled
#define S3BACKER_DEFAULT_MD5_CACHE_TIME             0               // disabled
#define S3BACKER_DEFAULT_MD5_CACHE_SIZE             0               // disabled
#define S3BACKER_DEFAULT_BLOCK_CACHE_SIZE           1000
#define S3BACKER_DEFAULT_BLOCK_CACHE_NUM_THREADS    20
#define S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY    250             // 250ms
#define S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT        0
#define S3BACKER_DEFAULT_BLOCK_CACHE_MAX_DIRTY      0
#define S3BACKER_DEFAULT_READ_AHEAD                 4
#define S3BACKER_DEFAULT_READ_AHEAD_TRIGGER         2
#define S3BACKER_DEFAULT_COMPRESSION                "deflate"
#define S3BACKER_DEFAULT_ENCRYPTION                 "AES-128-CBC"
#define S3BACKER_DEFAULT_LIST_BLOCKS_THREADS        16

// Macro for quoting stuff
#define s3bquote0(x)                    #x
#define s3bquote(x)                     s3bquote0(x)

// MacFUSE setting for kernel daemon timeout
#ifdef __APPLE__
#ifndef FUSE_MAX_DAEMON_TIMEOUT
#define FUSE_MAX_DAEMON_TIMEOUT         600
#endif
#define FUSE_MAX_DAEMON_TIMEOUT_STRING  s3bquote(FUSE_MAX_DAEMON_TIMEOUT)
#endif  // __APPLE__

/****************************************************************************
 *                          FUNCTION DECLARATIONS                           *
 ****************************************************************************/

static print_stats_t s3b_config_print_stats;
static clear_stats_t s3b_config_clear_stats;

static void insert_fuse_arg(int pos, const char *arg);
static void append_fuse_arg(const char *arg);
static void remove_fuse_arg(int pos);
static void read_fuse_args(const char *filename, int pos);
static int search_access_for(const char *file, const char *accessId, char **idptr, char **pwptr);
static int handle_unknown_option(void *data, const char *arg, int key, struct fuse_args *outargs);
static int validate_config(void);
static void usage(void);

/****************************************************************************
 *                          VARIABLE DEFINITIONS                            *
 ****************************************************************************/

// Upload/download strings
static const char *const upload_download_names[] = { "download", "upload" };

// Valid S3 access values
static const char *const s3_acls[] = {
    S3_ACCESS_PRIVATE,
    S3_ACCESS_PUBLIC_READ,
    S3_ACCESS_PUBLIC_READ_WRITE,
    S3_ACCESS_AUTHENTICATED_READ,
    NULL
};

// Valid S3 authentication types
static const char *const s3_auth_types[] = {
    AUTH_VERSION_AWS2,
    AUTH_VERSION_AWS4,
    NULL
};

// Valid S3 storage classes
static const char *const s3_storage_classes[] = {
    STORAGE_CLASS_STANDARD,
    STORAGE_CLASS_STANDARD_IA,
    STORAGE_CLASS_ONEZONE_IA,
    STORAGE_CLASS_REDUCED_REDUNDANCY,
    STORAGE_CLASS_INTELLIGENT_TIERING,
    STORAGE_CLASS_GLACIER,
    STORAGE_CLASS_DEEP_ARCHIVE,
    STORAGE_CLASS_OUTPOSTS,
    NULL
};

// Configuration structure
static char user_agent_buf[64];
static struct s3b_config config = {

    // HTTP config
    .http_io= {
        .accessId=              NULL,
        .accessKey=             NULL,
        .baseURL=               NULL,
        .region=                NULL,
        .sse=                   NULL,
        .sse_key_id=            NULL,
        .accessType=            S3BACKER_DEFAULT_ACCESS_TYPE,
        .authVersion=           S3BACKER_DEFAULT_AUTH_VERSION,
        .user_agent=            user_agent_buf,
        .timeout=               S3BACKER_DEFAULT_TIMEOUT,
        .initial_retry_pause=   S3BACKER_DEFAULT_INITIAL_RETRY_PAUSE,
        .max_retry_pause=       S3BACKER_DEFAULT_MAX_RETRY_PAUSE,
        .list_blocks_threads=   S3BACKER_DEFAULT_LIST_BLOCKS_THREADS,
    },

    // "Eventual consistency" protection config
    .ec_protect= {
        .min_write_delay=       S3BACKER_DEFAULT_MIN_WRITE_DELAY,
        .cache_time=            S3BACKER_DEFAULT_MD5_CACHE_TIME,
        .cache_size=            S3BACKER_DEFAULT_MD5_CACHE_SIZE,
    },

    // Block cache config
    .block_cache= {
        .cache_size=            S3BACKER_DEFAULT_BLOCK_CACHE_SIZE,
        .num_threads=           S3BACKER_DEFAULT_BLOCK_CACHE_NUM_THREADS,
        .write_delay=           S3BACKER_DEFAULT_BLOCK_CACHE_WRITE_DELAY,
        .max_dirty=             S3BACKER_DEFAULT_BLOCK_CACHE_MAX_DIRTY,
        .timeout=               S3BACKER_DEFAULT_BLOCK_CACHE_TIMEOUT,
        .read_ahead=            S3BACKER_DEFAULT_READ_AHEAD,
        .read_ahead_trigger=    S3BACKER_DEFAULT_READ_AHEAD_TRIGGER,
    },

    // FUSE operations config
    .fuse_ops= {
        .filename=              S3BACKER_DEFAULT_FILENAME,
        .stats_filename=        S3BACKER_DEFAULT_STATS_FILENAME,
        .file_mode=             -1,             // default depends on 'read_only'
    },

    // Common/global stuff
    .block_size=            0,
    .file_size=             0,
    .bucket=                NULL,
    .prefix=                S3BACKER_DEFAULT_PREFIX,
    .accessKeyEnv=          NULL,
    .blockHashPrefix=       0,
    .quiet=                 0,
    .erase=                 0,
    .no_auto_detect=        0,
    .reset=                 0,
    .log=                   stderr_logger
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
        .templ=     "--accessKeyEnv=%s",
        .offset=    offsetof(struct s3b_config, accessKeyEnv),
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
        .templ=     "--listBlocksThreads=%d",
        .offset=    offsetof(struct s3b_config, http_io.list_blocks_threads),
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
        .templ=     "--sse-key-id=%s",
        .offset=    offsetof(struct s3b_config, http_io.sse_key_id),
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
        .templ=     "--blockCacheRecoverDirtyBlocks",
        .offset=    offsetof(struct s3b_config, block_cache.recover_dirty_blocks),
        .value=     1
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
        .templ=     "--blockCacheNumProtected=%u",
        .offset=    offsetof(struct s3b_config, block_cache.num_protected),
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
        .templ=     "--blockCacheFileAdvise",
        .offset=    offsetof(struct s3b_config, block_cache.fadvise),
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
        .templ=     "--http11",
        .offset=    offsetof(struct s3b_config, http_io.http_11),
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
        .templ=     "--no-vhost",
        .offset=    offsetof(struct s3b_config, http_io.vhost),
        .value=     -1
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
        .templ=     "--blockHashPrefix",
        .offset=    offsetof(struct s3b_config, blockHashPrefix),
        .value=     1
    },
    {
        .templ=     "--prefix=%s",
        .offset=    offsetof(struct s3b_config, prefix),
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
        .offset=    offsetof(struct s3b_config, compress_flag),
        .value=     1
    },
    {
        .templ=     "--compress=%s",
        .offset=    offsetof(struct s3b_config, compress_alg),
    },
    {
        .templ=     "--compress-level=%s",
        .offset=    offsetof(struct s3b_config, compress_level),
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
        .templ=     "--test-errors",
        .offset=    offsetof(struct s3b_config, test_io.random_errors),
        .value=     1
    },
    {
        .templ=     "--test-delays",
        .offset=    offsetof(struct s3b_config, test_io.random_delays),
        .value=     1
    },
    {
        .templ=     "--test-discard",
        .offset=    offsetof(struct s3b_config, test_io.discard_data),
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

// Default flags we send to FUSE
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
//  "-ointr",
};

// s3backer_store layers
struct s3backer_store *block_cache_store;
struct s3backer_store *zero_cache_store;
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
    int num_subst = 0;
    char buf[1024];
    int i;

    // Remember user creds
    config.fuse_ops.uid = getuid();
    config.fuse_ops.gid = getgid();

    // Set user-agent
    snvprintf(user_agent_buf, sizeof(user_agent_buf), "%s/%s/%s", PACKAGE, VERSION, s3backer_version);

    // Copy program name
    memset(&config.fuse_args, 0, sizeof(config.fuse_args));
    if (argc < 1)
        errx(1, "missing program name");
    append_fuse_arg(argv[0]);

    // Add our default FUSE options so they are seen first
    for (i = 0; i < sizeof(s3backer_fuse_defaults) / sizeof(*s3backer_fuse_defaults); i++)
        append_fuse_arg(s3backer_fuse_defaults[i]);

    // Append command line args
    for (i = 1; i < argc; i++)
        append_fuse_arg(argv[i]);

    // Find and substitute "--configFile=FILE" flags, recursing if necessary
    for (i = 1; i < config.fuse_args.argc; ) {
        char *optfile;
        int num_args;

        // Check for "--configFile=FILE" flag and variants
        if (strncmp(config.fuse_args.argv[i], "--configFile=", 13) == 0) {
            if ((optfile = strdup(config.fuse_args.argv[i] + 13)) == NULL)
                err(1, "strdup");
            num_args = 1;
        } else if (strcmp(config.fuse_args.argv[i], "-F") == 0 && i + 1 < config.fuse_args.argc) {
            if ((optfile = strdup(config.fuse_args.argv[i + 1])) == NULL)
                err(1, "strdup");
            num_args = 2;
        } else if (strcmp(config.fuse_args.argv[i], "-o") == 0 && i + 1 < config.fuse_args.argc) {
            char *trailing_comma;
            char *flag;

            // Find "configFile=" among comma-separated option list
            if (strncmp(config.fuse_args.argv[i + 1], "configFile=", 11) == 0)
                flag = config.fuse_args.argv[i + 1];
            else if ((flag = strstr(config.fuse_args.argv[i + 1], ",configFile=")) != NULL)
                flag++;
            else {
                i++;
                continue;
            }

            // Extract filename from list
            if ((trailing_comma = strchr(flag + 11, ',')) == NULL) {
                if ((optfile = strdup(flag + 11)) == NULL)
                    err(1, "strdup");
                *flag = '\0';
                num_args = flag > config.fuse_args.argv[i + 1] ? 0 : 2;
            } else {
                *trailing_comma = '\0';
                if ((optfile = strdup(flag + 11)) == NULL)
                    err(1, "strdup");
                memmove(flag, trailing_comma + 1, strlen(trailing_comma + 1) + 1);
                num_args = 0;
            }
        } else {
            i++;
            continue;
        }

        // Check for infinite loops
        if (++num_subst > 100)
            errx(1, "too many levels of `--configFile' nesting");

        // Replace the `--configFile' flag with arguments read from file
        read_fuse_args(optfile, i + num_args);
        free(optfile);
        while (num_args-- > 0)
            remove_fuse_arg(i);
    }

    // Create the equivalent fstab options (without the "--") for each option in the option list
    memcpy(dup_option_list, option_list, sizeof(option_list));
    memcpy(dup_option_list + num_options, option_list, sizeof(option_list));
    for (i = num_options; i < 2 * num_options; i++)
        dup_option_list[i].templ += 2;
    dup_option_list[2 * num_options].templ = NULL;

    // Parse command line flags
    if (fuse_opt_parse(&config.fuse_args, &config, dup_option_list, handle_unknown_option) != 0)
        return NULL;

    // Validate configuration
    if (validate_config() != 0)
        return NULL;

    // Set fsname based on configuration
    snvprintf(buf, sizeof(buf), "-ofsname=%s", config.description);
    insert_fuse_arg(1, buf);

    // Set up fuse_ops callbacks
    config.fuse_ops.print_stats = s3b_config_print_stats;
    config.fuse_ops.clear_stats = s3b_config_clear_stats;
    config.fuse_ops.s3bconf = &config;

    // Debug
    if (config.debug)
        dump_config(&config);

    // Done
    return &config;
}

/*
 * Create the s3backer_store used at runtime.
 */
struct s3backer_store *
s3backer_create_store(struct s3b_config *conf)
{
    struct s3backer_store *store;
    int32_t old_mount_token;
    int32_t new_mount_token;
    int r;

    // Sanity check
    if (http_io_store != NULL || test_io_store != NULL) {
        errno = EINVAL;
        return NULL;
    }

    // Create HTTP (or test) layer
    if (conf->test) {
        if ((test_io_store = test_io_create(&conf->test_io)) == NULL)
            return NULL;
        store = test_io_store;
    } else {
        if ((http_io_store = http_io_create(&conf->http_io)) == NULL)
            return NULL;
        store = http_io_store;
    }

    // Create eventual consistency protection layer (if desired)
    if (conf->ec_protect.cache_size > 0) {
        if ((ec_protect_store = ec_protect_create(&conf->ec_protect, store)) == NULL)
            goto fail_with_errno;
        store = ec_protect_store;
    }

    // Create block cache layer (if desired)
    if (conf->block_cache.cache_size > 0) {
        if ((block_cache_store = block_cache_create(&conf->block_cache, store)) == NULL)
            goto fail_with_errno;
        store = block_cache_store;
    }

    // Create zero block cache
    if ((zero_cache_store = zero_cache_create(&conf->zero_cache, store)) == NULL)
        goto fail_with_errno;
    store = zero_cache_store;

    // Set mount token and check previous value one last time
    new_mount_token = -1;
    if (!conf->fuse_ops.read_only) {
        srandom((long)time(NULL) ^ (long)&old_mount_token);
        do
            new_mount_token = random();
        while (new_mount_token <= 0);
    }
    if ((r = (*store->set_mount_token)(store, &old_mount_token, new_mount_token)) != 0) {
        (*conf->log)(LOG_ERR, "error reading mount token on %s: %s", conf->description, strerror(r));
        goto fail;
    }
    if (old_mount_token != 0) {
        if (!conf->force && !conf->block_cache.perform_flush) {
            (*conf->log)(LOG_ERR, "%s appears to be mounted by another s3backer process (using mount token 0x%08x)",
              config.description, (int)old_mount_token);
            r = EBUSY;
            goto fail;
        }
    }
    if (new_mount_token != -1)
        (*conf->log)(LOG_INFO, "established new mount token 0x%08x", (int)new_mount_token);

    // Done
    return store;

fail_with_errno:
    r = errno;
fail:
    if (store != NULL) {
        (*store->shutdown)(store);
        (*store->destroy)(store);
    }
    block_cache_store = NULL;
    zero_cache_store = NULL;
    ec_protect_store = NULL;
    http_io_store = NULL;
    test_io_store = NULL;
    errno = r;
    return NULL;
}

#define FORCE_FREE2(p, def) do {                                        \
                                if ((p) != NULL && (p) != (def)) {      \
                                    free((void *)(intptr_t)(p));        \
                                    (p) = NULL;                         \
                                }                                       \
                            } while (0)
#define FORCE_FREE(p)       FORCE_FREE2(p, NULL)

// Free memory prior to unload/exit. This is mainly to make valgrind happy.
void
s3b_cleanup(void)
{
    // Config flags
    FORCE_FREE(config.http_io.accessId);
    FORCE_FREE(config.http_io.accessKey);
    FORCE_FREE(config.accessKeyEnv);
    FORCE_FREE2(config.http_io.accessType, S3BACKER_DEFAULT_ACCESS_TYPE);
    FORCE_FREE(config.http_io.ec2iam_role);
    FORCE_FREE2(config.http_io.authVersion, S3BACKER_DEFAULT_AUTH_VERSION);
    FORCE_FREE(config.http_io.baseURL);
    FORCE_FREE2(config.http_io.region, S3BACKER_DEFAULT_REGION);
    FORCE_FREE(config.http_io.sse);
    FORCE_FREE(config.http_io.sse_key_id);
    FORCE_FREE(config.block_cache.cache_file);
    FORCE_FREE(config.block_size_str);
    FORCE_FREE(config.max_speed_str[HTTP_UPLOAD]);
    FORCE_FREE(config.max_speed_str[HTTP_DOWNLOAD]);
    FORCE_FREE2(config.prefix, S3BACKER_DEFAULT_PREFIX);
    FORCE_FREE(config.http_io.default_ce);
    FORCE_FREE(config.http_io.vhostURL);
    FORCE_FREE(config.file_size_str);
    FORCE_FREE2(config.fuse_ops.filename, S3BACKER_DEFAULT_FILENAME);
    FORCE_FREE2(config.fuse_ops.stats_filename, S3BACKER_DEFAULT_STATS_FILENAME);
    FORCE_FREE(config.http_io.storage_class);
    FORCE_FREE(config.http_io.cacert);
    FORCE_FREE2(config.compress_alg, S3BACKER_DEFAULT_COMPRESSION);
    FORCE_FREE(config.compress_level);
    FORCE_FREE(config.http_io.encryption);
    FORCE_FREE(config.http_io.password);
    FORCE_FREE(config.password_file);

    // Config params
    FORCE_FREE(config.bucket);
    FORCE_FREE(config.mount);

    // Misc
    FORCE_FREE(zero_block);
    fuse_opt_free_args(&config.fuse_args);

    // Done
    memset(&config, 0, sizeof(config));
}

/****************************************************************************
 *                    INTERNAL FUNCTION DEFINITIONS                         *
 ****************************************************************************/

static void
s3b_config_print_stats(void *prarg, printer_t *printer)
{
    struct http_io_stats http_io_stats;
    struct ec_protect_stats ec_protect_stats;
    struct zero_cache_stats zero_cache_stats;
    struct block_cache_stats block_cache_stats;
    double curl_reuse_ratio = 0.0;
    u_int total_oom = 0;
    u_int total_curls;

    // Get HTTP stats
    if (http_io_store != NULL)
        http_io_get_stats(http_io_store, &http_io_stats);

    // Get zero cache stats
    if (zero_cache_store != NULL)
        zero_cache_get_stats(zero_cache_store, &zero_cache_stats);

    // Get EC protection stats
    if (ec_protect_store != NULL)
        ec_protect_get_stats(ec_protect_store, &ec_protect_stats);

    // Get block cache stats
    if (block_cache_store != NULL)
        block_cache_get_stats(block_cache_store, &block_cache_stats);

    // Print stats in human-readable form
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
        (*printer)(prarg, "%-28s %u\n", "http_redirect", http_io_stats.http_redirect);
        (*printer)(prarg, "%-28s %u\n", "http_verified", http_io_stats.http_verified);
        (*printer)(prarg, "%-28s %u\n", "http_mismatch", http_io_stats.http_mismatch);
        (*printer)(prarg, "%-28s %u\n", "http_5xx_error", http_io_stats.http_5xx_error);
        (*printer)(prarg, "%-28s %u\n", "http_4xx_error", http_io_stats.http_4xx_error);
        (*printer)(prarg, "%-28s %u\n", "http_3xx_error", http_io_stats.http_3xx_error);
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
        (*printer)(prarg, "%-28s %.8f\n", "block_cache_dirty_ratio", block_cache_stats.dirty_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_read_hits", block_cache_stats.read_hits);
        (*printer)(prarg, "%-28s %u\n", "block_cache_read_misses", block_cache_stats.read_misses);
        (*printer)(prarg, "%-28s %.8f\n", "block_cache_read_hit_ratio", read_hit_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_write_hits", block_cache_stats.write_hits);
        (*printer)(prarg, "%-28s %u\n", "block_cache_write_misses", block_cache_stats.write_misses);
        (*printer)(prarg, "%-28s %.8f\n", "block_cache_write_hit_ratio", write_hit_ratio);
        (*printer)(prarg, "%-28s %u\n", "block_cache_verified", block_cache_stats.verified);
        (*printer)(prarg, "%-28s %u\n", "block_cache_mismatch", block_cache_stats.mismatch);
        total_oom += block_cache_stats.out_of_memory_errors;
    }
    if (zero_cache_store != NULL) {
        (*printer)(prarg, "%-28s %ju blocks\n", "zero_block_cache_size", (uintmax_t)zero_cache_stats.current_cache_size);
        (*printer)(prarg, "%-28s %u\n", "zero_block_cache_read_hits", zero_cache_stats.read_hits);
        (*printer)(prarg, "%-28s %u\n", "zero_block_cache_write_hits", zero_cache_stats.write_hits);
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

static void
s3b_config_clear_stats(void)
{
    // Clear HTTP stats
    if (http_io_store != NULL)
        http_io_clear_stats(http_io_store);

    // Clear EC protection stats
    if (ec_protect_store != NULL)
        ec_protect_clear_stats(ec_protect_store);

    // Clear zero block cache stats
    if (zero_cache_store != NULL)
        zero_cache_clear_stats(zero_cache_store);

    // Clear block cache stats
    if (block_cache_store != NULL)
        block_cache_clear_stats(block_cache_store);
}

static void
insert_fuse_arg(int pos, const char *arg)
{
    assert(pos >= 0 && pos <= config.fuse_args.argc);
    if (fuse_opt_insert_arg(&config.fuse_args, pos, arg) != 0)
        err(1, "fuse_opt_insert_arg");
}

static void
append_fuse_arg(const char *arg)
{
    insert_fuse_arg(config.fuse_args.argc, arg);
}

static void
remove_fuse_arg(int pos)
{
    assert(pos >= 0 && pos < config.fuse_args.argc);
    if (config.fuse_args.allocated)
        free(config.fuse_args.argv[pos]);
    memmove(config.fuse_args.argv + pos, config.fuse_args.argv + pos + 1,
      (config.fuse_args.argc-- - pos) * sizeof(*config.fuse_args.argv));        // include trailing NULL
}

static void
read_fuse_args(const char *filename, int pos)
{
    char buf[1024];
    int lineno;
    char *arg;
    FILE *fp;

    if ((fp = fopen(filename, "r")) == NULL)
        err(1, "%s", filename);
    for (lineno = 1; fgets(buf, sizeof(buf), fp) != NULL; lineno++) {

        // Check for buffer overflow
        if (*buf != '\0' && buf[strlen(buf) - 1] != '\n' && !feof(fp))
            errx(1, "%s:%d: line too long", filename, lineno);

        // Trim whitespace fore & aft
        arg = buf;
        while (isspace(*arg))
            arg++;
        while (*arg != '\0' && isspace(arg[strlen(arg) - 1]))
            arg[strlen(arg) - 1] = '\0';

        // Ignore blank lines and comments
        if (*arg != '\0' && *arg != '#')
            insert_fuse_arg(pos++, arg);
    }
    if (ferror(fp))
        err(1, "%s", filename);
    fclose(fp);
}

/**
 * Handle command-line flag.
 */
static int
handle_unknown_option(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    // Check options
    if (key == FUSE_OPT_KEY_OPT) {

        // Debug flags
        if (strcmp(arg, "-d") == 0) {
            config.debug = 1;
            config.foreground = 1;
        }
        if (strcmp(arg, "-f") == 0)
            config.foreground = 1;

        // Version
        if (strcmp(arg, "--version") == 0 || strcmp(arg, "-v") == 0) {
            fprintf(stderr, "%s version %s (%s)\n", PACKAGE, VERSION, s3backer_version);
            fprintf(stderr, "Copyright (C) 2008-2020 Archie L. Cobbs.\n");
            fprintf(stderr, "This is free software; see the source for copying conditions.  There is NO\n");
            fprintf(stderr, "warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n");
            exit(0);
        }

        // Help
        if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0 || strcmp(arg, "-?") == 0) {
            usage();
            exit(0);
        }

        // Unknown; pass it through to fuse_main()
        return 1;
    }

    // Get bucket parameter
    if (config.bucket == NULL) {
        if ((config.bucket = strdup(arg)) == NULL)
            err(1, "strdup");
        return 0;
    }

    // Copy mount point
    if (config.mount == NULL) {
        if ((config.mount = strdup(arg)) == NULL)
            err(1, "strdup");
        return 1;
    }

    // Pass subsequent parameters on to fuse_main()
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
    off_t big_num_blocks;
    uintmax_t value;
    const char *s;
    char blockSizeBuf[64];
    char fileSizeBuf[64];
    struct stat sb;
    char urlbuf[512];
    char *pbuf;
    char *p;
    int i;
    int r;

    // Default to $HOME/.s3backer for accessFile
    if (config.http_io.ec2iam_role == NULL && config.accessFile == NULL) {
        const char *home = getenv("HOME");
        char buf[PATH_MAX];

        if (home != NULL) {
            snvprintf(buf, sizeof(buf), "%s/%s", home, S3BACKER_DEFAULT_PWD_FILE);
            if ((config.accessFile = strdup(buf)) == NULL)
                err(1, "strdup");
        }
    }

    // Auto-set file mode in read_only if not explicitly set
    if (config.fuse_ops.file_mode == -1) {
        config.fuse_ops.file_mode = config.fuse_ops.read_only ?
          S3BACKER_DEFAULT_FILE_MODE_READ_ONLY : S3BACKER_DEFAULT_FILE_MODE;
    }

    // If no accessId specified, default to first in accessFile
    if (config.http_io.accessId == NULL && config.accessFile != NULL)
        search_access_for(config.accessFile, NULL, &config.http_io.accessId, NULL);
    if (config.http_io.accessId != NULL && *config.http_io.accessId == '\0')
        config.http_io.accessId = NULL;

    // If no accessId, only read operations will succeed
    if (!config.test && config.http_io.accessId == NULL
      && !config.fuse_ops.read_only && !customBaseURL && config.http_io.ec2iam_role == NULL) {
        warnx("warning: no `accessId' specified; only read operations will succeed");
        warnx("you can eliminate this warning by providing the `--readOnly' flag");
    }

    // Read accessKey from environment variable if specified
    if (config.accessKeyEnv != NULL) {
        if (config.http_io.accessKey != NULL) {
            warnx("flags `accessKey' and `accessKeyEnv' are mutually exclusive");
            return -1;
        }
        if ((p = getenv(config.accessKeyEnv)) == NULL) {
            warnx("`accessKeyEnv' environment variable `%s' not found", config.accessKeyEnv);
            return -1;
        }
        if ((config.http_io.accessKey = strdup(p)) == NULL)
            err(1, "strdup");
    }

    // Find key in file if not specified explicitly
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

    // Check for conflict between explicit accessId and EC2 IAM role
    if (config.http_io.accessId != NULL && config.http_io.ec2iam_role != NULL) {
        warnx("an `accessKey' must not be specified when an `accessEC2IAM' role is specified");
        return -1;
    }

    // Check auth version
    if (!find_string_in_table(s3_auth_types, config.http_io.authVersion)) {
        warnx("illegal authentication version `%s'", config.http_io.authVersion);
        return -1;
    }

    // Check bucket/testdir; extract prefix from bucket if slash is present
    if (!config.test) {
        if (config.bucket == NULL) {
            warnx("no S3 bucket specified");
            return -1;
        }
        if (*config.bucket == '\0' || *config.bucket == '/') {
            warnx("invalid S3 bucket `%s'", config.bucket);
            return -1;
        }
        if ((p = strchr(config.bucket, '/')) != NULL) {

            // Can't use bucket+prefix and --prefix at the same time
            if (*config.prefix != '\0') {
                warnx("S3 bucket/prefix `%s' conflicts with `--prefix' flag", config.bucket);
                return -1;
            }

            // Disallow empty string, or initial, trailing, or duplicate slashes in directory name
            p++;
            if (*p == '\0' || *p == '/' || p[strlen(p) - 1] == '/' || strstr(p, "//") != NULL) {
                warnx("invalid S3 bucket/prefix `%s'", config.bucket);
                return -1;
            }

            // Terminate bucket name at the slash
            p[-1] = '\0';

            // Copy what follows the slash, with another slash added on, as the new prefix
            if ((pbuf = malloc(strlen(p) + 2)) == NULL) {
                warn("malloc");
                return -1;
            }
            snvprintf(pbuf, strlen(p) + 2, "%s/", p);
            config.prefix = pbuf;
        }
    } else {
        if (config.bucket == NULL) {
            warnx("no test directory specified");
            return -1;
        }
        if (!config.foreground && *config.bucket != '/') {
            warnx("%s: absolute pathname required for test mode directory unless `-f' flag is used", config.bucket);
            return -1;
        }
        if (stat(config.bucket, &sb) == -1) {
            warn("%s", config.bucket);
            return -1;
        }
        if (!S_ISDIR(sb.st_mode)) {
            errno = ENOTDIR;
            warn("%s", config.bucket);
            return -1;
        }
    }

    // Check storage class
    if (config.http_io.storage_class != NULL && !find_string_in_table(s3_storage_classes, config.http_io.storage_class)) {
        warnx("invalid storage class `%s'", config.http_io.storage_class);
        return -1;
    }

    // Check server side encryption type and get key ID if needed
    if (config.http_io.sse != NULL) {
        if (strcmp(config.http_io.sse, SSE_AWS_KMS) == 0) {
            if (config.http_io.sse_key_id == NULL) {
                warnx("`--sse-key-id' flag is required when `--sse' flag is used");
                return -1;
            }
        } else if (strcmp(config.http_io.sse, SSE_AES256) != 0) {
            warnx("unknown sse type `%s'", config.http_io.sse);
            return -1;
        }
    }

    // Set default or custom region
    if (config.http_io.region == NULL)
        config.http_io.region = S3BACKER_DEFAULT_REGION;
    if (customRegion && config.http_io.vhost != -1)
        config.http_io.vhost = 1;

    // Handle --no-vhost
    if (config.http_io.vhost == -1)
        config.http_io.vhost = 0;

    // Set default base URL
    if (config.http_io.baseURL == NULL) {
        if (customRegion && strcmp(config.http_io.region, S3BACKER_DEFAULT_REGION) != 0)
            snvprintf(urlbuf, sizeof(urlbuf), "http%s://s3.%s.%s/", config.ssl ? "s" : "", config.http_io.region, S3_DOMAIN);
        else
            snvprintf(urlbuf, sizeof(urlbuf), "http%s://s3.%s/", config.ssl ? "s" : "", S3_DOMAIN);
        if ((config.http_io.baseURL = strdup(urlbuf)) == NULL) {
            warn("malloc");
            return -1;
        }
    }

    // Check base URL
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

    // Construct the virtual host style URL (prefix hostname with bucket name)
    {
        int scheme_len;
        char *buf;

        scheme_len = strchr(config.http_io.baseURL, ':') - config.http_io.baseURL + 3;
        if (asprintf(&buf, "%.*s%s.%s", scheme_len,
          config.http_io.baseURL, config.bucket, config.http_io.baseURL + scheme_len) == -1)
            err(1, "asprintf");
        config.http_io.vhostURL = buf;
    }

    // Always use the virtual host style URL if configured to do so
    if (config.http_io.vhost)
        config.http_io.baseURL = config.http_io.vhostURL;

    // Check S3 access privilege
    if (!find_string_in_table(s3_acls, config.http_io.accessType)) {
        warnx("illegal access type `%s'", config.http_io.accessType);
        return -1;
    }

    // Check filenames
    if (strchr(config.fuse_ops.filename, '/') != NULL || *config.fuse_ops.filename == '\0') {
        warnx("illegal filename `%s'", config.fuse_ops.filename);
        return -1;
    }
    if (strchr(config.fuse_ops.stats_filename, '/') != NULL) {
        warnx("illegal stats filename `%s'", config.fuse_ops.stats_filename);
        return -1;
    }

    // Apply default encryption
    if (config.http_io.encryption == NULL && config.encrypt)
        config.http_io.encryption = strdup(S3BACKER_DEFAULT_ENCRYPTION);

    // Uppercase encryption name for consistency
    if (config.http_io.encryption != NULL) {
        char *t;

        if ((t = strdup(config.http_io.encryption)) == NULL)
            err(1, "strdup()");
        for (i = 0; t[i] != '\0'; i++)
            t[i] = toupper(t[i]);
        config.http_io.encryption = t;
    }

    // Check encryption and get key
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

    // Disallow "--compress-level" without "--compress"
    if (config.compress_alg == NULL && config.compress_level != NULL) {
        warnx("the `--compress-level' flag requires the `--compress' flag");
        return -1;
    }

    // Apply backwards-compatibility for "--compress" flag
    if (config.compress_alg == NULL && config.compress_level == NULL && config.compress_flag) {
        static char buf[16];

        snvprintf(buf, sizeof(buf), "%d", Z_DEFAULT_COMPRESSION);
        config.compress_alg = buf;
    }
    if (config.compress_alg != NULL && config.compress_level == NULL && sscanf(config.compress_alg, "%d", &i) == 1) {
        config.compress_level = config.compress_alg;
        config.compress_alg = S3BACKER_DEFAULT_COMPRESSION;
    }

    // We always want to compress if we are encrypting
    if (config.http_io.encryption != NULL && config.compress_alg == NULL)
        config.compress_alg = S3BACKER_DEFAULT_COMPRESSION;

    // Parse compression, if any, and compression level, if any
    if (config.compress_alg != NULL) {
        const struct comp_alg *calg;
        void *level = NULL;

        // Find the compression algorithm
        if ((calg = comp_find(config.compress_alg)) == NULL) {
            warnx("unknown compression algorithm `%s'", config.compress_alg);
            return -1;
        }

        // Parse the compression level, if any
        if (config.compress_level != NULL && (level = (*calg->lparse)(config.compress_level)) == NULL)
            return -1;

        // Done
        config.http_io.compress_alg = calg;
        config.http_io.compress_level = level;
    }

    // Disable md5 cache when in read only mode
    if (config.fuse_ops.read_only) {
        config.ec_protect.cache_size = 0;
        config.ec_protect.cache_time = 0;
        config.ec_protect.min_write_delay = 0;
    }

    // Check time/cache values
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

    // Parse block and file sizes
    if (config.block_size_str != NULL) {
        if (parse_size_string(config.block_size_str, "block size", sizeof(u_int), &value) == -1)
            return -1;
        config.block_size = value;
    }
    if (config.file_size_str != NULL) {
        if (parse_size_string(config.file_size_str, "file size", sizeof(uintmax_t), &value) == -1)
            return -1;
        config.file_size = value;
    }

    // Parse upload/download speeds
    for (i = 0; i < 2; i++) {
        char speed_desc[32];

        snprintf(speed_desc, sizeof(speed_desc), "max %s speed", upload_download_names[i]);
        if (config.max_speed_str[i] != NULL) {
            if (parse_size_string(config.max_speed_str[i], speed_desc, sizeof(uintmax_t), &value) == -1)
                return -1;
            if ((curl_off_t)(value / 8) != (value / 8)) {
                warnx("%s `%s' is too big", speed_desc, config.max_speed_str[i]);
                return -1;
            }
            config.http_io.max_speed[i] = value;
        }
        if (config.http_io.max_speed[i] != 0 && config.block_size / (config.http_io.max_speed[i] / 8) >= config.http_io.timeout) {
            warnx("configured timeout of %us is too short for block size of %u bytes and %s %s bps",
              config.http_io.timeout, config.block_size, speed_desc, config.max_speed_str[i]);
            return -1;
        }
    }

    // Check block cache config
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
    if (config.block_cache.cache_file == NULL && config.block_cache.recover_dirty_blocks) {
        warnx("`--blockCacheRecoverDirtyBlocks' requires specifying `--blockCacheFile'");
        return -1;
    }
    if (config.block_cache.num_protected > config.block_cache.cache_size)
        warnx("`--blockCacheNumProtected' is larger than cache size; this may cause performance problems");

    // Check mount point
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

    // Check list blocks threads
    if (config.list_blocks && config.http_io.list_blocks_threads < 1) {
        warnx("invalid listBlocksThreads %u", config.http_io.list_blocks_threads);
        return -1;
    }

    // Configure logging module
    log_enable_debug = config.debug;

    // Format descriptive string of what we're mounting
    if (config.test)
        snvprintf(config.description, sizeof(config.description), "%s%s/%s", "file://", config.bucket, config.prefix);
    else if (config.http_io.vhost)
        snvprintf(config.description, sizeof(config.description), "%s%s", config.http_io.baseURL, config.prefix);
    else
        snvprintf(config.description, sizeof(config.description), "%s%s/%s", config.http_io.baseURL, config.bucket, config.prefix);

    /*
     * Read the first block (if any) to determine existing file and block size,
     * and compare with configured sizes (if given).
     */
    if (config.test)
        config.no_auto_detect = 1;
    if (config.no_auto_detect)
        r = ENOENT;
    else {
        config.http_io.prefix = config.prefix;
        config.http_io.bucket = config.bucket;
        config.http_io.blockHashPrefix = config.blockHashPrefix;
        config.http_io.debug = config.debug;
        config.http_io.quiet = config.quiet;
        config.http_io.log = config.log;
        if ((s3b = http_io_create(&config.http_io)) == NULL)
            err(1, "http_io_create");
        if (!config.quiet)
            warnx("auto-detecting block size and total file size...");
        r = (*s3b->meta_data)(s3b, &auto_file_size, &auto_block_size);
        (*s3b->shutdown)(s3b);
        (*s3b->destroy)(s3b);
    }

    // Check result
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

    // Check computed block and file sizes
    if (sizeof(off_t) < sizeof(uint64_t)) {
        warnx("sizeof(off_t) is too small (%d < %d)", (int)sizeof(off_t), (int)sizeof(uint64_t));
        return -1;
    }
    if (config.block_size != (1 << (ffs(config.block_size) - 1))) {
        warnx("block size must be a power of 2");
        return -1;
    }
    if (config.file_size % config.block_size != 0) {
        warnx("file size must be a multiple of block size");
        return -1;
    }
    big_num_blocks = config.file_size / config.block_size;
    if (sizeof(config.num_blocks) < sizeof(big_num_blocks) && big_num_blocks >= ((off_t)1 << (sizeof(config.num_blocks) * 8))) {
        warnx("more than 2^%d blocks: decrease file size or increase block size", (int)(sizeof(config.num_blocks) * 8));
        return -1;
    }
    config.num_blocks = (s3b_block_t)big_num_blocks;

    // Allocate zero block
    if ((zero_block = calloc(1, config.block_size)) == NULL) {
        warn("calloc");
        return -1;
    }

    // Check block size vs. encryption block size
    if (config.http_io.encryption != NULL && config.block_size % EVP_MAX_IV_LENGTH != 0) {
        warnx("block size must be at least %u when encryption is enabled", EVP_MAX_IV_LENGTH);
        return -1;
    }

    // Check that MD5 cache won't eventually deadlock
    if (config.ec_protect.cache_size > 0
      && config.ec_protect.cache_time == 0
      && config.ec_protect.cache_size < config.num_blocks) {
        warnx("`md5CacheTime' is infinite but `md5CacheSize' is less than the number of blocks, so eventual deadlock will result");
        return -1;
    }

    // No point in the caches being bigger than necessary
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
    // On MacOS, warn if kernel timeouts can happen prior to our own timeout
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

        // Convert from milliseconds to seconds
        total_time = (total_time + 999) / 1000;

        // Warn if exceeding MacFUSE limit
        if (total_time >= FUSE_MAX_DAEMON_TIMEOUT && !config.quiet) {
            warnx("warning: maximum possible I/O delay (%us) >= MacFUSE limit (%us);", total_time, FUSE_MAX_DAEMON_TIMEOUT);
            warnx("consider lower settings for `--maxRetryPause' and/or `--timeout'.");
        }
    }
#endif  // __APPLE__

    // Copy common stuff into sub-module configs
    config.block_cache.block_size = config.block_size;
    config.block_cache.log = config.log;
    config.http_io.prefix = config.prefix;
    config.http_io.bucket = config.bucket;
    config.http_io.blockHashPrefix = config.blockHashPrefix;
    config.http_io.debug = config.debug;
    config.http_io.quiet = config.quiet;
    config.http_io.block_size = config.block_size;
    config.http_io.num_blocks = config.num_blocks;
    config.http_io.log = config.log;
    config.zero_cache.block_size = config.block_size;
    config.zero_cache.num_blocks = config.num_blocks;
    config.zero_cache.list_blocks = config.list_blocks;
    config.zero_cache.log = config.log;
    config.ec_protect.block_size = config.block_size;
    config.ec_protect.log = config.log;
    config.fuse_ops.block_size = config.block_size;
    config.fuse_ops.num_blocks = config.num_blocks;
    config.fuse_ops.log = config.log;
    config.test_io.debug = config.debug;
    config.test_io.log = config.log;
    config.test_io.block_size = config.block_size;
    config.test_io.num_blocks = config.num_blocks;
    config.test_io.prefix = config.prefix;
    config.test_io.bucket = config.bucket;
    config.test_io.blockHashPrefix = config.blockHashPrefix;

    // Check whether already mounted, and if so, compare mount token against on-disk cache (if any)
    if (!config.test && !config.erase && !config.reset) {
        int32_t mount_token;
        int conflict;

        // Read s3 mount token
        if ((s3b = http_io_create(&config.http_io)) == NULL)
            err(1, "http_io_create");
        r = (*s3b->set_mount_token)(s3b, &mount_token, -1);
        (*s3b->shutdown)(s3b);
        (*s3b->destroy)(s3b);
        if (r != 0) {
            errno = r;
            err(1, "error reading mount token");
        }
        conflict = mount_token != 0;

        /*
         * The disk cache also has a mount token, so we need to do some extra checking.
         * Either token can be 0 (i.e., not present -> not mounted) or != 0 (mounted).
         *
         * If neither token is present, proceed with mount. Note: there should not be
         * any dirty blocks in the disk cache in this case, because this represents a
         * clean unmount situation.
         *
         * If the cache has a token, but S3 has none, that means someone must have used
         * `--reset-mounted-flag' to clear it from S3 since the last time the disk cache was
         * used. In that case, `--force' is required to continue using the disk cache,
         * or `--reset-mounted-flag' must be used to clear the disk cache flag as well.
         *
         * If --blockCacheRecoverDirtyBlocks is specified and the tokens match, we
         * have the corresponding cache file for the last mount. Proceed with mount and,
         * if configured, enable cache writeback of dirty blocks.
         */
        if (config.block_cache.cache_file != NULL) {
            int32_t cache_mount_token = -1;
            struct stat cache_file_stat;
            struct s3b_dcache *dcache;

            // Open disk cache file, if any, and read the mount token therein, if any
            if (stat(config.block_cache.cache_file, &cache_file_stat) == -1) {
                if (errno != ENOENT)
                    err(1, "can't open cache file `%s'", config.block_cache.cache_file);
            } else {
                if ((r = s3b_dcache_open(&dcache, &config.block_cache, NULL, NULL, 0)) != 0)
                    errx(1, "error opening cache file `%s': %s", config.block_cache.cache_file, strerror(r));
                if (s3b_dcache_has_mount_token(dcache) && (r = s3b_dcache_set_mount_token(dcache, &cache_mount_token, -1)) != 0)
                    errx(1, "error reading mount token from `%s': %s", config.block_cache.cache_file, strerror(r));
                s3b_dcache_close(dcache);
            }

            // If cache file is older format, then cache_mount_token will be -1, otherwise >= 0
            if (cache_mount_token > 0) {

                // If tokens do not agree, bail out, otherwise enable write-back of dirty blocks if tokens are non-zero
                if (cache_mount_token != mount_token) {
                    warnx("cache file `%s' mount token mismatch (disk:0x%08x != s3:0x%08x)",
                      config.block_cache.cache_file, cache_mount_token, mount_token);
                } else if (config.block_cache.recover_dirty_blocks) {
                    if (!config.quiet)
                        warnx("recovering from unclean shutdown: dirty blocks in cache file will be written back to S3");
                    config.block_cache.perform_flush = 1;
                    conflict = 0;
                }
            }
        }

        // If there is a conflicting mount, additional `--force' is required
        if (conflict) {
            if (!config.force) {
                warnx("%s appears to be already mounted (using mount token 0x%08x)", config.description, (int)mount_token);
                errx(1, "reset mount token with `--reset-mounted-flag', or use `--force' to override");
            }
            if (!config.quiet) {
                warnx("warning: filesystem appears already mounted but you said `--force'\n"
                  " so I'll proceed anyway even though your data may get corrupted.\n");
            }
        }
    }

    // Done
    return 0;
}

void
dump_config(const struct s3b_config *const c)
{
    int i;

    (*c->log)(LOG_DEBUG, "s3backer config:");
    (*c->log)(LOG_DEBUG, "%24s: %s", "test mode", c->test ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %s", "directIO", c->fuse_ops.direct_io ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "accessId", c->http_io.accessId != NULL ? c->http_io.accessId : "");
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "accessKey", c->http_io.accessKey != NULL ? "****" : "");
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "accessFile", c->accessFile);
    (*c->log)(LOG_DEBUG, "%24s: %s", "accessType", c->http_io.accessType);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "ec2iam_role", c->http_io.ec2iam_role != NULL ? c->http_io.ec2iam_role : "");
    (*c->log)(LOG_DEBUG, "%24s: %s", "authVersion", c->http_io.authVersion);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "baseURL", c->http_io.baseURL);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "region", c->http_io.region);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", c->test ? "testdir" : "bucket", c->bucket);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "prefix", c->prefix);
    (*c->log)(LOG_DEBUG, "%24s: %s", "blockHashPrefix", c->blockHashPrefix ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "defaultContentEncoding",
      c->http_io.default_ce != NULL ? c->http_io.default_ce : "(none)");
    (*c->log)(LOG_DEBUG, "%24s: %s", "list_blocks", c->list_blocks ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %d", "list_blocks_threads", c->http_io.list_blocks_threads);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "mount", c->mount);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "filename", c->fuse_ops.filename);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "stats_filename", c->fuse_ops.stats_filename);
    (*c->log)(LOG_DEBUG, "%24s: %s (%u)", "block_size", c->block_size_str != NULL ? c->block_size_str : "-", c->block_size);
    (*c->log)(LOG_DEBUG, "%24s: %s (%jd)", "file_size", c->file_size_str != NULL ? c->file_size_str : "-", (intmax_t)c->file_size);
    (*c->log)(LOG_DEBUG, "%24s: %jd", "num_blocks", (intmax_t)c->num_blocks);
    (*c->log)(LOG_DEBUG, "%24s: 0%o", "file_mode", c->fuse_ops.file_mode);
    (*c->log)(LOG_DEBUG, "%24s: %s", "read_only", c->fuse_ops.read_only ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %s", "compress", c->http_io.compress_alg ? c->http_io.compress_alg->name : "(none)");
    (*c->log)(LOG_DEBUG, "%24s: %s", "encryption", c->http_io.encryption != NULL ? c->http_io.encryption : "(none)");
    (*c->log)(LOG_DEBUG, "%24s: %u", "key_length", c->http_io.key_length);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "password", c->http_io.password != NULL ? "****" : "");
    (*c->log)(LOG_DEBUG, "%24s: %s bps (%ju)", "max_upload",
      c->max_speed_str[HTTP_UPLOAD] != NULL ? c->max_speed_str[HTTP_UPLOAD] : "-",
      c->http_io.max_speed[HTTP_UPLOAD]);
    (*c->log)(LOG_DEBUG, "%24s: %s bps (%ju)", "max_download",
      c->max_speed_str[HTTP_DOWNLOAD] != NULL ? c->max_speed_str[HTTP_DOWNLOAD] : "-",
      c->http_io.max_speed[HTTP_DOWNLOAD]);
    (*c->log)(LOG_DEBUG, "%24s: %s", "http_11", c->http_io.http_11 ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %us", "timeout", c->http_io.timeout);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "sse", c->http_io.sse);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "sse-key-id", c->http_io.sse_key_id);
    (*c->log)(LOG_DEBUG, "%24s: %ums", "initial_retry_pause", c->http_io.initial_retry_pause);
    (*c->log)(LOG_DEBUG, "%24s: %ums", "max_retry_pause", c->http_io.max_retry_pause);
    (*c->log)(LOG_DEBUG, "%24s: %ums", "min_write_delay", c->ec_protect.min_write_delay);
    (*c->log)(LOG_DEBUG, "%24s: %ums", "md5_cache_time", c->ec_protect.cache_time);
    (*c->log)(LOG_DEBUG, "%24s: %u entries", "md5_cache_size", c->ec_protect.cache_size);
    (*c->log)(LOG_DEBUG, "%24s: %u entries", "block_cache_size", c->block_cache.cache_size);
    (*c->log)(LOG_DEBUG, "%24s: %u threads", "block_cache_threads", c->block_cache.num_threads);
    (*c->log)(LOG_DEBUG, "%24s: %ums", "block_cache_timeout", c->block_cache.timeout);
    (*c->log)(LOG_DEBUG, "%24s: %ums", "block_cache_write_delay", c->block_cache.write_delay);
    (*c->log)(LOG_DEBUG, "%24s: %u blocks", "block_cache_max_dirty", c->block_cache.max_dirty);
    (*c->log)(LOG_DEBUG, "%24s: %s", "block_cache_sync", c->block_cache.synchronous ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %s", "recover_dirty_blocks", c->block_cache.recover_dirty_blocks ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %u blocks", "read_ahead", c->block_cache.read_ahead);
    (*c->log)(LOG_DEBUG, "%24s: %u blocks", "read_ahead_trigger", c->block_cache.read_ahead_trigger);
    (*c->log)(LOG_DEBUG, "%24s: \"%s\"", "block_cache_cache_file",
      c->block_cache.cache_file != NULL ? c->block_cache.cache_file : "");
    (*c->log)(LOG_DEBUG, "%24s: %s", "block_cache_no_verify", c->block_cache.no_verify ? "true" : "false");
    (*c->log)(LOG_DEBUG, "%24s: %s", "fadvise", c->block_cache.fadvise ? "true" : "false");
    (*c->log)(LOG_DEBUG, "fuse_main arguments:");
    for (i = 0; i < c->fuse_args.argc; i++)
        (*c->log)(LOG_DEBUG, "  [%d] = \"%s\"", i, c->fuse_args.argv[i]);
}

static void
usage(void)
{
    const char *const *sptr;

    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "\ts3backer [options] bucket[/subdir] /mount/point\n");
    fprintf(stderr, "\ts3backer --test [options] directory /mount/point\n");
    fprintf(stderr, "\ts3backer --erase [options] bucket[/subdir]\n");
    fprintf(stderr, "\ts3backer --reset-mounted-flag [options] bucket[/subdir]\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "\t--%-27s %s\n", "accessFile=FILE", "File containing `accessID:accessKey' pairs");
    fprintf(stderr, "\t--%-27s %s\n", "accessId=ID", "S3 access key ID");
    fprintf(stderr, "\t--%-27s %s\n", "accessKey=KEY", "S3 secret access key");
    fprintf(stderr, "\t--%-27s %s\n", "accessKeyEnv=VARNAME", "S3 secret access key from environment variable");
    fprintf(stderr, "\t--%-27s %s\n", "accessType=TYPE", "S3 ACL used when creating new items; one of:");
    fprintf(stderr, "\t  %-27s ", "");
    for (sptr = s3_acls; *sptr != NULL; sptr++)
        fprintf(stderr, "%s%s", sptr != s3_acls ? ", " : "  ", *sptr);
    fprintf(stderr, "\n");
    fprintf(stderr, "\t--%-27s %s\n", "authVersion=TYPE", "Specify S3 authentication style; one of:");
    fprintf(stderr, "\t  %-27s ", "");
    for (sptr = s3_auth_types; *sptr != NULL; sptr++)
        fprintf(stderr, "%s%s", sptr != s3_auth_types ? ", " : "  ", *sptr);
    fprintf(stderr, "\n");
    fprintf(stderr, "\t--%-27s %s\n", "accessEC2IAM=ROLE", "Acquire S3 credentials from EC2 machine via IAM role");
    fprintf(stderr, "\t--%-27s %s\n", "baseURL=URL", "Base URL for all requests");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheFile=FILE", "Block cache persistent file");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheMaxDirty=NUM", "Block cache maximum number of dirty blocks");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheNoVerify", "Disable verification of data loaded from cache file");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheFileAdvise", "Use posix_fadvise(2) after reading from cache file");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheSize=NUM", "Block cache size (in number of blocks)");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheSync", "Block cache performs all writes synchronously");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheRecoverDirtyBlocks", "Recover dirty cache file blocks on startup");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheThreads=NUM", "Block cache write-back thread pool size");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheTimeout=MILLIS", "Block cache entry timeout (zero = infinite)");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheWriteDelay=MILLIS", "Block cache maximum write-back delay");
    fprintf(stderr, "\t--%-27s %s\n", "blockCacheNumProtected=NUM", "Preferentially retain NUM blocks in the block cache");
    fprintf(stderr, "\t--%-27s %s\n", "blockSize=SIZE", "Block size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-27s %s\n", "blockHashPrefix", "Prepend hash to block names for even distribution");
    fprintf(stderr, "\t--%-27s %s\n", "cacert=FILE", "Specify SSL certificate authority file");
    fprintf(stderr, "\t--%-27s %s\n", "compress[=LEVEL]", "Enable block compression, with 1=fast up to 9=small");
    fprintf(stderr, "\t--%-27s %s\n", "configFile=FILE", "Substitute command line flags and arguments read from FILE");
    fprintf(stderr, "\t--%-27s %s\n", "debug", "Enable logging of debug messages");
    fprintf(stderr, "\t--%-27s %s\n", "debug-http", "Print HTTP headers to standard output");
    fprintf(stderr, "\t--%-27s %s\n", "directIO", "Disable kernel caching of the backed file");
    fprintf(stderr, "\t--%-27s %s\n", "encrypt[=CIPHER]", "Enable encryption (implies `--compress')");
    fprintf(stderr, "\t--%-27s %s\n", "erase", "Erase all blocks in the filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "fileMode=MODE", "Permissions of backed file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "filename=NAME", "Name of backed file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "force", "Ignore different auto-detected block and file sizes");
    fprintf(stderr, "\t--%-27s %s\n", "help", "Show this information and exit");
    fprintf(stderr, "\t--%-27s %s\n", "http11", "Restrict to HTTP version 1.1");
    fprintf(stderr, "\t--%-27s %s\n", "initialRetryPause=MILLIS", "Initial retry pause after stale data or server error");
    fprintf(stderr, "\t--%-27s %s\n", "insecure", "Don't verify SSL server identity");
    fprintf(stderr, "\t--%-27s %s\n", "keyLength", "Override generated cipher key length");
    fprintf(stderr, "\t--%-27s %s\n", "listBlocks", "Auto-detect non-empty blocks at startup");
    fprintf(stderr, "\t--%-27s %s\n", "listBlocksThreads", "List blocks in parallel using this many threads");
    fprintf(stderr, "\t--%-27s %s\n", "maxDownloadSpeed=BITSPERSEC", "Max download bandwidth for a single read");
    fprintf(stderr, "\t--%-27s %s\n", "maxRetryPause=MILLIS", "Max total pause after stale data or server error");
    fprintf(stderr, "\t--%-27s %s\n", "maxUploadSpeed=BITSPERSEC", "Max upload bandwidth for a single write");
    fprintf(stderr, "\t--%-27s %s\n", "md5CacheSize=NUM", "Max size of MD5 cache (zero = disabled)");
    fprintf(stderr, "\t--%-27s %s\n", "md5CacheTime=MILLIS", "Expire time for MD5 cache (zero = infinite)");
    fprintf(stderr, "\t--%-27s %s\n", "minWriteDelay=MILLIS", "Minimum time between same block writes");
    fprintf(stderr, "\t--%-27s %s\n", "no-vhost", "Disable virtual hosted style requests");
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
    fprintf(stderr, "\t--%-27s %s\n", "size=SIZE", "File size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-27s %s\n", "sse=TYPE", "Specify server side encryption ('" SSE_AES256 "' or '" SSE_AWS_KMS "')");
    fprintf(stderr, "\t--%-27s %s\n", "ss-key-id=ID", "Specify server side encryption customer key ID");
    fprintf(stderr, "\t--%-27s %s\n", "ssl", "Enable SSL");
    fprintf(stderr, "\t--%-27s %s\n", "statsFilename=NAME", "Name of statistics file in filesystem");
    fprintf(stderr, "\t--%-27s %s\n", "storageClass=TYPE", "Specify storage class for written blocks");
    fprintf(stderr, "\t--%-27s %s\n", "test", "Run in local test mode (bucket is a directory)");
    fprintf(stderr, "\t--%-27s %s\n", "test-delays", "In test mode, introduce random I/O delays");
    fprintf(stderr, "\t--%-27s %s\n", "test-discard", "In test mode, discard data and perform no I/O operations");
    fprintf(stderr, "\t--%-27s %s\n", "test-errors", "In test mode, introduce random I/O errors");
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
    fprintf(stderr, "\t--%-27s %u\n", "listBlocksThreads", S3BACKER_DEFAULT_LIST_BLOCKS_THREADS);
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

