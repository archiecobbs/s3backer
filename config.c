
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

/****************************************************************************
 *                          DEFINITIONS                                     *
 ****************************************************************************/

/* S3 URL */
#define S3_BASE_URL                         "http://s3.amazonaws.com/"

/* S3 access permission strings */
#define S3_ACCESS_PRIVATE                   "private"
#define S3_ACCESS_PUBLIC_READ               "public-read"
#define S3_ACCESS_PUBLIC_READ_WRITE         "public-read-write"
#define S3_ACCESS_AUTHENTICATED_READ        "authenticated-read"

/* Default values for some configuration parameters */
#define S3BACKER_DEFAULT_ACCESS             S3_ACCESS_PRIVATE
#define S3BACKER_DEFAULT_BASE_URL           S3_BASE_URL
#define S3BACKER_DEFAULT_PWD_FILE           ".s3backer_passwd"
#define S3BACKER_DEFAULT_PREFIX             ""
#define S3BACKER_DEFAULT_FILENAME           "file"
#define S3BACKER_DEFAULT_BLOCKSIZE          4096
#define S3BACKER_DEFAULT_CONNECT_TIMEOUT    30
#define S3BACKER_DEFAULT_IO_TIMEOUT         30
#define S3BACKER_DEFAULT_FILE_MODE          0600
#define S3BACKER_DEFAULT_MAX_RETRY          9
#define S3BACKER_DEFAULT_RETRY_PAUSE        1000            // 1s
#define S3BACKER_DEFAULT_MIN_WRITE_DELAY    500             // 500ms
#define S3BACKER_DEFAULT_CACHE_TIME         10000           // 10s
#define S3BACKER_DEFAULT_CACHE_SIZE         10000

/****************************************************************************
 *                          FUNCTION DECLARATIONS                           *
 ****************************************************************************/

static int parse_size_string(const char *s, uintmax_t *valp);
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
static struct s3backer_conf config = {
    .accessId=          NULL,
    .accessKey=         NULL,
    .accessFile=        NULL,
    .baseURL=           S3BACKER_DEFAULT_BASE_URL,
    .bucket=            NULL,
    .prefix=            S3BACKER_DEFAULT_PREFIX,
    .access=            S3BACKER_DEFAULT_ACCESS,
    .filename=          S3BACKER_DEFAULT_FILENAME,
    .user_agent=        user_agent_buf,
    .block_size=        0,
    .file_size=         0,
    .file_mode=         S3BACKER_DEFAULT_FILE_MODE,
    .connect_timeout=   S3BACKER_DEFAULT_CONNECT_TIMEOUT,
    .io_timeout=        S3BACKER_DEFAULT_IO_TIMEOUT,
    .max_retry=         S3BACKER_DEFAULT_MAX_RETRY,
    .retry_pause=       S3BACKER_DEFAULT_RETRY_PAUSE,
    .min_write_delay=   S3BACKER_DEFAULT_MIN_WRITE_DELAY,
    .cache_time=        S3BACKER_DEFAULT_CACHE_TIME,
    .cache_size=        S3BACKER_DEFAULT_CACHE_SIZE,
    .log=               syslog_logger
};

/* Command line flags */
static const struct fuse_opt option_list[] = {
    {
        .templ=     "--accessId=%s",
        .offset=    offsetof(struct s3backer_conf, accessId),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--accessKey=%s",
        .offset=    offsetof(struct s3backer_conf, accessKey),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--accessFile=%s",
        .offset=    offsetof(struct s3backer_conf, accessFile),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--access=%s",
        .offset=    offsetof(struct s3backer_conf, access),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--baseURL=%s",
        .offset=    offsetof(struct s3backer_conf, baseURL),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--prefix=%s",
        .offset=    offsetof(struct s3backer_conf, prefix),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--filename=%s",
        .offset=    offsetof(struct s3backer_conf, filename),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--size=%s",
        .offset=    offsetof(struct s3backer_conf, file_size_str),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--blockSize=%s",
        .offset=    offsetof(struct s3backer_conf, block_size_str),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--connectTimeout=%u",
        .offset=    offsetof(struct s3backer_conf, connect_timeout),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--ioTimeout=%u",
        .offset=    offsetof(struct s3backer_conf, io_timeout),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--maxRetry=%u",
        .offset=    offsetof(struct s3backer_conf, max_retry),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--retryPause=%u",
        .offset=    offsetof(struct s3backer_conf, retry_pause),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--minWriteDelay=%u",
        .offset=    offsetof(struct s3backer_conf, min_write_delay),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--cacheTime=%u",
        .offset=    offsetof(struct s3backer_conf, cache_time),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--cacheSize=%u",
        .offset=    offsetof(struct s3backer_conf, cache_size),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    {
        .templ=     "--force",
        .offset=    offsetof(struct s3backer_conf, force),
        .value=     FUSE_OPT_KEY_DISCARD
    },
    FUSE_OPT_END
};

/* Default flags we send to FUSE */
static const char *const s3backer_fuse_defaults[] = {
    "-okernel_cache",
    "-ofsname=s3backer",
    "-ouse_ino",
    "-oentry_timeout=31536000",
    "-onegative_timeout=31536000",
    "-oattr_timeout=31536000",
    "-odefault_permissions",
    "-onodev",
    "-onosuid",
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
};

/****************************************************************************
 *                      PUBLIC FUNCTION DEFINITIONS                         *
 ****************************************************************************/

struct s3backer_conf *
s3backer_get_config(int argc, char **argv)
{
    int i;

    /* One time only */
    assert(config.start_time == 0);

    /* Remember user creds and start time */
    config.uid = getuid();
    config.gid = getgid();
    config.start_time = time(NULL);

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
    if (fuse_opt_parse(&config.fuse_args, &config, option_list, handle_unknown_option) != 0) {
        usage();
        return NULL;
    }

    /* Validate configuration */
    if (validate_config() != 0) {
        usage();
        return NULL;
    }

    /* Debug */
    if (config.debug)
        dump_config();

    /* Done */
    return &config;
}

/****************************************************************************
 *                    INTERNAL FUNCTION DEFINITIONS                         *
 ****************************************************************************/

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
            if (strcasecmp(suffix, size_suffixes[i].suffix) == 0)
                *valp <<= size_suffixes[i].bits;
        }
    }
    return 0;
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
            fprintf(stderr, "%s version %s (r%d)\n", PACKAGE, VERSION, s3backer_svnrev);
            fprintf(stderr, "Copyright (C) 2008 Archie L. Cobbs.\n");
            fprintf(stderr, "This is free software; see the source for copying conditions.  There is NO\n");
            fprintf(stderr, "warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n");
            exit(0);
        }

        /* Help */
        if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
            usage();
            exit(0);
        }

        /* Unknown; pass it through to fuse_main() */
        return 1;
    }

    /* Get bucket parameter */
    if (config.bucket == NULL) {
        if ((config.bucket = strdup(arg)) == NULL)
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
    char buf[1024];
    int i;
    int r;

    /* Default to $HOME/.s3backer for accessFile */
    if (config.accessFile == NULL) {
        const char *home = getenv("HOME");

        if (home != NULL) {
            snprintf(buf, sizeof(buf), "%s/%s", home, S3BACKER_DEFAULT_PWD_FILE);
            if ((config.accessFile = strdup(buf)) == NULL)
                err(1, "strdup");
        }
    }

    /* If no accessId specified, default to first in accessFile */
    if (config.accessId == NULL && config.accessFile != NULL)
        search_access_for(config.accessFile, NULL, &config.accessId, NULL);
    if (config.accessId == NULL)
        warnx("warning: no accessId specified");

    /* Find key in file if not specified explicitly */
    if (config.accessId == NULL && config.accessKey != NULL) {
        warnx("an `accessKey' was specified but no `accessId' was specified");
        return -1;
    }
    if (config.accessId != NULL) {
        if (config.accessKey == NULL && config.accessFile != NULL)
            search_access_for(config.accessFile, config.accessId, NULL, &config.accessKey);
        if (config.accessKey == NULL) {
            warnx("no accessKey specified");
            return -1;
        }
    }

    /* Check bucket */
    if (config.bucket == NULL) {
        warnx("no S3 bucket specified");
        return -1;
    }
    if (*config.bucket == '\0' || *config.bucket == '/' || strchr(config.bucket, '/') != 0) {
        warnx("invalid S3 bucket `%s'", config.bucket);
        return -1;
    }

    /* Check base URL */
    s = NULL;
    if (strncmp(config.baseURL, "http://", 7) == 0)
        s = config.baseURL + 7;
    else if (strncmp(config.baseURL, "https://", 8) == 0)
        s = config.baseURL + 8;
    if (s != NULL && (*s == '/' || *s == '\0'))
        s = NULL;
    if (s != NULL && (s = strchr(s, '/')) == NULL)
        s = NULL;
    if (s != NULL && s[1] != '\0') {
        warnx("base URL must end with a '/'");
        s = NULL;
    }
    if (s == NULL) {
        warnx("invalid base URL `%s'", config.baseURL);
        return -1;
    }

    /* Check S3 access privilege */
    for (i = 0; i < sizeof(s3_acls) / sizeof(*s3_acls); i++) {
        if (strcmp(config.access, s3_acls[i]) == 0)
            break;
    }
    if (i == sizeof(s3_acls) / sizeof(*s3_acls)) {
        warnx("illegal access type `%s'", config.access);
        return -1;
    }

    /* Check filename */
    if (strchr(config.filename, '/') != NULL) {
        warnx("illegal filename `%s'", config.filename);
        return -1;
    }

    /* Check time/cache values */
    if (config.cache_size == 0 && config.cache_time > 0) {
        warnx("`cacheTime' must zero when cache is disabled");
        return -1;
    }
    if (config.cache_size == 0 && config.min_write_delay > 0) {
        warnx("`minWriteDelay' must zero when cache is disabled");
        return -1;
    }
    if (config.cache_time < config.min_write_delay) {
        warnx("`cacheTime' must be at least `minWriteDelay'");
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

    /*
     * Read the first block (if any) to determine existing file and block size,
     * and compare with configured sizes (if given).
     */
    if ((s3b = s3backer_create(&config)) == NULL)
        err(1, "s3backer_create");
    warnx("auto-detecting block size and total size...");
    switch ((r = (*s3b->detect_sizes)(s3b, &auto_file_size, &auto_block_size))) {
    case 0:
        warnx("auto-detected block size=%u and total size=%ju", auto_block_size, (uintmax_t)auto_file_size);
        if (config.block_size == 0)
            config.block_size = auto_block_size;
        else if (auto_block_size != config.block_size) {
            if (config.force) {
                warnx("warning: configured block size %u != filesystem block size %u,"
                  " but you said `--force' so I'll proceed anyway even though"
                  " your data will probably not read back correctly.", config.block_size, auto_block_size);
            } else
                errx(1, "error: configured block size %u != filesystem block size %u", config.block_size, auto_block_size);
        }
        if (config.file_size == 0)
            config.file_size = auto_file_size;
        else if (auto_file_size != config.file_size) {
            if (config.force) {
                warnx("warning: configured file size %ju != filesystem file size %ju,"
                  " but you said `--force' so I'll proceed anyway even though"
                  " your data will probably not read back correctly.", (uintmax_t)config.file_size, (uintmax_t)auto_file_size);
            } else
                errx(1, "error: configured file size %ju != filesystem file size %ju", (uintmax_t)config.file_size, (uintmax_t)auto_file_size);
        }
        break;
    case ENOENT:
    case ENXIO:
        if (config.file_size == 0)
            errx(1, "error: auto-detection of filesystem size failed; please specify `--size'");
        if (config.block_size == 0) {
            config.block_size = S3BACKER_DEFAULT_BLOCKSIZE;
            warnx("assuming default block size of %u", config.block_size);
        }
        break;
    default:
        errno = r;
        err(1, "can't read block zero meta-data");
        break;
    }
    (*s3b->destroy)(s3b);

    /* Check computed block and file sizes */
    config.block_bits = ffs(config.block_size) - 1;
    if (config.block_size != (1 << config.block_bits)) {
        warnx("block size must be a power of 2");
        return -1;
    }
    if (config.file_size % config.block_size != 0) {
        warnx("file size must be a multiple of block size");
        return -1;
    }
    config.num_blocks = config.file_size / config.block_size;
    if (config.num_blocks > ((off_t)1 << (sizeof(s3b_block_t) * 8))) {    // cf. struct defer_info.block_num
        warnx("more than 2^%d blocks: decrease file size or increase block size", sizeof(s3b_block_t) * 8);
        return -1;
    }

    /* Done */
    return 0;
}

static void
dump_config(void)
{
    int i;

    (*config.log)(LOG_DEBUG, "s3backer config:");
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "accessId", config.accessId != NULL ? config.accessId : "");
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "accessKey", config.accessKey != NULL ? "****" : "");
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "accessFile", config.accessFile);
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "access", config.access);
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "baseURL", config.baseURL);
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "bucket", config.bucket);
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "prefix", config.prefix);
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "mount", config.mount);
    (*config.log)(LOG_DEBUG, "%16s: \"%s\"", "filename", config.filename);
    (*config.log)(LOG_DEBUG, "%16s: %s (%u)", "block_size", config.block_size_str != NULL ? config.block_size_str : "-", config.block_size);
    (*config.log)(LOG_DEBUG, "%16s: %u", "block_bits", config.block_bits);
    (*config.log)(LOG_DEBUG, "%16s: %s (%jd)", "file_size", config.file_size_str, (intmax_t)config.file_size);
    (*config.log)(LOG_DEBUG, "%16s: %jd", "num_blocks", (intmax_t)config.num_blocks);
    (*config.log)(LOG_DEBUG, "%16s: 0%o", "file_mode", config.file_mode);
    (*config.log)(LOG_DEBUG, "%16s: %us", "connect_timeout", config.connect_timeout);
    (*config.log)(LOG_DEBUG, "%16s: %us", "io_timeout", config.io_timeout);
    (*config.log)(LOG_DEBUG, "%16s: %us", "max_retry", config.max_retry);
    (*config.log)(LOG_DEBUG, "%16s: %ums", "retry_pause", config.retry_pause);
    (*config.log)(LOG_DEBUG, "%16s: %ums", "min_write_delay", config.min_write_delay);
    (*config.log)(LOG_DEBUG, "%16s: %ums", "cache_time", config.cache_time);
    (*config.log)(LOG_DEBUG, "%16s: %u entries", "cache_size", config.cache_size);
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
    fprintf(stderr, "\t--%-22s %s\n", "accessId=ID", "S3 access key ID");
    fprintf(stderr, "\t--%-22s %s\n", "accessKey=KEY", "S3 secret access key");
    fprintf(stderr, "\t--%-22s %s\n", "accessFile=FILE", "File containing `accessID:accessKey' pairs");
    fprintf(stderr, "\t--%-22s %s\n", "size=SIZE", "File size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-22s %s\n", "blockSize=SIZE", "Block size (with optional suffix 'K', 'M', 'G', etc.)");
    fprintf(stderr, "\t--%-22s %s\n", "accessType=TYPE", "ACL used when creating items; one of:");
    fprintf(stderr, "\t  %-22s ", "");
    for (i = 0; i < sizeof(s3_acls) / sizeof(*s3_acls); i++)
        fprintf(stderr, "%s%s", i > 0 ? ", " : "", s3_acls[i]);
    fprintf(stderr, "\n");
    fprintf(stderr, "\t--%-22s %s\n", "baseURL=URL", "Base URL for all requests");
    fprintf(stderr, "\t--%-22s %s\n", "prefix=STRING", "Prefix for resource names within bucket");
    fprintf(stderr, "\t--%-22s %s\n", "filename=NAME", "Name of backed file in filesystem");
    fprintf(stderr, "\t--%-22s %s\n", "connectTimeout=SECONDS", "Timeout for initial HTTP connection");
    fprintf(stderr, "\t--%-22s %s\n", "ioTimeout=SECONDS", "Timeout for completion of HTTP operation");
    fprintf(stderr, "\t--%-22s %s\n", "maxRetry=COUNT", "# retries when HTTP returns stale data or server error");
    fprintf(stderr, "\t--%-22s %s\n", "retryPause=MILLIS", "Time to pause between aforementioned retries");
    fprintf(stderr, "\t--%-22s %s\n", "minWriteDelay=MILLIS", "Min time between same block writes");
    fprintf(stderr, "\t--%-22s %s\n", "cacheTime=MILLIS", "Expire time for MD5 cache (zero = infinite)");
    fprintf(stderr, "\t--%-22s %s\n", "cacheSize=NUM", "Max size of MD5 cache (zero = disabled)");
    fprintf(stderr, "\t--%-22s %s\n", "force", "Apply block and file sizes even if they disagree");
    fprintf(stderr, "\t--%-22s %s\n", "version", "Show version information and exit");
    fprintf(stderr, "\t--%-22s %s\n", "help", "Show this information and exit");
    fprintf(stderr, "Default values:\n");
    fprintf(stderr, "\t--%-22s %d\n", "blockSize", S3BACKER_DEFAULT_BLOCKSIZE);
    fprintf(stderr, "\t--%-22s \"%s\"\n", "accessFile", "$HOME/" S3BACKER_DEFAULT_PWD_FILE);
    fprintf(stderr, "\t--%-22s \"%s\"\n", "accessType", S3BACKER_DEFAULT_ACCESS);
    fprintf(stderr, "\t--%-22s \"%s\"\n", "baseURL", S3BACKER_DEFAULT_BASE_URL);
    fprintf(stderr, "\t--%-22s \"%s\"\n", "prefix", S3BACKER_DEFAULT_PREFIX);
    fprintf(stderr, "\t--%-22s \"%s\"\n", "filename", S3BACKER_DEFAULT_FILENAME);
    fprintf(stderr, "\t--%-22s %u\n", "connectTimeout", S3BACKER_DEFAULT_CONNECT_TIMEOUT);
    fprintf(stderr, "\t--%-22s %u\n", "ioTimeout", S3BACKER_DEFAULT_IO_TIMEOUT);
    fprintf(stderr, "\t--%-22s %u\n", "maxRetry", S3BACKER_DEFAULT_MAX_RETRY);
    fprintf(stderr, "\t--%-22s %u\n", "retryPause", S3BACKER_DEFAULT_RETRY_PAUSE);
    fprintf(stderr, "\t--%-22s %u\n", "minWriteDelay", S3BACKER_DEFAULT_MIN_WRITE_DELAY);
    fprintf(stderr, "\t--%-22s %u\n", "cacheTime", S3BACKER_DEFAULT_CACHE_TIME);
    fprintf(stderr, "\t--%-22s %u\n", "cacheSize", S3BACKER_DEFAULT_CACHE_SIZE);
    fprintf(stderr, "FUSE options (partial list):\n");
    fprintf(stderr, "\t%-24s %s\n", "-d", "Debug mode (implies -f)");
    fprintf(stderr, "\t%-24s %s\n", "-f", "Run in the foreground (do not fork)");
    fprintf(stderr, "\t%-24s %s\n", "-s", "Run in single-threaded mode");
    fprintf(stderr, "\t%-24s %s\n", "-o allow_root", "Allow root (only) to view backed file");
    fprintf(stderr, "\t%-24s %s\n", "-o allow_other", "Allow all users to view backed file");
    fprintf(stderr, "\t%-24s %s\n", "-o nonempty", "Allow all users to view backed file");
    fprintf(stderr, "\t%-24s %s\n", "-o uid=UID", "Set user ID");
    fprintf(stderr, "\t%-24s %s\n", "-o gid=GID", "Set group ID");
    fprintf(stderr, "\t%-24s %s\n", "-o sync_read", "Do synchronous reads");
    fprintf(stderr, "\t%-24s %s\n", "-o max_readahead=NUM", "Set maximum read-ahead (bytes)");
}

