
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
#include "util.h"

// Definitions
#define MAX_CHILD_PROCESSES     10

// Size suffixes
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

// Logging flags
int log_enable_debug;
int daemonized;

// A block's worth of data containing only zero bytes
const void *zero_block;
static size_t zero_block_size;

// stderr logging mutex
static pthread_mutex_t stderr_log_mutex = PTHREAD_MUTEX_INITIALIZER;

// Internal state
static struct child_proc child_procs[MAX_CHILD_PROCESSES];
static int num_child_procs;

// Internal functions
static pid_t fork_off(const char *executable, char **argv);

/****************************************************************************
 *                      PUBLIC FUNCTION DEFINITIONS                         *
 ****************************************************************************/

// Returns 0 if found, -1 if not found or unsupported (too big)
int
parse_size_string(const char *s, const char *description, u_int max_bytes, uintmax_t *valp)
{
    char suffix[3] = { '\0' };
    int nconv;
    int i;

    nconv = sscanf(s, "%ju%2s", valp, suffix);
    if (nconv >= 2 && *valp != 0) {
        for (i = 0; i < sizeof(size_suffixes) / sizeof(*size_suffixes); i++) {
            const struct size_suffix *const ss = &size_suffixes[i];

            if (strcasecmp(suffix, ss->suffix) != 0)
                continue;
            if (ss->bits >= max_bytes * 8) {
                warnx("%s value `%s' is too big for this build of s3backer", description, s);
                return -1;
            }
            *valp <<= ss->bits;
            return 0;
        }
    }
    warnx("invalid %s `%s'", description, s);
    return -1;
}

void
unparse_size_string(char *buf, int bmax, uintmax_t value)
{
    int i;

    if (value == 0) {
        snvprintf(buf, bmax, "0");
        return;
    }
    for (i = sizeof(size_suffixes) / sizeof(*size_suffixes); i-- > 0; ) {
        const struct size_suffix *const ss = &size_suffixes[i];
        uintmax_t unit;

        if (ss->bits >= sizeof(uintmax_t) * 8)
            continue;
        unit = (uintmax_t)1 << ss->bits;
        if (value % unit == 0) {
            snvprintf(buf, bmax, "%ju%s", value / unit, ss->suffix);
            return;
        }
    }
    snvprintf(buf, bmax, "%ju", value);
}

void
describe_size(char *buf, int bmax, uintmax_t value)
{
    int i;

    for (i = sizeof(size_suffixes) / sizeof(*size_suffixes); i-- > 0; ) {
        const struct size_suffix *const ss = &size_suffixes[i];
        uintmax_t unit;

        if (ss->bits >= sizeof(uintmax_t) * 8)
            continue;
        unit = (uintmax_t)1 << ss->bits;
        if (value >= unit) {
            snvprintf(buf, bmax, "%.2f%s", (double)(value >> (ss->bits - 8)) / (double)(1 << 8), ss->suffix);
            return;
        }
    }
    snvprintf(buf, bmax, "%ju", value);
}

int
find_string_in_table(const char *const *table, const char *value)
{
    while (*table != NULL) {
        if (strcmp(value, *table) == 0)
            return 1;
        table++;
    }
    return 0;
}

// Returns the number of bitmap_t's in a bitmap big enough to hold num_blocks bits
size_t
bitmap_size(s3b_block_t num_blocks)
{
    const size_t bits_per_word = sizeof(bitmap_t) * 8;
    const size_t nwords = (num_blocks + bits_per_word - 1) / bits_per_word;

    return nwords;
}

bitmap_t *
bitmap_init(s3b_block_t num_blocks, int value)
{
    bitmap_t *bitmap;
    size_t nbytes;

    if (value == 0)
        bitmap = calloc(bitmap_size(num_blocks), sizeof(*bitmap));
    else {
        nbytes = bitmap_size(num_blocks) * sizeof(*bitmap);
        if ((bitmap = malloc(nbytes)) != NULL)
            memset(bitmap, 0xff, nbytes);
    }
    return bitmap;
}

void
bitmap_free(bitmap_t **bitmapp)
{
    free(*bitmapp);
    *bitmapp = NULL;
}

int
bitmap_test(const bitmap_t *bitmap, s3b_block_t block_num)
{
    const int bits_per_word = sizeof(*bitmap) * 8;
    const int index = block_num / bits_per_word;
    const bitmap_t bit = (bitmap_t)1 << (block_num % bits_per_word);

    return (bitmap[index] & bit) != 0;
}

void
bitmap_set(bitmap_t *bitmap, s3b_block_t block_num, int value)
{
    const int bits_per_word = sizeof(*bitmap) * 8;
    const int index = block_num / bits_per_word;
    const bitmap_t bit = (bitmap_t)1 << (block_num % bits_per_word);

    if (value)
        bitmap[index] |= bit;
    else
        bitmap[index] &= ~bit;
}

void
bitmap_and(bitmap_t *dst, const bitmap_t *src, s3b_block_t num_blocks)
{
    const size_t nwords = bitmap_size(num_blocks);
    size_t i;

    for (i = 0; i < nwords; i++)
        dst[i] &= src[i];
}

void
bitmap_or(bitmap_t *dst, const bitmap_t *src, s3b_block_t num_blocks)
{
    const size_t nwords = bitmap_size(num_blocks);
    size_t i;

    for (i = 0; i < nwords; i++)
        dst[i] |= src[i];
}

size_t
bitmap_or2(bitmap_t *dst, const bitmap_t *src, s3b_block_t num_blocks)
{
    const size_t nwords = bitmap_size(num_blocks);
    size_t count = 0;
    size_t i;

    assert(sizeof(bitmap_t) == 4);
    for (i = 0; i < nwords; i++)
        count += popcount32(dst[i] |= src[i]);
    return count;
}

void
bitmap_not(bitmap_t *bitmap, s3b_block_t num_blocks)
{
    const size_t nwords = bitmap_size(num_blocks);
    size_t i;

    for (i = 0; i < nwords; i++)
        bitmap[i] = ~bitmap[i];
}

// https://stackoverflow.com/q/109023/263801
int
popcount32(uint32_t value)
{
     value = value - ((value >> 1) & 0x55555555);
     value = (value & 0x33333333) + ((value >> 2) & 0x33333333);
     value = (value + (value >> 4)) & 0x0F0F0F0F;
     return (int)((value * 0x01010101) >> 24);
}

int
init_zero_block(u_int block_size)
{
    assert(zero_block == NULL);
    if ((zero_block = calloc(1, block_size)) == NULL)
        return -1;
    zero_block_size = block_size;
    return 0;
}

int
block_is_zeros(const void *data)
{
    assert(zero_block != NULL);
    assert(zero_block_size > 0);
    return data == zero_block || memcmp(data, zero_block, zero_block_size) == 0;
}

void
block_list_init(struct block_list *list)
{
    memset(list, 0, sizeof(*list));
}

int
block_list_append(struct block_list *list, s3b_block_t block_num)
{
    s3b_block_t *new_blocks;
    s3b_block_t new_alloc;

    if (list->num_alloc <= list->num_blocks) {
        new_alloc = (list->num_blocks * 2) + 13;
        if ((new_blocks = realloc(list->blocks, new_alloc * sizeof(*list->blocks))) == NULL)
            return errno;
        list->blocks = new_blocks;
        list->num_alloc = new_alloc;
    }
    list->blocks[list->num_blocks++] = block_num;
    return 0;
}

void
block_list_free(struct block_list *list)
{
    free(list->blocks);
    memset(list, 0, sizeof(*list));
}

int
generic_bulk_zero(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks)
{
    int r;

    while (num_blocks-- > 0) {
        if ((r = (s3b->write_block)(s3b, *block_nums++, NULL, NULL, NULL, NULL)) != 0)
            return r;
    }
    return 0;
}

void
syslog_logger(int level, const char *fmt, ...)
{
    va_list args;

    // Filter debug messages
    if (!log_enable_debug && level == LOG_DEBUG)
        return;

    // Send message to syslog
    va_start(args, fmt);
    vsyslog(level, fmt, args);
    va_end(args);
}

void
stderr_logger(int level, const char *fmt, ...)
{
    va_list args;
    char *fmt2;

    // Filter debug messages
    if (!log_enable_debug && level == LOG_DEBUG)
        return;

    // Prefix format string
    if ((fmt2 = prefix_log_format(level, fmt)) == NULL)
        return;

    // Print log message
    va_start(args, fmt);
    pthread_mutex_lock(&stderr_log_mutex);
    vfprintf(stderr, fmt2, args);
    fprintf(stderr, "\n");
    CHECK_RETURN(pthread_mutex_unlock(&stderr_log_mutex));
    va_end(args);
    free(fmt2);
}

// Prefixes a printf() format string with timestamp and log level.
// Caller must free the returned string.
char *
prefix_log_format(int level, const char *fmt)
{
    const char *levelstr;
    char timebuf[32];
    struct tm tm;
    time_t now;
    char *fmt2;

    // Get level descriptor
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

    // Prefix format string
    time(&now);
    strftime(timebuf, sizeof(timebuf), "%F %T", localtime_r(&now, &tm));
    if (asprintf(&fmt2, "%s %s: %s", timebuf, levelstr, fmt) == -1)
        return NULL;

    // Done
    return fmt2;
}

// Like snprintf(), but aborts if the buffer is overflowed and gracefully handles negative buffer lengths
int
snvprintf(char *buf, int size, const char *format, ...)
{
    va_list args;
    int len;

    // Format string (unless size is zero or less)
    va_start(args, format);
    len = size > 0 ? vsnprintf(buf, size, format, args) : 0;
    va_end(args);

    // Check for overflow
    if (len > size - 1) {
        fprintf(stderr, "buffer overflow: \"%s\": %d > %d", format, len, (int)size);
        abort();
    }

    // Done
    return len;
}

void
daemon_debug(const struct s3b_config *config, const char *fmt, ...)
{
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (!daemonized)
        vwarnx(fmt, ap);
    else {
        vsnprintf(buf, sizeof(buf), fmt, ap);
        (*config->log)(LOG_DEBUG, "%s: %s", PACKAGE, buf);
    }
    va_end(ap);
}

void
daemon_warn(const struct s3b_config *config, const char *fmt, ...)
{
    const int errval = errno;
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (!daemonized)
        vwarn(fmt, ap);
    else {
        vsnprintf(buf, sizeof(buf), fmt, ap);
        (*config->log)(LOG_WARNING, "%s: %s: %s", PACKAGE, buf, strerror(errval));
    }
    va_end(ap);
}

void
daemon_warnx(const struct s3b_config *config, const char *fmt, ...)
{
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (!daemonized)
        vwarnx(fmt, ap);
    else {
        vsnprintf(buf, sizeof(buf), fmt, ap);
        (*config->log)(LOG_WARNING, "%s: %s", PACKAGE, buf);
    }
    va_end(ap);
}

void
daemon_err(const struct s3b_config *config, int exval, const char *fmt, ...)
{
    const int errval = errno;
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (!daemonized)
        verr(exval, fmt, ap);
    else {
        vsnprintf(buf, sizeof(buf), fmt, ap);
        (*config->log)(LOG_ERR, "%s: %s: %s", PACKAGE, buf, strerror(errval));
        exit(exval);
    }
    va_end(ap);
}

void
daemon_errx(const struct s3b_config *config, int exval, const char *fmt, ...)
{
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (!daemonized)
        verrx(exval, fmt, ap);
    else {
        vsnprintf(buf, sizeof(buf), fmt, ap);
        (*config->log)(LOG_ERR, "%s: %s", PACKAGE, buf);
        exit(exval);
    }
    va_end(ap);
}

// Calculate the partial initial, partial trailing, and complete central blocks associated with a range of bytes
// It's allowed for buf == NULL, in which case on return all data pointers in *info will also be NULL.
void
calculate_boundary_info(struct boundary_info *info, u_int block_size, const void *buf, size_t size, off_t offset)
{
    const u_int shift = ffs(block_size) - 1;
    const off_t mask = block_size - 1;
    s3b_block_t current_block;
    char *current_data;

    // Initialize
    memset(info, 0, sizeof(*info));
    current_block = offset >> shift;
    current_data = (char *)(uintptr_t)buf;

    // Handle header, if any
    info->header.offset = (u_int)(offset & mask);
    if (info->header.offset > 0) {
        info->header.data = current_data;
        info->header.block = current_block;
        info->header.length = block_size - info->header.offset;
        if (info->header.length > size)
            info->header.length = size;
        size -= info->header.length;
        offset += size;
        if (current_data != NULL)
            current_data += info->header.length;
        current_block++;
    }
    if (size == 0)
        return;

    // Handle center, if any
    info->mid_block_count = size >> shift;
    if (info->mid_block_count > 0) {
        info->mid_data = current_data;
        info->mid_block_start = current_block;
        if (current_data != NULL)
            current_data += info->mid_block_count * block_size;
        current_block += info->mid_block_count;
    }

    // Handle footer, if any
    info->footer.length = (u_int)(size & mask);
    if (info->footer.length > 0) {
        info->footer.data = current_data;
        info->footer.block = current_block;
    }
}

pid_t
start_child_process(const struct s3b_config *config, const char *executable, struct string_array *params)
{
    struct child_proc *proc;
    pid_t pid;
    int i;

    // Debug
    if (config->debug) {
        daemon_debug(config, "executing %s with these parameters:", executable);
        for (i = 0; params->strings[i] != NULL; i++)
            daemon_debug(config, "  [%02d] \"%s\"", i, params->strings[i]);
    }

    // Sanity check
    if (num_child_procs >= MAX_CHILD_PROCESSES)
        daemon_errx(config, 1, "%s: %s", executable, "child process table is full");

    // Fork & exec
    if ((pid = fork_off(executable, params->strings)) == -1)
        daemon_err(config, 1, "%s", executable);

    // Add to list
    proc = &child_procs[num_child_procs++];
    memset(proc, 0, sizeof(*proc));
    proc->name = executable;
    proc->pid = pid;

    // Debug
    if (config->debug)
        daemon_debug(config, "started %s as process %d", proc->name, (int)proc->pid);

    // Done
    return pid;
}

// Wait for any child process to exit.
// Returns:
//  -1  Got interrupted by signal
//   0  No more child processes left
//  >0  Child process ID (also populates *child)
pid_t
wait_for_child_to_exit(const struct s3b_config *config, struct child_proc *child, int sleep_if_none, int expect_signal)
{
    struct child_proc *proc = NULL;
    int child_index;
    int wstatus;
    pid_t pid;

    // What to do if there are no children left?
    if (num_child_procs == 0) {
        if (!sleep_if_none)
            return (pid_t)0;
        while (1) {
            if (usleep(999999) == -1) {         // interrupted by signal
                if (config->debug)
                    daemon_debug(config, "rec'd signal during sleep");
                return (pid_t)-1;
            }
        }
    }

    // Wait for some child to exit or a signal
    if ((pid = wait(&wstatus)) == -1) {
        if (errno == EINTR) {           // interrupted by signal
            if (config->debug)
                daemon_debug(config, "rec'd signal during wait");
            return (pid_t)-1;
        }
        daemon_err(config, 1, "waitpid");
    }

    // Find the child that we just reaped
    for (child_index = 0; child_index < num_child_procs; child_index++) {
        if (pid == child_procs[child_index].pid) {
            proc = &child_procs[child_index];
            proc->wstatus = wstatus;
            break;
        }
    }
    if (proc == NULL)
        daemon_err(config, 1, "reaped unknown child process %d", (int)pid);     // this should never happen

    // Log what happened
    if (WIFEXITED(wstatus)) {
        if (WEXITSTATUS(wstatus) != 0) {
            daemon_warnx(config, "child process %s (%d) terminated with exit value %d",
              proc->name, (int)proc->pid, (int)WEXITSTATUS(wstatus));
        } else if (config->debug)
            daemon_debug(config, "child process %s (%d) terminated normally", proc->name, (int)proc->pid);
    } else if (WIFSIGNALED(wstatus)) {
        if (WTERMSIG(wstatus) != expect_signal)
            daemon_warnx(config, "child process %s (%d) terminated on signal %d", proc->name, (int)pid, (int)WTERMSIG(wstatus));
        else if (config->debug)
            daemon_debug(config, "child process %s (%d) terminated on signal %d", proc->name, (int)pid, (int)WTERMSIG(wstatus));
    } else
        daemon_warnx(config, "weird status from wait(2): %d", wstatus);

    // Populate return info
    if (child != NULL)
        memcpy(child, proc, sizeof(*proc));

    // Remove this child from the list
    memcpy(child_procs + child_index, child_procs + child_index + 1, (--num_child_procs - child_index) * sizeof(*child_procs));

    // Done
    return pid;
}

void
kill_remaining_children(const struct s3b_config *config, pid_t except, int signal)
{
    int child_index;

    for (child_index = 0; child_index < num_child_procs; child_index++) {
        struct child_proc *const proc = &child_procs[child_index];

        if (proc->pid == except)
            continue;
        if (config->debug)
            daemon_debug(config, "killing child %s (%d)", proc->name, (int)proc->pid);
        if (kill(proc->pid, signal) == -1 && config->debug)
            daemon_warn(config, "kill(%s (%d), %d)", proc->name, (int)proc->pid, signal);
    }
}

static pid_t
fork_off(const char *executable, char **argv)
{
    pid_t child;

    // Fork
    if ((child = fork()) == -1)
        return -1;
    if (child > 0)                                      // we are the parent
        return child;

    // Execute
    execve(executable, argv, environ);
    err(1, "%s", executable);
}

void
apply_process_tweaks(void)
{
#if HAVE_DECL_PR_SET_IO_FLUSHER
    (void)prctl(PR_SET_IO_FLUSHER, (unsigned long)1, (unsigned long)0, (unsigned long)0, (unsigned long)0);
#endif
}

// Sync (i.e., persist) the specified file or directory
int
fsync_path(const char *path, int must_exist)
{
    int fd;
    int r = 0;

    // Open file
    if ((fd = open(path, O_RDONLY|O_CLOEXEC)) == -1) {
        if (errno == ENOENT && !must_exist)
            return 0;
        return errno;
    }

    // Sync file
    if (fsync(fd) == -1)
        r = errno;

    // Done
    (void)close(fd);
    return r;
}

int
add_string(struct string_array *array, const char *format, ...)
{
    size_t new_num_alloc;
    char **new_strings;
    char *string;
    va_list args;
    int r;

    // Extend array if needed
    if (array->num_strings + 2 > array->num_alloc) {
        new_num_alloc = 13 + array->num_alloc * 2;
        if ((new_strings = realloc(array->strings, new_num_alloc * sizeof(*array->strings))) == NULL)
            return -1;
        array->strings = new_strings;
        array->num_alloc = new_num_alloc;
    }

    // Format new string
    va_start(args, format);
    r = vasprintf(&string, format, args);
    va_end(args);
    if (r == -1)
        return -1;

    // Add string to array
    array->strings[array->num_strings++] = string;
    array->strings[array->num_strings] = NULL;                  // keep string list NULL-terminated (when non-empty)

    // Done
    return 0;
}

void
free_strings(struct string_array *array)
{
    while (array->num_strings > 0)
        free(array->strings[--array->num_strings]);
    free(array->strings);
    memset(array, 0, sizeof(*array));
}

void
set_config_log(struct s3b_config *config, log_func_t *log)
{
    config->log = log;
    config->block_cache.log = log;
    config->http_io.log = log;
    config->zero_cache.log = log;
    config->ec_protect.log = log;
    config->fuse_ops.log = log;
    config->test_io.log = log;
}

void
md5_quick(const void *data, size_t len, u_char *result)
{
    EVP_MD_CTX *ctx;
    u_int md5_len;
    int r;

    ctx = EVP_MD_CTX_new();
    assert(ctx != NULL);
    r = EVP_DigestInit_ex(ctx, EVP_md5(), NULL);
    assert(r == 0);
    r = EVP_DigestUpdate(ctx, data, len);
    assert(r == 0);
    r = EVP_DigestFinal_ex(ctx, result, &md5_len);
    assert(r == 0);
    assert(md5_len == MD5_DIGEST_LENGTH);
    EVP_MD_CTX_free(ctx);
}
