
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
#include "util.h"

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

/* Debug logging flag */
int log_enable_debug;

/* stderr logging mutex */
static pthread_mutex_t stderr_log_mutex = PTHREAD_MUTEX_INITIALIZER;

/****************************************************************************
 *                      PUBLIC FUNCTION DEFINITIONS                         *
 ****************************************************************************/

int
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

            if (ss->bits >= sizeof(uintmax_t) * 8)
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

void
unparse_size_string(char *buf, size_t bmax, uintmax_t value)
{
    int i;

    if (value == 0) {
        snprintf(buf, bmax, "0");
        return;
    }
    for (i = sizeof(size_suffixes) / sizeof(*size_suffixes); i-- > 0; ) {
        const struct size_suffix *const ss = &size_suffixes[i];
        uintmax_t unit;

        if (ss->bits >= sizeof(uintmax_t) * 8)
            continue;
        unit = (uintmax_t)1 << ss->bits;
        if (value % unit == 0) {
            snprintf(buf, bmax, "%ju%s", value / unit, ss->suffix);
            return;
        }
    }
    snprintf(buf, bmax, "%ju", value);
}

void
describe_size(char *buf, size_t bmax, uintmax_t value)
{
    int i;

    for (i = sizeof(size_suffixes) / sizeof(*size_suffixes); i-- > 0; ) {
        const struct size_suffix *const ss = &size_suffixes[i];
        uintmax_t unit;

        if (ss->bits >= sizeof(uintmax_t) * 8)
            continue;
        unit = (uintmax_t)1 << ss->bits;
        if (value >= unit) {
            snprintf(buf, bmax, "%.2f%s", (double)(value >> (ss->bits - 8)) / (double)(1 << 8), ss->suffix);
            return;
        }
    }
    snprintf(buf, bmax, "%ju", value);
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

/* Returns the number of bitmap_t's in a bitmap big enough to hold num_blocks bits */
size_t
bitmap_size(s3b_block_t num_blocks)
{
    const size_t bits_per_word = sizeof(bitmap_t) * 8;
    const size_t nwords = (num_blocks + bits_per_word - 1) / bits_per_word;

    return nwords;
}

bitmap_t *
bitmap_init(s3b_block_t num_blocks)
{
    return calloc(bitmap_size(num_blocks), sizeof(bitmap_t));
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

int
block_is_zeros(const void *data, u_int block_size)
{
    static const u_long zero;
    const u_int *ptr;
    int i;

    if (block_size <= sizeof(zero))
        return memcmp(data, &zero, block_size) == 0;
    assert(block_size % sizeof(*ptr) == 0);
    ptr = (const u_int *)data;
    for (i = 0; i < block_size / sizeof(*ptr); i++) {
        if (*ptr++ != 0)
            return 0;
    }
    return 1;
}

void
syslog_logger(int level, const char *fmt, ...)
{
    va_list args;

    /* Filter debug messages */
    if (!log_enable_debug && level == LOG_DEBUG)
        return;

    /* Send message to syslog */
    va_start(args, fmt);
    vsyslog(level, fmt, args);
    va_end(args);
}

void
stderr_logger(int level, const char *fmt, ...)
{
    const char *levelstr;
    char timebuf[32];
    va_list args;
    struct tm tm;
    time_t now;

    /* Filter debug messages */
    if (!log_enable_debug && level == LOG_DEBUG)
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
    pthread_mutex_lock(&stderr_log_mutex);
    fprintf(stderr, "%s %s: ", timebuf, levelstr);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
    pthread_mutex_unlock(&stderr_log_mutex);
    va_end(args);
}
