
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
 * $Id$
 */

#include "s3backer.h"
#include "http_io.h"
#include "block_part.h"
#include "test_io.h"

/* Do we want random errors? */
#define RANDOM_ERROR_PERCENT    0

/* Internal state */
struct test_io_private {
    struct http_io_conf         *config;
    u_char                      zero_block[0];
};

/* s3backer_store functions */
static int test_io_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict);
static int test_io_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *md5,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int test_io_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int test_io_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);
static int test_io_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static void test_io_destroy(struct s3backer_store *s3b);

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
test_io_create(struct http_io_conf *config)
{
    struct s3backer_store *s3b;
    struct test_io_private *priv;

    /* Initialize structures */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL)
        return NULL;
    s3b->read_block = test_io_read_block;
    s3b->write_block = test_io_write_block;
    s3b->read_block_part = test_io_read_block_part;
    s3b->write_block_part = test_io_write_block_part;
    s3b->list_blocks = test_io_list_blocks;
    s3b->destroy = test_io_destroy;
    if ((priv = calloc(1, sizeof(*priv) + config->block_size)) == NULL) {
        free(s3b);
        errno = ENOMEM;
        return NULL;
    }
    priv->config = config;
    s3b->data = priv;

    /* Random initialization */
    srandom((u_int)time(NULL));

    /* Done */
    return s3b;
}

/*
 * Destructor
 */
static void
test_io_destroy(struct s3backer_store *const s3b)
{
    struct test_io_private *const priv = s3b->data;

    free(priv);
    free(s3b);
}

static int
test_io_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    u_char md5[MD5_DIGEST_LENGTH];
    char path[PATH_MAX];
    int zero_block;
    MD5_CTX ctx;
    int fd;
    int r;

    /* Logging */
    if (config->debug)
        (*config->log)(LOG_DEBUG, "test_io: read %0*jx started", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Random delay */
    usleep((random() % 200) * 1000);

    /* Random error */
    if ((random() % 100) < RANDOM_ERROR_PERCENT) {
        (*config->log)(LOG_ERR, "test_io: random failure reading %0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
        return EAGAIN;
    }

    /* Generate path */
    snprintf(path, sizeof(path), "%s/%s%0*jx", config->bucket, config->prefix, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Read block */
    if ((fd = open(path, O_RDONLY)) != -1) {
        int total;

        /* Read file */
        for (total = 0; total < config->block_size; total += r) {
            if ((r = read(fd, (char *)dest + total, config->block_size - total)) == -1) {
                r = errno;
                (*config->log)(LOG_ERR, "can't read %s: %s", path, strerror(r));
                close(fd);
                return r;
            }
            if (r == 0)
                break;
        }
        close(fd);

        /* Check for short read */
        if (total != config->block_size) {
            (*config->log)(LOG_ERR, "%s: file is truncated (only read %d out of %u bytes)", path, total, config->block_size);
            return EIO;
        }

        /* Done */
        r = 0;
    } else
        r = errno;

    /* Convert ENOENT into a read of all zeroes */
    if ((zero_block = (r == ENOENT))) {
        memset(dest, 0, config->block_size);
        r = 0;
    }

    /* Check for other error */
    if (r != 0) {
        (*config->log)(LOG_ERR, "can't open %s: %s", path, strerror(r));
        return r;
    }

    /* Compute MD5 */
    if (zero_block)
        memset(md5, 0, MD5_DIGEST_LENGTH);
    else {
        MD5_Init(&ctx);
        MD5_Update(&ctx, dest, config->block_size);
        MD5_Final(md5, &ctx);
    }
    if (actual_md5 != NULL)
        memcpy(actual_md5, md5, MD5_DIGEST_LENGTH);

    /* Check expected MD5 */
    if (expect_md5 != NULL) {
        const int match = memcmp(md5, expect_md5, MD5_DIGEST_LENGTH);

        if (strict) {
            if (!match) {
                (*config->log)(LOG_ERR, "%s: wrong MD5 checksum?!", path);
                return EINVAL;
            }
        } else if (match)
            r = EEXIST;
    }

    /* Logging */
    if (config->debug) {
        (*config->log)(LOG_DEBUG, "test_io: read %0*jx complete%s%s", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
          zero_block ? " (zero)" : "", r == EEXIST ? " (expected md5 match)" : "");
    }

    /* Done */
    return r;
}

static int
test_io_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *caller_md5,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    u_char md5[MD5_DIGEST_LENGTH];
    char temp[PATH_MAX];
    char path[PATH_MAX];
    MD5_CTX ctx;
    int total;
    int fd;
    int r;

    /* Check for zero block */
    if (src != NULL && memcmp(src, priv->zero_block, config->block_size) == 0)
        src = NULL;

    /* Compute MD5 */
    if (src != NULL) {
        MD5_Init(&ctx);
        MD5_Update(&ctx, src, config->block_size);
        MD5_Final(md5, &ctx);
    } else
        memset(md5, 0, MD5_DIGEST_LENGTH);

    /* Return MD5 to caller */
    if (caller_md5 != NULL)
        memcpy(caller_md5, md5, MD5_DIGEST_LENGTH);

    /* Logging */
    if (config->debug) {
        (*config->log)(LOG_DEBUG, "test_io: write %0*jx started%s",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, src == NULL ? " (zero block)" : "");
    }

    /* Random delay */
    usleep((random() % 200) * 1000);

    /* Random error */
    if ((random() % 100) < RANDOM_ERROR_PERCENT) {
        (*config->log)(LOG_ERR, "test_io: random failure writing %0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
        return EAGAIN;
    }

    /* Generate path */
    snprintf(path, sizeof(path), "%s/%s%0*jx", config->bucket, config->prefix, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Delete zero blocks */
    if (src == NULL) {
        if (unlink(path) == -1 && errno != ENOENT) {
            r = errno;
            (*config->log)(LOG_ERR, "can't unlink %s: %s", path, strerror(r));
            return r;
        }
        return 0;
    }

    /* Write into temporary file */
    snprintf(temp, sizeof(temp), "%s.XXXXXX", path);
    if ((fd = mkstemp(temp)) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "%s: %s", temp, strerror(r));
        return r;
    }
    for (total = 0; total < config->block_size; total += r) {
        if ((r = write(fd, (const char *)src + total, config->block_size - total)) == -1) {
            r = errno;
            (*config->log)(LOG_ERR, "can't write %s: %s", temp, strerror(r));
            close(fd);
            (void)unlink(temp);
            return r;
        }
    }
    close(fd);

    /* Rename file */
    if (rename(temp, path) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "can't rename %s: %s", temp, strerror(r));
        (void)unlink(temp);
        return r;
    }

    /* Logging */
    if (config->debug)
        (*config->log)(LOG_DEBUG, "test_io: write %0*jx complete", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    /* Done */
    return 0;
}

static int
test_io_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;

    return block_part_read_block_part(s3b, block_num, config->block_size, off, len, dest);
}

static int
test_io_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;

    return block_part_write_block_part(s3b, block_num, config->block_size, off, len, src);
}

static int
test_io_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    struct test_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    s3b_block_t block_num;
    struct dirent *dent;
    DIR *dir;
    int i;

    /* Open directory */
    if ((dir = opendir(config->bucket)) == NULL)
        return errno;

    /* Scan directory */
    for (i = 0; (dent = readdir(dir)) != NULL; i++) {
        if (http_io_parse_block(config, dent->d_name, &block_num) == 0)
            (*callback)(arg, block_num);
    }

    /* Close directory */
    closedir(dir);

    /* Done */
    return 0;
}

