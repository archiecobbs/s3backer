
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
 *                              DEFINITIONS                                 *
 ****************************************************************************/

#define ROOT_INODE      1
#define FILE_INODE      2
#define STATS_INODE     3

/* Represents an open 'stats' file */
struct s3b_stats_file {
    char    *buf;           // note: not necessarily nul-terminated
    size_t  len;            // length of string in 'buf'
    size_t  bufsiz;         // size allocated for 'buf'
    int     memerr;         // we got a memory error
};

/****************************************************************************
 *                          FUNCTION DECLARATIONS                           *
 ****************************************************************************/

/* FUSE functions */
static void *fuse_op_init(struct fuse_conn_info *conn);
static void fuse_op_destroy(void *data);
static int fuse_op_getattr(const char *path, struct stat *st);
static int fuse_op_fgetattr(const char *path, struct stat *st, struct fuse_file_info *);
static int fuse_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi);
static int fuse_op_open(const char *path, struct fuse_file_info *fi);
static int fuse_op_release(const char *path, struct fuse_file_info *fi);
static int fuse_op_read(const char *path, char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi);
static int fuse_op_write(const char *path, const char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi);
static int fuse_op_statfs(const char *path, struct statvfs *st);
static int fuse_op_truncate(const char *path, off_t size);
static int fuse_op_flush(const char *path, struct fuse_file_info *fi);
static int fuse_op_fsync(const char *path, int isdatasync, struct fuse_file_info *fi);

/* Attribute functions */
static void fuse_op_getattr_file(struct stat *st);
static void fuse_op_getattr_stats(struct s3b_stats_file *sfile, struct stat *st);

/* Stats functions */
static struct s3b_stats_file *fuse_op_stats_create(struct s3backer_store *s3b);
static void fuse_op_stats_destroy(struct s3b_stats_file *sfile);
static void fuse_op_stats_printf(struct s3b_stats_file *sfile, const char *fmt, ...)
  __attribute__ ((__format__ (__printf__, 2, 3)));

/****************************************************************************
 *                          VARIABLE DEFINITIONS                            *
 ****************************************************************************/

/* FUSE operations */
const struct fuse_operations s3backer_fuse_ops = {
    .init       = fuse_op_init,
    .destroy    = fuse_op_destroy,
    .getattr    = fuse_op_getattr,
    .fgetattr   = fuse_op_fgetattr,
    .readdir    = fuse_op_readdir,
    .open       = fuse_op_open,
    .read       = fuse_op_read,
    .write      = fuse_op_write,
    .statfs     = fuse_op_statfs,
    .truncate   = fuse_op_truncate,
    .flush      = fuse_op_flush,
    .fsync      = fuse_op_fsync,
    .release    = fuse_op_release,
};

/* Configuration */
static struct s3backer_conf *config;

/****************************************************************************
 *                      PUBLIC FUNCTION DEFINITIONS                         *
 ****************************************************************************/

const struct fuse_operations *
s3backer_get_fuse_ops(struct s3backer_conf *the_config)
{
    assert(config == NULL);
    config = the_config;
    return &s3backer_fuse_ops;
}

/****************************************************************************
 *                    INTERNAL FUNCTION DEFINITIONS                         *
 ****************************************************************************/

static void *
fuse_op_init(struct fuse_conn_info *conn)
{
    return s3backer_create(config);
}

static void
fuse_op_destroy(void *data)
{
    struct s3backer_store *const s3b = data;

    (*s3b->destroy)(s3b);
}

static int
fuse_op_getattr(const char *path, struct stat *st)
{
    struct s3backer_store *const s3b = (struct s3backer_store *)fuse_get_context()->private_data;

    memset(st, 0, sizeof(*st));
    if (strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        st->st_ino = ROOT_INODE;
        st->st_uid = config->uid;
        st->st_gid = config->gid;
        st->st_atime = config->start_time;
        st->st_mtime = config->start_time;
        st->st_ctime = config->start_time;
        return 0;
    }
    if (*path == '/' && strcmp(path + 1, config->filename) == 0) {
        fuse_op_getattr_file(st);
        return 0;
    }
    if (*path == '/' && strcmp(path + 1, config->stats_filename) == 0) {
        struct s3b_stats_file *sfile;

        if ((sfile = fuse_op_stats_create(s3b)) == NULL)
            return -ENOMEM;
        fuse_op_getattr_stats(sfile, st);
        fuse_op_stats_destroy(sfile);
        return 0;
    }
    return -ENOENT;
}

static int
fuse_op_fgetattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
    if (fi->fh != 0) {
        struct s3b_stats_file *const sfile = (struct s3b_stats_file *)(uintptr_t)fi->fh;

        fuse_op_getattr_stats(sfile, st);
    } else
        fuse_op_getattr_file(st);
    return 0;
}

static void
fuse_op_getattr_file(struct stat *st)
{
    st->st_mode = S_IFREG | config->file_mode;
    st->st_nlink = 1;
    st->st_ino = FILE_INODE;
    st->st_uid = config->uid;
    st->st_gid = config->gid;
    st->st_size = config->file_size;
    st->st_blksize = config->block_size;
    st->st_blocks = config->file_size / config->block_size;
    st->st_atime = config->start_time;
    st->st_mtime = config->start_time;
    st->st_ctime = config->start_time;
}

static void
fuse_op_getattr_stats(struct s3b_stats_file *sfile, struct stat *st)
{
    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    st->st_nlink = 1;
    st->st_ino = STATS_INODE;
    st->st_uid = config->uid;
    st->st_gid = config->gid;
    st->st_size = sfile->len;
    st->st_blksize = config->block_size;
    st->st_blocks = (sfile->len + config->block_size - 1) / config->block_size;
    st->st_atime = config->start_time;
    st->st_mtime = config->start_time;
    st->st_ctime = config->start_time;
}

static int
fuse_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{
    (void)offset;
    (void)fi;
    if (strcmp(path, "/") != 0)
        return -ENOENT;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, config->filename, NULL, 0);
    if (*config->stats_filename != '\0')
        filler(buf, config->stats_filename, NULL, 0);
    return 0;
}

static int
fuse_op_open(const char *path, struct fuse_file_info *fi)
{
    struct s3backer_store *const s3b = (struct s3backer_store *)fuse_get_context()->private_data;

    /* Backed file */
    if (path[0] == '/' && strcmp(path + 1, config->filename) == 0) {
        fi->fh = 0;
        return 0;
    }

    /* Stats file */
    if (path[0] == '/' && *config->stats_filename != '\0' && strcmp(path + 1, config->stats_filename) == 0) {
        struct s3b_stats_file *sfile;

        if ((sfile = fuse_op_stats_create(s3b)) == NULL)
            return -ENOMEM;
        fi->fh = (uint64_t)(uintptr_t)sfile;
        return 0;
    }

    /* Unknown file */
    return -ENOENT;
}

static int
fuse_op_release(const char *path, struct fuse_file_info *fi)
{
    if (fi->fh != 0) {
        struct s3b_stats_file *const sfile = (struct s3b_stats_file *)(uintptr_t)fi->fh;

        fuse_op_stats_destroy(sfile);
    }
    return 0;
}

static int
fuse_op_read(const char *path, char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi)
{
    struct s3backer_store *const s3b = (struct s3backer_store *)fuse_get_context()->private_data;
    const u_int mask = config->block_size - 1;
    const size_t orig_size = size;
    char *fragment = NULL;
    s3b_block_t block_num;
    size_t num_blocks;
    int r;

    /* Handle stats file */
    if (fi->fh != 0) {
        struct s3b_stats_file *const sfile = (struct s3b_stats_file *)(uintptr_t)fi->fh;

        if (offset > sfile->len)
            return 0;
        if (offset + size > sfile->len)
            size = sfile->len - offset;
        memcpy(buf, sfile->buf + offset, size);
        return size;
    }

    /* Check for out of range */
    if (offset + size > config->file_size) {
        (*config->log)(LOG_ERR, "read offset=0x%jx size=0x%jx out of range", (uintmax_t)offset, (uintmax_t)size);
        return -ESPIPE;
    }

    /* Read first block fragment (if any) */
    if ((offset & mask) != 0) {
        size_t fragoff = (size_t)(offset & mask);
        size_t fraglen = (size_t)config->block_size - fragoff;

        if (fraglen > size)
            fraglen = size;
        block_num = offset >> config->block_bits;
        fragment = alloca(config->block_size);
        if ((r = (*s3b->read_block)(s3b, block_num, fragment)) != 0)
            return -r;
        memcpy(buf, fragment + fragoff, fraglen);
        buf += fraglen;
        offset += fraglen;
        size -= fraglen;
    }

    /* Get block number and count */
    block_num = offset >> config->block_bits;
    num_blocks = size >> config->block_bits;

    /* Read intermediate complete blocks */
    while (num_blocks-- > 0) {
        if ((r = (*s3b->read_block)(s3b, block_num++, buf)) != 0)
            return -r;
        buf += config->block_size;
    }

    /* Read last block fragment (if any) */
    if ((size & mask) != 0) {
        const size_t fraglen = size & mask;

        if (fragment == NULL)
            fragment = alloca(config->block_size);
        if ((r = (*s3b->read_block)(s3b, block_num, fragment)) != 0)
            return -r;
        memcpy(buf, fragment, fraglen);
    }

    /* Done */
    return orig_size;
}

static int fuse_op_write(const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
    struct s3backer_store *const s3b = (struct s3backer_store *)fuse_get_context()->private_data;
    const u_int mask = config->block_size - 1;
    const size_t orig_size = size;
    char *fragment = NULL;
    s3b_block_t block_num;
    size_t num_blocks;
    int r;

    /* Handle stats file */
    if (fi->fh != 0)
        return -EINVAL;

    /* Check for out of range */
    if (offset + size > config->file_size) {
        (*config->log)(LOG_ERR, "write offset=0x%jx size=0x%jx out of range", (uintmax_t)offset, (uintmax_t)size);
        return -ESPIPE;
    }

    /* Write first block fragment (if any) */
    if ((offset & mask) != 0) {
        size_t fragoff = (size_t)(offset & mask);
        size_t fraglen = (size_t)config->block_size - fragoff;

        if (fraglen > size)
            fraglen = size;
        block_num = offset >> config->block_bits;
        fragment = alloca(config->block_size);
        if ((r = (*s3b->read_block)(s3b, block_num, fragment)) != 0)
            return -r;
        memcpy(fragment + fragoff, buf, fraglen);
        if ((r = (*s3b->write_block)(s3b, block_num, fragment)) != 0)
            return -r;
        buf += fraglen;
        offset += fraglen;
        size -= fraglen;
    }

    /* Get block number and count */
    block_num = offset >> config->block_bits;
    num_blocks = size >> config->block_bits;

    /* Write intermediate complete blocks */
    while (num_blocks-- > 0) {
        if ((r = (*s3b->write_block)(s3b, block_num++, buf)) != 0)
            return -r;
        buf += config->block_size;
    }

    /* Write last block fragment (if any) */
    if ((size & mask) != 0) {
        const size_t fraglen = size & mask;

        if (fragment == NULL)
            fragment = alloca(config->block_size);
        if ((r = (*s3b->read_block)(s3b, block_num, fragment)) != 0)
            return -r;
        memcpy(fragment, buf, fraglen);
        if ((r = (*s3b->write_block)(s3b, block_num, fragment)) != 0)
            return -r;
    }

    /* Done */
    return orig_size;
}

static int
fuse_op_statfs(const char *path, struct statvfs *st)
{
    st->f_bsize = config->block_size;
    st->f_frsize = config->block_size;
    st->f_blocks = config->file_size / config->block_size;
    st->f_bfree = 0;
    st->f_bavail = 0;
    st->f_files = 3;
    st->f_ffree = 0;
    st->f_favail = 0;
    return 0;
}

static int
fuse_op_truncate(const char *path, off_t size)
{
    return 0;
}

static int
fuse_op_flush(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

static int
fuse_op_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    return 0;
}

static struct s3b_stats_file *
fuse_op_stats_create(struct s3backer_store *s3b)
{
    struct s3backer_stats stats;
    struct s3b_stats_file *sfile;

    if ((sfile = calloc(1, sizeof(*sfile))) == NULL)
        return NULL;
    (*s3b->get_stats)(s3b, &stats);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "total_blocks_read", stats.total_blocks_read);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "total_blocks_written", stats.total_blocks_written);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "zero_blocks_read", stats.zero_blocks_read);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "zero_blocks_written", stats.zero_blocks_written);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "empty_blocks_read", stats.empty_blocks_read);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "empty_blocks_written", stats.empty_blocks_written);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_gets", stats.http_gets);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_puts", stats.http_puts);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_deletes", stats.http_deletes);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_unauthorized", stats.http_unauthorized);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_forbidden", stats.http_forbidden);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_stale", stats.http_stale);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_5xx_error", stats.http_5xx_error);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_4xx_error", stats.http_4xx_error);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "http_other_error", stats.http_other_error);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_handles_created", stats.curl_handles_created);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_handles_reused", stats.curl_handles_reused);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_timeouts", stats.curl_timeouts);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_connect_failed", stats.curl_connect_failed);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_host_unknown", stats.curl_host_unknown);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_out_of_memory", stats.curl_out_of_memory);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "curl_other_error", stats.curl_other_error);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "num_retries", stats.num_retries);
    fuse_op_stats_printf(sfile, "%-24s %ju\n", "retry_delay", (uintmax_t)stats.retry_delay);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "current_cache_size", stats.current_cache_size);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "cache_data_hits", stats.cache_data_hits);
    fuse_op_stats_printf(sfile, "%-24s %ju\n", "cache_full_delay", (uintmax_t)stats.cache_full_delay);
    fuse_op_stats_printf(sfile, "%-24s %ju\n", "repeated_write_delay", (uintmax_t)stats.repeated_write_delay);
    fuse_op_stats_printf(sfile, "%-24s %u\n", "out_of_memory_errors", stats.out_of_memory_errors);
    if (sfile->memerr != 0) {
        fuse_op_stats_destroy(sfile);
        return NULL;
    }
    return sfile;
}

static void
fuse_op_stats_destroy(struct s3b_stats_file *sfile)
{
    free(sfile->buf);
    free(sfile);
}

static void
fuse_op_stats_printf(struct s3b_stats_file *sfile, const char *fmt, ...)
{
    va_list args;
    char *new_buf;
    size_t new_bufsiz;
    size_t remain;
    int added;

    /* Bail if no memory */
    if (sfile->memerr)
        return;

again:
    /* Append to string buffer */
    remain = sfile->bufsiz - sfile->len;
    va_start(args, fmt);
    added = vsnprintf(sfile->buf + sfile->len, sfile->bufsiz - sfile->len, fmt, args);
    va_end(args);
    if (added <= remain) {
        sfile->len += added;
        return;
    }

    /* We need a bigger buffer */
    new_bufsiz = ((sfile->bufsiz + added + 1023) / 1024) * 1024;
    if ((new_buf = realloc(sfile->buf, new_bufsiz)) == NULL) {
        sfile->memerr = 1;
        return;
    }
    sfile->buf = new_buf;
    sfile->bufsiz = new_bufsiz;
    goto again;
}

