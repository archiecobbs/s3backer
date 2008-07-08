
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
 *                          FUNCTION DECLARATIONS                           *
 ****************************************************************************/

/* FUSE functions */
static void *fuse_op_init(struct fuse_conn_info *conn);
static void fuse_op_destroy(void *data);
static int fuse_op_getattr(const char *path, struct stat *st);
static int fuse_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi);
static int fuse_op_open(const char *path, struct fuse_file_info *fi);
static int fuse_op_read(const char *path, char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi);
static int fuse_op_write(const char *path, const char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi);
static int fuse_op_statfs(const char *path, struct statvfs *st);
static int fuse_op_truncate(const char *path, off_t size);
static int fuse_op_flush(const char *path, struct fuse_file_info *fi);
static int fuse_op_fsync(const char *path, int isdatasync, struct fuse_file_info *fi);

/****************************************************************************
 *                          VARIABLE DEFINITIONS                            *
 ****************************************************************************/

/* FUSE operations */
const struct fuse_operations s3backer_fuse_ops = {
    .init       = fuse_op_init,
    .destroy    = fuse_op_destroy,
    .getattr    = fuse_op_getattr,
    .readdir    = fuse_op_readdir,
    .open       = fuse_op_open,
    .read       = fuse_op_read,
    .write      = fuse_op_write,
    .statfs     = fuse_op_statfs,
    .truncate   = fuse_op_truncate,
    .flush      = fuse_op_flush,
    .fsync      = fuse_op_fsync,
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
    memset(st, 0, sizeof(*st));
    if (strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        st->st_ino = 1;
        st->st_uid = config->uid;
        st->st_gid = config->gid;
        st->st_atime = config->start_time;
        st->st_mtime = config->start_time;
        st->st_ctime = config->start_time;
        return 0;
    }
    if (*path == '/' && strcmp(path + 1, config->filename) == 0) {
        st->st_mode = S_IFREG | config->file_mode;
        st->st_nlink = 1;
        st->st_ino = 2;
        st->st_uid = config->uid;
        st->st_gid = config->gid;
        st->st_size = config->file_size;
        st->st_blksize = config->block_size;
        st->st_blocks = config->file_size / config->block_size;
        st->st_atime = config->start_time;
        st->st_mtime = config->start_time;
        st->st_ctime = config->start_time;
        return 0;
    }
    return -ENOENT;
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
    return 0;
}

static int
fuse_op_open(const char *path, struct fuse_file_info *fi)
{
    if (path[0] != '/' || strcmp(path + 1, config->filename) != 0)
        return -ENOENT;
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

    //(*config->log)(LOG_DEBUG, "***  READ: off=0x%jx size=0x%jx (block #%lu, %lu blocks)", (uintmax_t)offset, (uintmax_t)size, (unsigned long)(offset >> config->block_bits), (unsigned long)(size >> config->block_bits));

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

    //(*config->log)(LOG_DEBUG, "*** WRITE: off=0x%jx size=0x%jx (block #%lu, %lu blocks)", (uintmax_t)offset, (uintmax_t)size, (unsigned long)(offset >> config->block_bits), (unsigned long)(size >> config->block_bits));

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
    st->f_files = 2;
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

