
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
#include "dcache.h"

/*
 * This file implements a simple on-disk storage area for cached blocks.
 * The file contains a header, a directory, and a data area. Each directory
 * entry indicates which block is stored in the corresponding "data slot"
 * in the data area and that block's MD5 checksum. Note the MD5 checksum is
 * the checksum of the stored data, which will differ from the actual block
 * data's MD5 if the block was compressed, encrypted, etc. when stored.
 *
 * File format:
 *
 *  [ struct file_header ]
 *  directory entry for data slot #0
 *  directory entry for data slot #1
 *  ...
 *  directory entry for data slot #N-1
 *  padding up to getpagesize()
 *  data slot #0
 *  data slot #1
 *  ...
 *  data slot #N-1
 */

/* Definitions */
#define DCACHE_SIGNATURE            0xe496f17b
#define ROUNDUP2(x, y)              (((x) + (y) - 1) & ~((y) - 1))
#define DIRECTORY_READ_CHUNK        1024

#define HDR_SIZE(flags)             (((flags) & HDRFLG_NEW_FORMAT) == 0 ? sizeof(struct ofile_header) : sizeof(struct file_header))
#define DIR_ENTSIZE(flags)          (((flags) & HDRFLG_NEW_FORMAT) == 0 ? sizeof(struct odir_entry) : sizeof(struct dir_entry))
#define DIR_OFFSET(flags, dslot)    ((off_t)HDR_SIZE(flags) + (off_t)(dslot) * DIR_ENTSIZE(flags))
#define DATA_OFFSET(priv, dslot)    ((off_t)(priv)->data + (off_t)(dslot) * (priv)->block_size)

/* Bits for file_header.flags */
#define HDRFLG_NEW_FORMAT           0x00000001
#define HDRFLG_MASK                 0x00000001

/* Bits for dir_entry.flags */
#define ENTFLG_DIRTY                0x00000001
#define ENTFLG_MASK                 0x00000001

/* File header (old format) */
struct ofile_header {
    uint32_t                        signature;
    uint32_t                        header_size;
    uint32_t                        u_int_size;
    uint32_t                        s3b_block_t_size;
    uint32_t                        block_size;
    uint32_t                        data_align;
    uint32_t                        flags;
    u_int                           max_blocks;
} __attribute__ ((packed));

/* File header */
struct file_header {
    uint32_t                        signature;
    uint32_t                        header_size;
    uint32_t                        u_int_size;
    uint32_t                        s3b_block_t_size;
    uint32_t                        block_size;
    uint32_t                        data_align;
    uint32_t                        flags;
    u_int                           max_blocks;
    int32_t                         mount_token;
    uint32_t                        spare[7];           /* future expansion */
} __attribute__ ((packed));

/* One directory entry (old format) */
struct odir_entry {
    s3b_block_t                     block_num;
    u_char                          md5[MD5_DIGEST_LENGTH];
} __attribute__ ((packed));

/* One directory entry (new format) */
struct dir_entry {
    s3b_block_t                     block_num;
    u_char                          md5[MD5_DIGEST_LENGTH];
    uint32_t                        flags;
} __attribute__ ((packed));

/* Private structure */
struct s3b_dcache {
    int                             fd;
    log_func_t                      *log;
    char                            *filename;
    void                            *zero_block;
    u_int                           block_size;
    u_int                           max_blocks;
    u_int                           num_alloc;
    uint32_t                        flags;              /* copy of file_header.flags */
    off_t                           data;
    u_int                           free_list_len;
    u_int                           free_list_alloc;
    s3b_block_t                     *free_list;
};

/* Internal functions */
static int s3b_dcache_write_entry(struct s3b_dcache *priv, u_int dslot, const struct dir_entry *entry);
#ifndef NDEBUG
static int s3b_dcache_entry_is_empty(struct s3b_dcache *priv, u_int dslot);
static int s3b_dcache_entry_write_ok(struct s3b_dcache *priv, u_int dslot, s3b_block_t block_num, u_int dirty);
static int s3b_dcache_read_entry(struct s3b_dcache *priv, u_int dslot, struct dir_entry *entryp);
#endif
static int s3b_dcache_create_file(struct s3b_dcache *priv, int *fdp, const char *filename, u_int max_blocks,
            struct file_header *headerp);
static int s3b_dcache_resize_file(struct s3b_dcache *priv, const struct file_header *header);
static int s3b_dcache_init_free_list(struct s3b_dcache *priv, s3b_dcache_visit_t *visitor, void *arg, u_int visit_dirty);
static int s3b_dcache_push(struct s3b_dcache *priv, u_int dslot);
static void s3b_dcache_pop(struct s3b_dcache *priv, u_int *dslotp);
static int s3b_dcache_read(struct s3b_dcache *priv, off_t offset, void *data, size_t len);
static int s3b_dcache_write(struct s3b_dcache *priv, off_t offset, const void *data, size_t len);
static int s3b_dcache_write2(struct s3b_dcache *priv, int fd, const char *filename, off_t offset, const void *data, size_t len);

/* Internal variables */
static const struct dir_entry zero_entry;

/* Public functions */

int
s3b_dcache_open(struct s3b_dcache **dcachep, log_func_t *log, const char *filename,
  u_int block_size, u_int max_blocks, s3b_dcache_visit_t *visitor, void *arg, u_int visit_dirty)
{
    struct ofile_header oheader;
    struct file_header header;
    struct s3b_dcache *priv;
    struct stat sb;
    int r;

    /* Sanity check */
    if (max_blocks == 0)
        return EINVAL;

    /* Initialize private structure */
    if ((priv = malloc(sizeof(*priv))) == NULL)
        return errno;
    memset(priv, 0, sizeof(*priv));
    priv->fd = -1;
    priv->log = log;
    priv->block_size = block_size;
    priv->max_blocks = max_blocks;
    if ((priv->filename = strdup(filename)) == NULL) {
        r = errno;
        goto fail1;
    }
    if ((priv->zero_block = calloc(1, block_size)) == NULL) {
        r = errno;
        goto fail2;
    }

    /* Create cache file if it doesn't already exist */
    if (stat(priv->filename, &sb) == -1 && errno == ENOENT) {
        (*priv->log)(LOG_NOTICE, "creating new cache file `%s' with capacity %u blocks", priv->filename, priv->max_blocks);
        if ((r = s3b_dcache_create_file(priv, &priv->fd, priv->filename, priv->max_blocks, NULL)) != 0)
            goto fail3;
        (void)close(priv->fd);
        priv->fd = -1;
    }

retry:
    /* Open cache file */
    assert(priv->fd == -1);
    if ((priv->fd = open(priv->filename, O_RDWR, 0)) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "can't open cache file `%s': %s", priv->filename, strerror(r));
        goto fail3;
    }

    /* Get file info */
    if (fstat(priv->fd, &sb) == -1) {
        r = errno;
        goto fail4;
    }

    /* Read in header with backward compatible support for older header format */
    if (sb.st_size < sizeof(oheader)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': file is truncated (size %ju < %u)",
          priv->filename, (uintmax_t)sb.st_size, (u_int)sizeof(oheader));
        r = EINVAL;
        goto fail4;
    }
    if ((r = s3b_dcache_read(priv, (off_t)0, &oheader, sizeof(oheader))) != 0) {
        (*priv->log)(LOG_ERR, "can't read cache file `%s' header: %s", priv->filename, strerror(r));
        goto fail4;
    }
    switch (oheader.header_size) {
    case sizeof(oheader):                               /* old format */
        memset(&header, 0, sizeof(header));
        memcpy(&header, &oheader, sizeof(oheader));
        break;
    case sizeof(header):                                /* new format */
        if ((r = s3b_dcache_read(priv, (off_t)0, &header, sizeof(header))) != 0) {
            (*priv->log)(LOG_ERR, "can't read cache file `%s' header: %s", priv->filename, strerror(r));
            goto fail4;
        }
        break;
    default:
        (*priv->log)(LOG_ERR, "invalid cache file `%s': %s %d", priv->filename, "invalid header size", (int)oheader.header_size);
        r = EINVAL;
        goto fail4;
    }

    /* Verify header - all but number of blocks */
    r = EINVAL;
    if (header.signature != DCACHE_SIGNATURE) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': wrong signature %08x != %08x",
          priv->filename, header.signature, DCACHE_SIGNATURE);
        goto fail4;
    }
    if (header.header_size != HDR_SIZE(header.flags)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': %s %d != %d",
          priv->filename, "invalid header size", (int)header.header_size, (int)HDR_SIZE(header.flags));
        goto fail4;
    }
    if (header.u_int_size != sizeof(u_int)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': created with sizeof(u_int) %u != %u",
          priv->filename, header.u_int_size, (u_int)sizeof(u_int));
        goto fail4;
    }
    if (header.s3b_block_t_size != sizeof(s3b_block_t)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': created with sizeof(s3b_block_t) %u != %u",
          priv->filename, header.s3b_block_t_size, (u_int)sizeof(s3b_block_t));
        goto fail4;
    }
    if (header.block_size != priv->block_size) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': created with block size %u != %u",
          priv->filename, header.block_size, priv->block_size);
        goto fail4;
    }
    if (header.data_align != getpagesize()) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': created with alignment %u != %u",
          priv->filename, header.data_align, getpagesize());
        goto fail4;
    }
    if ((header.flags & ~HDRFLG_MASK) != 0) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': %s", priv->filename, "unrecognized flags present");
        goto fail4;
    }
    priv->flags = header.flags;

    /* Check number of blocks, shrinking or expanding if necessary */
    if (header.max_blocks != priv->max_blocks) {
        (*priv->log)(LOG_NOTICE, "cache file `%s' was created with capacity %u != %u blocks, automatically %s",
          priv->filename, header.max_blocks, priv->max_blocks, header.max_blocks < priv->max_blocks ?
           "expanding" : "shrinking");
        if ((r = s3b_dcache_resize_file(priv, &header)) != 0)
            goto fail4;
        (*priv->log)(LOG_INFO, "successfully resized cache file `%s' from %u to %u blocks",
          priv->filename, header.max_blocks, priv->max_blocks);
        goto retry;
    }

    /* Verify file's directory is not truncated */
    if (sb.st_size < DIR_OFFSET(priv->flags, priv->max_blocks)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': file is truncated (size %ju < %ju)",
          priv->filename, (uintmax_t)sb.st_size, (uintmax_t)DIR_OFFSET(priv->flags, priv->max_blocks));
        goto fail4;
    }

    /* Compute offset of first data block */
    priv->data = ROUNDUP2(DIR_OFFSET(priv->flags, priv->max_blocks), header.data_align);

    /* Read the directory to build the free list and visit allocated blocks */
    if (visitor != NULL && (r = s3b_dcache_init_free_list(priv, visitor, arg, visit_dirty)) != 0)
        goto fail4;

    /* Done */
    *dcachep = priv;
    return 0;

fail4:
    close(priv->fd);
fail3:
    free(priv->zero_block);
fail2:
    free(priv->filename);
fail1:
    free(priv->free_list);
    free(priv);
    return r;
}

int
s3b_dcache_has_mount_token(struct s3b_dcache *priv)
{
    return (priv->flags & HDRFLG_NEW_FORMAT) != 0;
}

int
s3b_dcache_set_mount_token(struct s3b_dcache *priv, int32_t *old_valuep, int32_t new_value)
{
    int r;

    /* Read old value */
    if (old_valuep != NULL) {
        if ((r = s3b_dcache_read(priv, offsetof(struct file_header, mount_token), old_valuep, sizeof(*old_valuep))) != 0)
            return r;
    }

    /* Write new value */
    if (new_value >= 0) {

        /* Update file */
        if ((r = s3b_dcache_write(priv, offsetof(struct file_header, mount_token), &new_value, sizeof(new_value))) != 0)
            return r;

        /* Sync to disk */
        s3b_dcache_fsync(priv);
    }

    /* Done */
    return 0;
}

void
s3b_dcache_close(struct s3b_dcache *priv)
{
    close(priv->fd);
    free(priv->zero_block);
    free(priv->filename);
    free(priv->free_list);
    free(priv);
}

u_int
s3b_dcache_size(struct s3b_dcache *priv)
{
    return priv->num_alloc;
}

/*
 * Allocate a dslot for a block's data. We don't record this block in the directory yet;
 * that is done by s3b_dcache_record_block().
 */
int
s3b_dcache_alloc_block(struct s3b_dcache *priv, u_int *dslotp)
{
    /* Any free dslots? */
    if (priv->free_list_len == 0)
        return ENOMEM;

    /* Pop off the next free dslot */
    s3b_dcache_pop(priv, dslotp);

    /* Directory entry should be empty */
    assert(s3b_dcache_entry_is_empty(priv, *dslotp));

    /* Done */
    priv->num_alloc++;
    return 0;
}

/*
 * Record a block's dslot in the directory. After this function is called, the block will
 * be visible in the directory and picked up after a restart.
 *
 * If md5 != NULL, the block is CLEAN; if md5 == NULL, the block is DIRTY.
 *
 * This should be called AFTER the data for the block has already been written.
 *
 * There MUST NOT be a directory entry for the block.
 */
int
s3b_dcache_record_block(struct s3b_dcache *priv, u_int dslot, s3b_block_t block_num, const u_char *md5)
{
    const u_int dirty = md5 == NULL;
    struct dir_entry entry;
    int r;

    /* Sanity check */
    assert(dslot < priv->max_blocks);

    /* Directory entry should be writable */
    assert(s3b_dcache_entry_write_ok(priv, dslot, block_num, dirty));

    /* If cache file is older format, it doesn't store dirty blocks, so just erase it instead (prior behavior) */
    if (dirty && (priv->flags & HDRFLG_NEW_FORMAT) == 0) {
        s3b_dcache_erase_block(priv, dslot);
        return 0;
    }

    /* Make sure any new data is written to disk before updating the directory */
    if ((r = s3b_dcache_fsync(priv)) != 0)
        return r;

    /* Update directory */
    memset(&entry, 0, sizeof(entry));
    entry.block_num = block_num;
    entry.flags = dirty ? ENTFLG_DIRTY : 0;
    if (!dirty)
        memcpy(&entry.md5, md5, MD5_DIGEST_LENGTH);
    if ((r = s3b_dcache_write_entry(priv, dslot, &entry)) != 0)
        return r;

    /* Done */
    return 0;
}

/*
 * Erase the directory entry for a dslot. After this function is called, the block will
 * no longer be visible in the directory after a restart.
 *
 * This should be called BEFORE any new data for the block is written.
 *
 * There MUST be a directory entry for the block.
 */
int
s3b_dcache_erase_block(struct s3b_dcache *priv, u_int dslot)
{
    int r;

    /* Sanity check */
    assert(dslot < priv->max_blocks);

    /* Update directory */
    if ((r = s3b_dcache_write_entry(priv, dslot, &zero_entry)) != 0)
        return r;

    /* Make sure directory entry is written to disk before any new data is written */
    if ((r = s3b_dcache_fsync(priv)) != 0)
        return r;

    /* Done */
    return 0;
}

/*
 * Free a no-longer used dslot.
 *
 * There MUST NOT be a directory entry for the block.
 */
int
s3b_dcache_free_block(struct s3b_dcache *priv, u_int dslot)
{
    int r;

    /* Sanity check */
    assert(dslot < priv->max_blocks);

    /* Directory entry should be empty */
    assert(s3b_dcache_entry_is_empty(priv, dslot));

    /* Push dslot onto free list */
    if ((r = s3b_dcache_push(priv, dslot)) != 0)
        return r;

    /* Done */
    priv->num_alloc--;
    return 0;
}

/*
 * Read data from one dslot.
 */
int
s3b_dcache_read_block(struct s3b_dcache *priv, u_int dslot, void *dest, u_int off, u_int len)
{
    /* Sanity check */
    assert(dslot < priv->max_blocks);
    assert(off <= priv->block_size);
    assert(len <= priv->block_size);
    assert(off + len <= priv->block_size);

    /* Read data */
    return s3b_dcache_read(priv, DATA_OFFSET(priv, dslot) + off, dest, len);
}

/*
 * Write data into one dslot.
 */
int
s3b_dcache_write_block(struct s3b_dcache *priv, u_int dslot, const void *src, u_int off, u_int len)
{
    /* Sanity check */
    assert(dslot < priv->max_blocks);
    assert(off <= priv->block_size);
    assert(len <= priv->block_size);
    assert(off + len <= priv->block_size);

    /* Write data */
    return s3b_dcache_write(priv, DATA_OFFSET(priv, dslot) + off, src != NULL ? src : priv->zero_block, len);
}

/*
 * Synchronize outstanding changes to persistent storage.
 */
int
s3b_dcache_fsync(struct s3b_dcache *priv)
{
    int r;

#if HAVE_DECL_FDATASYNC
    r = fdatasync(priv->fd);
#else
    r = fsync(priv->fd);
#endif
    if (r == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "error fsync'ing cache file `%s': %s", priv->filename, strerror(r));
    }
    return 0;
}

/* Internal functions */

#ifndef NDEBUG
static int
s3b_dcache_entry_is_empty(struct s3b_dcache *priv, u_int dslot)
{
    struct dir_entry entry;

    (void)s3b_dcache_read_entry(priv, dslot, &entry);
    return memcmp(&entry, &zero_entry, sizeof(entry)) == 0;
}

static int
s3b_dcache_entry_write_ok(struct s3b_dcache *priv, u_int dslot, s3b_block_t block_num, u_int dirty)
{
    struct dir_entry entry;
    u_int old_dirty;

    if (s3b_dcache_entry_is_empty(priv, dslot))
        return 1;
    (void)s3b_dcache_read_entry(priv, dslot, &entry);
    old_dirty = (entry.flags & ENTFLG_DIRTY) != 0;
    return entry.block_num == block_num && old_dirty != dirty;
}

static int
s3b_dcache_read_entry(struct s3b_dcache *priv, u_int dslot, struct dir_entry *entry)
{
    assert(dslot < priv->max_blocks);
    memset(entry, 0, sizeof(*entry));
    return s3b_dcache_read(priv, DIR_OFFSET(priv->flags, dslot), entry, DIR_ENTSIZE(priv->flags));
}
#endif

/*
 * Write a directory entry.
 */
static int
s3b_dcache_write_entry(struct s3b_dcache *priv, u_int dslot, const struct dir_entry *entry)
{
    assert(dslot < priv->max_blocks);
    assert((entry->flags & ~((priv->flags & HDRFLG_NEW_FORMAT) != 0 ? ENTFLG_MASK : 0)) == 0);
    return s3b_dcache_write(priv, DIR_OFFSET(priv->flags, dslot), entry, DIR_ENTSIZE(priv->flags));
}

/*
 * Resize (and compress) an existing cache file. Upon successful return, priv->fd is closed
 * and the cache file must be re-opened.
 */
static int
s3b_dcache_resize_file(struct s3b_dcache *priv, const struct file_header *old_header)
{
    const u_int old_max_blocks = old_header->max_blocks;
    const u_int new_max_blocks = priv->max_blocks;
    struct file_header new_header;
    off_t old_data_base;
    off_t new_data_base;
    u_int base_old_dslot;
    u_int new_dslot = 0;
    u_int num_entries;
    u_char *block_buf = NULL;
    char *tempfile = NULL;
    int new_fd = -1;
    int r;

    /* Create new temporary cache file */
    if (asprintf(&tempfile, "%s.new", priv->filename) == -1) {
        r = errno;
        tempfile = NULL;
        (*priv->log)(LOG_ERR, "can't allocate string: %s", strerror(r));
        goto fail;
    }
    if ((r = s3b_dcache_create_file(priv, &new_fd, tempfile, new_max_blocks, &new_header)) != 0)
        goto fail;

    /* Allocate block data buffer */
    if ((block_buf = malloc(priv->block_size)) == NULL) {
        r = errno;
        (*priv->log)(LOG_ERR, "can't allocate buffer: %s", strerror(r));
        goto fail;
    }

    /* Copy non-empty cache entries from old file to new file */
    old_data_base = ROUNDUP2(DIR_OFFSET(old_header->flags, old_max_blocks), old_header->data_align);
    new_data_base = ROUNDUP2(DIR_OFFSET(new_header.flags, new_max_blocks), new_header.data_align);
    for (base_old_dslot = 0; base_old_dslot < old_max_blocks; base_old_dslot += num_entries) {
        char buffer[DIRECTORY_READ_CHUNK * DIR_ENTSIZE(old_header->flags)];
        int i;

        /* Read in the next chunk of old directory entries */
        num_entries = old_max_blocks - base_old_dslot;
        if (num_entries > DIRECTORY_READ_CHUNK)
            num_entries = DIRECTORY_READ_CHUNK;
        if ((r = s3b_dcache_read(priv, DIR_OFFSET(old_header->flags, base_old_dslot),
          buffer, num_entries * DIR_ENTSIZE(old_header->flags))) != 0) {
            (*priv->log)(LOG_ERR, "error reading cache file `%s' directory: %s", priv->filename, strerror(r));
            goto fail;
        }

        /* For each dslot: if not free, copy it to the next slot in the new file */
        for (i = 0; i < num_entries; i++) {
            const u_int old_dslot = base_old_dslot + i;
            struct dir_entry entry;
            off_t old_data;
            off_t new_data;

            /* Read old entry */
            memset(&entry, 0, sizeof(entry));
            memcpy(&entry, buffer + i * DIR_ENTSIZE(old_header->flags), DIR_ENTSIZE(old_header->flags));

            /* Is this entry non-empty? */
            if (memcmp(&entry, &zero_entry, sizeof(entry)) == 0)
                continue;

            /* Any more space? */
            if (new_dslot == new_max_blocks) {
                (*priv->log)(LOG_INFO, "cache file `%s' contains more than %u blocks; some will be discarded",
                  priv->filename, new_max_blocks);
                goto done;
            }

            /* Copy the directory entry */
            assert(DIR_ENTSIZE(new_header.flags) == sizeof(entry));
            if ((r = s3b_dcache_write2(priv, new_fd, tempfile,
              DIR_OFFSET(new_header.flags, new_dslot), &entry, sizeof(entry))) != 0)
                goto fail;

            /* Copy the data block */
            old_data = old_data_base + (off_t)old_dslot * priv->block_size;
            new_data = new_data_base + (off_t)new_dslot * priv->block_size;
            if ((r = s3b_dcache_read(priv, old_data, block_buf, priv->block_size)) != 0)
                goto fail;
            if ((r = s3b_dcache_write2(priv, new_fd, tempfile, new_data, block_buf, priv->block_size)) != 0)
                goto fail;

            /* Advance to the next slot */
            new_dslot++;
        }
    }

done:
    /* Close the new file */
    if (close(new_fd) == -1) {
        (*priv->log)(LOG_ERR, "error closing temporary cache file `%s': %s", tempfile, strerror(r));
        goto fail;
    }
    new_fd = -1;

    /* Replace old cache file with new cache file */
    if (rename(tempfile, priv->filename) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "error renaming `%s' to `%s': %s", tempfile, priv->filename, strerror(r));
        goto fail;
    }
    free(tempfile);
    tempfile = NULL;

    /* Update flags */
    priv->flags = new_header.flags;

    /* Close old file to release it and we're done */
    close(priv->fd);
    priv->fd = -1;
    r = 0;

fail:
    /* Clean up */
    if (block_buf != NULL)
        free(block_buf);
    if (new_fd != -1)
        (void)close(new_fd);
    if (tempfile != NULL) {
        (void)unlink(tempfile);
        free(tempfile);
    }
    return r;
}

static int
s3b_dcache_create_file(struct s3b_dcache *priv, int *fdp, const char *filename, u_int max_blocks, struct file_header *headerp)
{
    struct file_header header;
    int r;

    /* Initialize header */
    memset(&header, 0, sizeof(header));
    header.signature = DCACHE_SIGNATURE;
    header.flags = HDRFLG_NEW_FORMAT;
    header.header_size = HDR_SIZE(header.flags);
    header.u_int_size = sizeof(u_int);
    header.s3b_block_t_size = sizeof(s3b_block_t);
    header.block_size = priv->block_size;
    header.max_blocks = priv->max_blocks;
    header.data_align = getpagesize();

    /* Create file */
    if ((*fdp = open(filename, O_RDWR|O_CREAT|O_EXCL, 0644)) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "can't create file `%s': %s", filename, strerror(r));
        return r;
    }

    /* Write header */
    if ((r = s3b_dcache_write2(priv, *fdp, filename, (off_t)0, &header, sizeof(header))) != 0) {
        (*priv->log)(LOG_ERR, "error initializing cache file `%s': %s", filename, strerror(r));
        goto fail;
    }

    /* Extend the file to the required length; the directory will be filled with zeroes */
    if (ftruncate(*fdp, sizeof(header)) == -1 || ftruncate(*fdp, DIR_OFFSET(header.flags, max_blocks)) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "error initializing cache file `%s': %s", filename, strerror(r));
        goto fail;
    }

    /* Done */
    if (headerp != NULL)
        *headerp = header;
    return 0;

fail:
    (void)unlink(filename);
    (void)close(*fdp);
    *fdp = -1;
    return r;
}

static int
s3b_dcache_init_free_list(struct s3b_dcache *priv, s3b_dcache_visit_t *visitor, void *arg, u_int visit_dirty)
{
    off_t required_size;
    struct stat sb;
    u_int num_entries;
    u_int num_dslots_used;
    u_int base_dslot;
    u_int i;
    int r;

    /* Logging */
    (*priv->log)(LOG_INFO, "reading meta-data from cache file `%s'", priv->filename);
    assert(visitor != NULL);

    /* Inspect all directory entries */
    for (num_dslots_used = base_dslot = 0; base_dslot < priv->max_blocks; base_dslot += num_entries) {
        char buffer[DIRECTORY_READ_CHUNK * DIR_ENTSIZE(priv->flags)];

        /* Read in the next chunk of directory entries */
        num_entries = priv->max_blocks - base_dslot;
        if (num_entries > DIRECTORY_READ_CHUNK)
            num_entries = DIRECTORY_READ_CHUNK;
        if ((r = s3b_dcache_read(priv, DIR_OFFSET(priv->flags, base_dslot), buffer, num_entries * DIR_ENTSIZE(priv->flags))) != 0) {
            (*priv->log)(LOG_ERR, "error reading cache file `%s' directory: %s", priv->filename, strerror(r));
            return r;
        }

        /* For each dslot: if free, add to the free list, else let visitor decide what to do */
        for (i = 0; i < num_entries; i++) {
            const u_int dslot = base_dslot + i;
            struct dir_entry entry;

            memset(&entry, 0, sizeof(entry));
            memcpy(&entry, buffer + i * DIR_ENTSIZE(priv->flags), DIR_ENTSIZE(priv->flags));
            if (memcmp(&entry, &zero_entry, sizeof(entry)) == 0) {
                if ((r = s3b_dcache_push(priv, dslot)) != 0)
                    return r;
            } else if ((entry.flags & ENTFLG_DIRTY) != 0 && !visit_dirty) {     /* visitor doesn't want dirties, so just nuke it */
                if ((r = s3b_dcache_write_entry(priv, dslot, &zero_entry)) != 0)
                    return r;
                if ((r = s3b_dcache_push(priv, dslot)) != 0)
                    return r;
            } else {
                priv->num_alloc++;
                if (dslot + 1 > num_dslots_used)                    /* keep track of the number of dslots in use */
                    num_dslots_used = dslot + 1;
                if ((r = (*visitor)(arg, dslot, entry.block_num, (entry.flags & ENTFLG_DIRTY) == 0 ? entry.md5 : NULL)) != 0)
                    return r;
            }
        }
    }

    /* Reverse the free list so we allocate lower numbered slots first */
    for (i = 0; i < priv->free_list_len / 2; i++) {
        const s3b_block_t temp = priv->free_list[i];

        priv->free_list[i] = priv->free_list[priv->free_list_len - i - 1];
        priv->free_list[priv->free_list_len - i - 1] = temp;
    }

    /* Verify the cache file is not truncated */
    required_size = DIR_OFFSET(priv->flags, priv->max_blocks);
    if (num_dslots_used > 0) {
        if (required_size < DATA_OFFSET(priv, num_dslots_used))
            required_size = DATA_OFFSET(priv, num_dslots_used);
    }
    if (fstat(priv->fd, &sb) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "error reading cache file `%s' length: %s", priv->filename, strerror(r));
        return r;
    }
    if (sb.st_size < required_size) {
        (*priv->log)(LOG_ERR, "cache file `%s' is truncated (has size %ju < %ju bytes)",
          priv->filename, (uintmax_t)sb.st_size, (uintmax_t)required_size);
        return EINVAL;
    }

    /* Discard any unreferenced data beyond the last entry */
    if (sb.st_size > required_size && ftruncate(priv->fd, required_size) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "error trimming cache file `%s' to %ju bytes: %s",
          priv->filename, (uintmax_t)required_size, strerror(r));
        return EINVAL;
    }

    /* Report results */
    (*priv->log)(LOG_INFO, "loaded cache file `%s' with %u free and %u used blocks (max index %u)",
      priv->filename, priv->free_list_len, priv->max_blocks - priv->free_list_len, num_dslots_used);

    /* Done */
    return 0;
}

/*
 * Push a dslot onto the free list.
 */
static int
s3b_dcache_push(struct s3b_dcache *priv, u_int dslot)
{
    /* Sanity check */
    assert(dslot < priv->max_blocks);
    assert(priv->free_list_len < priv->max_blocks);

    /* Grow the free list array if necessary */
    if (priv->free_list_alloc == priv->free_list_len) {
        s3b_block_t *new_free_list;
        s3b_block_t new_free_list_alloc;
        int r;

        new_free_list_alloc = priv->free_list_alloc == 0 ? 1024 : 2 * priv->free_list_alloc;
        if ((new_free_list = realloc(priv->free_list, new_free_list_alloc * sizeof(*new_free_list))) == NULL) {
            r = errno;
            (*priv->log)(LOG_ERR, "realloc: %s", strerror(r));
            return r;
        }
        priv->free_list = new_free_list;
        priv->free_list_alloc = new_free_list_alloc;
    }

    /* Add new dslot */
    assert(priv->free_list_len < priv->free_list_alloc);
    priv->free_list[priv->free_list_len++] = dslot;
    return 0;
}

/*
 * Pop the next dslot off of the free list. There must be one.
 */
static void
s3b_dcache_pop(struct s3b_dcache *priv, u_int *dslotp)
{
    /* Sanity check */
    assert(priv->free_list_len > 0);

    /* Pop off dslot at the head of the list */
    *dslotp = priv->free_list[--priv->free_list_len];
    assert(*dslotp < priv->max_blocks);

    /* See if we can give back some memory */
    if (priv->free_list_alloc > 1024 && priv->free_list_len <= priv->free_list_alloc / 4) {
        s3b_block_t *new_free_list;
        s3b_block_t new_free_list_alloc;

        new_free_list_alloc = priv->free_list_alloc / 4;
        if ((new_free_list = realloc(priv->free_list, new_free_list_alloc * sizeof(*new_free_list))) == NULL)
            (*priv->log)(LOG_ERR, "can't shrink dcache free list: realloc: %s (ignored)", strerror(errno));
        else {
            priv->free_list = new_free_list;
            priv->free_list_alloc = new_free_list_alloc;
        }
    }
    assert(priv->free_list_len <= priv->free_list_alloc);
}

static int
s3b_dcache_read(struct s3b_dcache *priv, off_t offset, void *data, size_t len)
{
    size_t sofar;
    ssize_t r;

    for (sofar = 0; sofar < len; sofar += r) {
        const off_t posn = offset + sofar;

        if ((r = pread(priv->fd, (char *)data + sofar, len - sofar, offset + sofar)) == -1) {
            (*priv->log)(LOG_ERR, "error reading cache file `%s' at offset %ju: %s",
              priv->filename, (uintmax_t)posn, strerror(r));
            return r;
        }
        if (r == 0) {           /* truncated input */
            (*priv->log)(LOG_ERR, "error reading cache file `%s' at offset %ju: file is truncated",
              priv->filename, (uintmax_t)posn);
            return EINVAL;
        }
    }
    return 0;
}

static int
s3b_dcache_write(struct s3b_dcache *priv, off_t offset, const void *data, size_t len)
{
    return s3b_dcache_write2(priv, priv->fd, priv->filename, offset, data, len);
}

static int
s3b_dcache_write2(struct s3b_dcache *priv, int fd, const char *filename, off_t offset, const void *data, size_t len)
{
    size_t sofar;
    ssize_t r;

    for (sofar = 0; sofar < len; sofar += r) {
        const off_t posn = offset + sofar;

        if ((r = pwrite(fd, (const char *)data + sofar, len - sofar, offset + sofar)) == -1) {
            (*priv->log)(LOG_ERR, "error writing cache file `%s' at offset %ju: %s",
              filename, (uintmax_t)posn, strerror(r));
            return r;
        }
    }
    return 0;
}

