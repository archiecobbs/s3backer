
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

#define DIR_OFFSET(dslot)           ((off_t)sizeof(struct file_header) + (off_t)(dslot) * sizeof(struct dir_entry))
#define DATA_OFFSET(priv, dslot)    ((off_t)(priv)->data + (off_t)(dslot) * (priv)->block_size)

/* File header */
struct file_header {
    uint32_t                        signature;
    uint32_t                        header_size;
    uint32_t                        u_int_size;
    uint32_t                        s3b_block_t_size;
    uint32_t                        block_size;
    uint32_t                        data_align;
    uint32_t                        zero;
    s3b_block_t                     max_blocks;
} __attribute__ ((packed));

/* One directory entry */
struct dir_entry {
    s3b_block_t                     block_num;
    u_char                          md5[MD5_DIGEST_LENGTH];
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
    off_t                           data;
    u_int                           free_list_len;
    u_int                           free_list_alloc;
    s3b_block_t                     *free_list;
};

/* Internal functions */
static int s3b_dcache_write_entry(struct s3b_dcache *priv, u_int dslot, const struct dir_entry *entry);
#ifndef NDEBUG
static int s3b_dcache_entry_is_empty(struct s3b_dcache *priv, u_int dslot);
static int s3b_dcache_read_entry(struct s3b_dcache *priv, u_int dslot, struct dir_entry *entryp);
#endif
static int s3b_dcache_init_file(struct s3b_dcache *priv);
static int s3b_dcache_init_free_list(struct s3b_dcache *priv, s3b_dcache_visit_t *visitor, void *arg);
static int s3b_dcache_push(struct s3b_dcache *priv, u_int dslot);
static void s3b_dcache_pop(struct s3b_dcache *priv, u_int *dslotp);
static int s3b_dcache_read(struct s3b_dcache *priv, off_t offset, void *data, size_t len);
static int s3b_dcache_write(struct s3b_dcache *priv, off_t offset, const void *data, size_t len);

/* Internal variables */
static const struct dir_entry zero_entry;

/* Public functions */

int
s3b_dcache_open(struct s3b_dcache **dcachep, log_func_t *log, const char *filename,
  u_int block_size, u_int max_blocks, s3b_dcache_visit_t *visitor, void *arg)
{
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

    /* Open/create cache file */
    if ((priv->fd = open(priv->filename, O_RDWR|O_CREAT, 0644)) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "can't open cache file `%s': %s", priv->filename, strerror(r));
        goto fail3;
    }

    /* If file is new, initialize header */
    if (fstat(priv->fd, &sb) == -1) {
        r = errno;
        goto fail4;
    }
    if (sb.st_size == 0) {
        if ((r = s3b_dcache_init_file(priv)) != 0)
            goto fail4;
    } else if (sb.st_size < DIR_OFFSET(priv->max_blocks)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': file is truncated (size %ju < %ju)",
          priv->filename, (uintmax_t)sb.st_size, (uintmax_t)DIR_OFFSET(priv->max_blocks));
        r = EINVAL;
        goto fail4;
    }

    /* Read in header */
    if ((r = s3b_dcache_read(priv, (off_t)0, &header, sizeof(header))) != 0) {
        (*priv->log)(LOG_ERR, "can't read cache file `%s' header: %s", priv->filename, strerror(r));
        goto fail4;
    }

    /* Verify header */
    r = EINVAL;
    if (header.signature != DCACHE_SIGNATURE) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': wrong signature %08x != %08x",
          priv->filename, header.signature, DCACHE_SIGNATURE);
        goto fail4;
    }
    if (header.header_size != sizeof(header)) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': %s", priv->filename, "unrecognized format");
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
    if (header.max_blocks != priv->max_blocks) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': created with capacity %u != %u blocks",
          priv->filename, header.max_blocks, priv->max_blocks);
        goto fail4;
    }
    if (header.data_align != getpagesize()) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': created with alignment %u != %u",
          priv->filename, header.data_align, getpagesize());
        goto fail4;
    }
    if (header.zero != 0) {
        (*priv->log)(LOG_ERR, "invalid cache file `%s': %s", "unrecognized field", priv->filename);
        goto fail4;
    }

    /* Compute offset of first data block */
    priv->data = ROUNDUP2(DIR_OFFSET(priv->max_blocks), header.data_align);

    /* Read the directory to build the free list and visit allocated blocks */
    if ((r = s3b_dcache_init_free_list(priv, visitor, arg)) != 0)
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
 * There MUST NOT be a directory entry for the block.
 */
int
s3b_dcache_record_block(struct s3b_dcache *priv, u_int dslot, s3b_block_t block_num, const u_char *md5)
{
    struct dir_entry entry;

    /* Sanity check */
    assert(dslot < priv->max_blocks);

    /* Directory entry should be empty */
    assert(s3b_dcache_entry_is_empty(priv, dslot));

    /* Update directory */
    entry.block_num = block_num;
    memcpy(&entry.md5, md5, MD5_DIGEST_LENGTH);
    return s3b_dcache_write_entry(priv, dslot, &entry);
}

/*
 * Erase the directory entry for a dslot. After this function is called, the block will
 * no longer be visible in the directory after a restart.
 *
 * There MUST be a directory entry for the block.
 */
int
s3b_dcache_erase_block(struct s3b_dcache *priv, u_int dslot)
{
    /* Sanity check */
    assert(dslot < priv->max_blocks);

    /* Update directory */
    return s3b_dcache_write_entry(priv, dslot, &zero_entry);
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

#if HAVE_FDATASYNC
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
s3b_dcache_read_entry(struct s3b_dcache *priv, u_int dslot, struct dir_entry *entry)
{
    assert(dslot < priv->max_blocks);
    return s3b_dcache_read(priv, DIR_OFFSET(dslot), entry, sizeof(*entry));
}
#endif

/*
 * Write a directory entry.
 */
static int
s3b_dcache_write_entry(struct s3b_dcache *priv, u_int dslot, const struct dir_entry *entry)
{
    assert(dslot < priv->max_blocks);
    return s3b_dcache_write(priv, DIR_OFFSET(dslot), entry, sizeof(*entry));
}

/*
 * Initialize a new cache file.
 */
static int
s3b_dcache_init_file(struct s3b_dcache *priv)
{
    struct file_header header;
    int r;

    /* Initialize header */
    memset(&header, 0, sizeof(header));
    header.signature = DCACHE_SIGNATURE;
    header.header_size = sizeof(header);
    header.u_int_size = sizeof(u_int);
    header.s3b_block_t_size = sizeof(s3b_block_t);
    header.block_size = priv->block_size;
    header.max_blocks = priv->max_blocks;
    header.data_align = getpagesize();

    /* Write header */
    (*priv->log)(LOG_NOTICE, "creating new cache file `%s' with capacity %ju blocks",
      priv->filename, (uintmax_t)priv->max_blocks);
    if ((r = s3b_dcache_write(priv, (off_t)0, &header, sizeof(header))) != 0)
        return r;

    /* Extend the file to the required length; the directory will be filled with zeroes */
    if (ftruncate(priv->fd, sizeof(header)) == -1 || ftruncate(priv->fd, DIR_OFFSET(priv->max_blocks)) == -1) {
        r = errno;
        (*priv->log)(LOG_ERR, "error initializing cache file `%s': %s", priv->filename, strerror(r));
        return r;
    }

    /* Done */
    s3b_dcache_fsync(priv);
    return 0;
}

static int
s3b_dcache_init_free_list(struct s3b_dcache *priv, s3b_dcache_visit_t *visitor, void *arg)
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

    /* Inspect all directory entries */
    for (num_dslots_used = base_dslot = 0; base_dslot < priv->max_blocks; base_dslot += num_entries) {
        struct dir_entry entries[DIRECTORY_READ_CHUNK];

        /* Read in the next chunk of directory entries */
        num_entries = priv->max_blocks - base_dslot;
        if (num_entries > DIRECTORY_READ_CHUNK)
            num_entries = DIRECTORY_READ_CHUNK;
        if ((r = s3b_dcache_read(priv, DIR_OFFSET(base_dslot), entries, num_entries * sizeof(*entries))) != 0) {
            (*priv->log)(LOG_ERR, "error reading cache file `%s' directory: %s", priv->filename, strerror(r));
            return r;
        }

        /* For each dslot: if free, add to the free list, else notify visitor */
        for (i = 0; i < num_entries; i++) {
            const struct dir_entry *const entry = &entries[i];
            const u_int dslot = base_dslot + i;

            if (memcmp(entry, &zero_entry, sizeof(*entry)) == 0) {
                if ((r = s3b_dcache_push(priv, dslot)) != 0)
                    return r;
            } else {
                if (dslot + 1 > num_dslots_used)                    /* keep track of the number of dslots in use */
                    num_dslots_used = dslot + 1;
                if (visitor != NULL && (r = (*visitor)(arg, dslot, entry->block_num, entry->md5)) != 0)
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
    required_size = DIR_OFFSET(priv->max_blocks);
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
        if ((new_free_list = realloc(priv->free_list, new_free_list_alloc * sizeof(*new_free_list))) != NULL) {
            (*priv->log)(LOG_ERR, "realloc: %s (ignored)", strerror(errno));
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
    size_t sofar;
    ssize_t r;

    for (sofar = 0; sofar < len; sofar += r) {
        const off_t posn = offset + sofar;

        if ((r = pwrite(priv->fd, (const char *)data + sofar, len - sofar, offset + sofar)) == -1) {
            (*priv->log)(LOG_ERR, "error writing cache file `%s' at offset %ju: %s",
              priv->filename, (uintmax_t)posn, strerror(r));
            return r;
        }
    }
    return 0;
}

