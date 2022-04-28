
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

/*
 * Simple on-disk persistent cache.
 */

// Declarations
struct s3b_dcache;

/*
 * Startup visitor callback. Each non-empty slot in the disk cache is visited.
 *
 * The "etag" pointer is NULL for dirty blocks, and not NULL for clean blocks.
 */
typedef int s3b_dcache_visit_t(void *arg, s3b_block_t dslot, s3b_block_t block_num, const u_char *etag);

// dcache.c
extern int s3b_dcache_open(struct s3b_dcache **dcachep, log_func_t *log, const char *filename,
  u_int block_size, u_int max_blocks, s3b_dcache_visit_t *visitor, void *arg, u_int visit_dirty);
extern void s3b_dcache_close(struct s3b_dcache *dcache);
extern u_int s3b_dcache_size(struct s3b_dcache *dcache);
extern int s3b_dcache_alloc_block(struct s3b_dcache *priv, u_int *dslotp);
extern int s3b_dcache_record_block(struct s3b_dcache *priv, u_int dslot, s3b_block_t block_num, const u_char *etag);
extern int s3b_dcache_erase_block(struct s3b_dcache *priv, u_int dslot);
extern int s3b_dcache_free_block(struct s3b_dcache *dcache, u_int dslot);
extern int s3b_dcache_read_block(struct s3b_dcache *dcache, u_int dslot, void *dest, u_int off, u_int len);
extern int s3b_dcache_write_block(struct s3b_dcache *dcache, u_int dslot, const void *src, u_int off, u_int len);
extern int s3b_dcache_fsync(struct s3b_dcache *dcache);
extern int s3b_dcache_has_mount_token(struct s3b_dcache *priv);
extern int s3b_dcache_set_mount_token(struct s3b_dcache *priv, int32_t *old_valuep, int32_t new_value);

