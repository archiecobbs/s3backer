
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
 */

/*
 * Simple on-disk persistent cache.
 */

/* Definitions */
typedef int s3b_dcache_visit_t(void *arg, s3b_block_t dslot, s3b_block_t block_num, const u_char *md5);

/* Declarations */
struct s3b_dcache;

/* dcache.c */
extern int s3b_dcache_open(struct s3b_dcache **dcachep, log_func_t *log, const char *filename,
  u_int block_size, u_int max_blocks, s3b_dcache_visit_t *visitor, void *arg);
extern void s3b_dcache_close(struct s3b_dcache *dcache);
extern u_int s3b_dcache_size(struct s3b_dcache *dcache);
extern int s3b_dcache_alloc_block(struct s3b_dcache *priv, u_int *dslotp);
extern int s3b_dcache_record_block(struct s3b_dcache *priv, u_int dslot, s3b_block_t block_num, const u_char *md5);
extern int s3b_dcache_erase_block(struct s3b_dcache *priv, u_int dslot);
extern int s3b_dcache_free_block(struct s3b_dcache *dcache, u_int dslot);
extern int s3b_dcache_read_block(struct s3b_dcache *dcache, u_int dslot, void *dest, u_int off, u_int len);
extern int s3b_dcache_write_block(struct s3b_dcache *dcache, u_int dslot, const void *src, u_int off, u_int len);
extern int s3b_dcache_fsync(struct s3b_dcache *dcache);

