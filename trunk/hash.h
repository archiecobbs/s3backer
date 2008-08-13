
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

/*
 * Our hash table implementation.
 *
 * We make the following simplifying assumptions:
 *
 * 1.  Keys are of type s3b_block_t
 * 2.  Values are structures in which the first field is the key
 * 3.  No attempts will be made to overload the table
 */

/* Definitions */
typedef void s3b_hash_visit_t(void *arg, void *value);

/* Declarations */
struct s3b_hash;

/* hash.c */
extern int s3b_hash_create(struct s3b_hash **hashp, u_int maxkeys);
extern void s3b_hash_destroy(struct s3b_hash *hash);
extern u_int s3b_hash_size(struct s3b_hash *hash);
extern void *s3b_hash_get(struct s3b_hash *hash, s3b_block_t key);
extern void s3b_hash_put(struct s3b_hash *hash, void *value);
extern void s3b_hash_remove(struct s3b_hash *hash, s3b_block_t key);
extern void s3b_hash_foreach(struct s3b_hash *hash, s3b_hash_visit_t visitor, void *arg);

