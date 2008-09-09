
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
 * This is a simple closed hash table implementation with linear probing.
 * We pre-allocate the hash array based on the expected maximum size.
 */

#include "s3backer.h"
#include "hash.h"

/* Definitions */
#define LOAD_FACTOR     0.666666

/* Hash table structure */
struct s3b_hash {
    u_int       maxkeys;            /* max capacity */
    u_int       numkeys;            /* number of keys in table */
    u_int       alen;               /* hash array length */
    void        *array[0];          /* hash array */
};

/* Declarations */
static u_int s3b_hash_index(struct s3b_hash *hash, s3b_block_t key);

/* Public functions */

int
s3b_hash_create(struct s3b_hash **hashp, u_int maxkeys)
{
    struct s3b_hash *hash;
    u_int alen;

    if (maxkeys >= (u_int)(UINT_MAX * LOAD_FACTOR) - 1)
        return EINVAL;
    alen = (u_int)(maxkeys / LOAD_FACTOR) + 1;
    if ((hash = calloc(1, sizeof(*hash) + alen * sizeof(*hash->array))) == NULL)
        return ENOMEM;
    hash->maxkeys = maxkeys;
    hash->alen = alen;
    *hashp = hash;
    return 0;
}

void
s3b_hash_destroy(struct s3b_hash *hash)
{
    free(hash);
}

u_int
s3b_hash_size(struct s3b_hash *hash)
{
    return hash->numkeys;
}

void *
s3b_hash_get(struct s3b_hash *hash, s3b_block_t key)
{
    void *value;
    u_int i;

    for (i = s3b_hash_index(hash, key); (value = hash->array[i]) != NULL; i = i + 1 < hash->alen ? i + 1 : 0) {
        if (*(s3b_block_t *)value == key)
            return (void *)value;
    }
    return NULL;
}

void
s3b_hash_put(struct s3b_hash *hash, void *value)
{
    const s3b_block_t key = *(s3b_block_t *)value;
    void *entry;
    u_int i;

    for (i = s3b_hash_index(hash, key); (entry = hash->array[i]) != NULL; i = i + 1 < hash->alen ? i + 1 : 0) {
        if (*(s3b_block_t *)entry == key) {
            hash->array[i] = value;
            return;
        }
    }
    assert(hash->numkeys < hash->maxkeys);
    hash->array[i] = value;
    hash->numkeys++;
}

void
s3b_hash_remove(struct s3b_hash *hash, s3b_block_t key)
{
    void *value;
    u_int i;
    u_int j;
    u_int k;

    /* Find entry */
    for (i = s3b_hash_index(hash, key); (value = hash->array[i]) != NULL; i = i + 1 < hash->alen ? i + 1 : 0) {
        if (*(s3b_block_t *)value == key)
            break;
    }
    if (value == NULL)
        return;

    /* Repair subsequent entries as necessary */
    for (j = i + 1 < hash->alen ? i + 1 : 0; (value = hash->array[j]) != NULL; j = j + 1 < hash->alen ? j + 1 : 0) {
        k = s3b_hash_index(hash, *(s3b_block_t *)value);
        if (j > i ? (k <= i || k > j) : (k <= i && k > j)) {
            hash->array[i] = value;
            i = j;
        }
    }

    /* Remove entry */
    assert(hash->array[i] != NULL);
    hash->array[i] = NULL;
    hash->numkeys--;
}

void
s3b_hash_foreach(struct s3b_hash *hash, s3b_hash_visit_t visitor, void *arg)
{
    void *value;
    u_int i;

    for (i = 0; i < hash->alen; i++) {
        if ((value = hash->array[i]) != NULL)
            (*visitor)(arg, value);
    }
}

/*
 * Jenkins one-at-a-time hash
 */
static u_int
s3b_hash_index(struct s3b_hash *hash, s3b_block_t key)
{
    u_int value = 0;
    int i;
 
    for (i = 0; i < sizeof(key); i++) {
        value += ((u_char *)&key)[i];
        value += (value << 10);
        value ^= (value >> 6);
    }
    value += (value << 3);
    value ^= (value >> 11);
    value += (value << 15);
    return value % hash->alen;
}

