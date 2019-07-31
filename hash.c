
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

/*
 * This is a simple closed hash table implementation with linear probing.
 * We pre-allocate the hash array based on the expected maximum size.
 */

#include "s3backer.h"
#include "hash.h"

/* Definitions */
#define LOAD_FACTOR                 0.666666
#define FIRST(hash, key)            (s3b_hash_index((hash), (key)))
#define NEXT(hash, index)           ((index) + 1 < (hash)->alen ? (index) + 1 : 0)
#define EMPTY(value)                ((value) == NULL)
#define VALUE(hash, index)          ((hash)->array[(index)])
#define KEY(value)                  (*(s3b_block_t *)(value))

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
    u_int i;

    for (i = FIRST(hash, key); 1; i = NEXT(hash, i)) {
        void *const value = VALUE(hash, i);

        if (EMPTY(value))
            return NULL;
        if (KEY(value) == key)
            return value;
    }
}

/*
 * Add/replace entry.
 *
 * Note that the value being replaced (if any) is referenced by this function,
 * so it should not be free'd until after this function returns.
 */
void *
s3b_hash_put(struct s3b_hash *hash, void *value)
{
    const s3b_block_t key = KEY(value);
    u_int i;

    for (i = FIRST(hash, key); 1; i = NEXT(hash, i)) {
        void *const value2 = VALUE(hash, i);

        if (EMPTY(value))
            break;
        if (KEY(value2) == key) {
            VALUE(hash, i) = value;         /* replace existing value having the same key with new value */
            return value2;
        }
    }
    assert(hash->numkeys < hash->maxkeys);
    VALUE(hash, i) = value;
    hash->numkeys++;
    return NULL;
}

/*
 * Optimization of s3b_hash_put() for when it is known that no matching entry exists.
 */
void
s3b_hash_put_new(struct s3b_hash *hash, void *value)
{
    const s3b_block_t key = KEY(value);
    u_int i;

    for (i = FIRST(hash, key); 1; i = NEXT(hash, i)) {
        void *const value2 = VALUE(hash, i);

        if (EMPTY(value2))
            break;
        assert(KEY(value2) != key);
    }
    assert(hash->numkeys < hash->maxkeys);
    VALUE(hash, i) = value;
    hash->numkeys++;
}

void
s3b_hash_remove(struct s3b_hash *hash, s3b_block_t key)
{
    u_int i;
    u_int j;
    u_int k;

    /* Find entry */
    for (i = FIRST(hash, key); 1; i = NEXT(hash, i)) {
        void *const value = VALUE(hash, i);

        if (EMPTY(value))               /* no such entry */
            return;
        if (KEY(value) == key)          /* entry found */
            break;
    }

    /* Repair subsequent entries as necessary */
    for (j = NEXT(hash, i); 1; j = NEXT(hash, j)) {
        void *const value = VALUE(hash, j);

        if (value == NULL)
            break;
        k = FIRST(hash, KEY(value));
        if (j > i ? (k <= i || k > j) : (k <= i && k > j)) {
            VALUE(hash, i) = value;
            i = j;
        }
    }

    /* Remove entry */
    assert(VALUE(hash, i) != NULL);
    VALUE(hash, i) = NULL;
    hash->numkeys--;
}

void
s3b_hash_foreach(struct s3b_hash *hash, s3b_hash_visit_t *visitor, void *arg)
{
    u_int i;

    for (i = 0; i < hash->alen; i++) {
        void *const value = VALUE(hash, i);

        if (value != NULL)
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

