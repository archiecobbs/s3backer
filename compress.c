
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

#include "s3backer.h"
#include "compress.h"
#ifdef ZSTD
 #include <zstd.h>
#endif
// Deflate
static comp_cfunc_t    deflate_compress;
static comp_dfunc_t    deflate_decompress;
static comp_lparse_t   deflate_lparse;
static comp_lfree_t    deflate_lfree;

// zstd
static comp_cfunc_t    zstd_compress;
static comp_dfunc_t    zstd_decompress;
static comp_lparse_t   zstd_lparse;
static comp_lfree_t    zstd_lfree;

// Compression algorithms
const struct comp_alg comp_algs[] = {
#if COMP_ALG_ZLIB != 0
#error incorrect COMP_ALG_ZLIB
#endif
    {
        .name=      "deflate",
        .cfunc=     deflate_compress,
        .dfunc=     deflate_decompress,
        .lparse=    deflate_lparse,
        .lfree=     deflate_lfree
    }
#ifdef ZSTD
    , {
        .name=      "zstd",
        .cfunc=     zstd_compress,
        .dfunc=     zstd_decompress,
        .lparse=    zstd_lparse,
        .lfree=     zstd_lfree
    }
#endif
};
const size_t num_comp_algs = sizeof(comp_algs) / sizeof(*comp_algs);

/****************************************************************************
 *                          GENERAL PURPOSE                                 *
 ****************************************************************************/

const struct comp_alg *
comp_find(const char *name)
{
    int i;

    for (i = 0; i < num_comp_algs; i++) {
        const struct comp_alg *calg = &comp_algs[i];

        if (strcasecmp(name, calg->name) == 0)
            return calg;
    }
    return NULL;
}

/****************************************************************************
 *                                DEFLATE                                   *
 ****************************************************************************/

static int
deflate_compress(log_func_t *log, const void *input, size_t inlen, void **outputp, size_t *outlenp, void *levelp)
{
    u_long clen;
    void *cbuf;
    int level;
    int r;

    // Allocate buffer
    clen = compressBound(inlen);
    if ((cbuf = malloc(clen)) == NULL) {
        r = errno;
        (*log)(LOG_ERR, "malloc: %s", strerror(r));
        return r;
    }

    // Extract compression level
    level = levelp != NULL ? *(int *)levelp : Z_DEFAULT_COMPRESSION;

    // Compress data
    r = compress2(cbuf, &clen, input, inlen, level);
    switch (r) {
    case Z_OK:
        *outputp = cbuf;
        *outlenp = clen;
        return 0;
    case Z_MEM_ERROR:
        (*log)(LOG_ERR, "zlib compress: %s", strerror(ENOMEM));
        r = ENOMEM;
        break;
    default:
        (*log)(LOG_ERR, "zlib compress: error %d", r);
        r = EIO;
        break;
    }

    // Fail
    free(cbuf);
    return r;
}

static int
deflate_decompress(log_func_t *log, const void *input, size_t inlen, void *output, size_t *outlenp)
{
    u_long uclen = *outlenp;
    int r;

    switch ((r = uncompress(output, &uclen, input, inlen))) {
    case Z_OK:
        *outlenp = uclen;
        return 0;
    case Z_MEM_ERROR:
        (*log)(LOG_ERR, "zlib uncompress: %s", strerror(ENOMEM));
        return ENOMEM;
    case Z_BUF_ERROR:
        (*log)(LOG_ERR, "zlib uncompress: %s", "decompressed block is oversize");
        return EIO;
    case Z_DATA_ERROR:
        (*log)(LOG_ERR, "zlib uncompress: %s", "data is corrupted or truncated");
        return EIO;
    default:
        (*log)(LOG_ERR, "zlib uncompress: error %d", r);
        return EIO;
    }
}

static void *
deflate_lparse(const char *string)
{
    char *endptr;
    long level;
    int *levelp;

    // Parse level
    errno = 0;
    level = strtol(string, &endptr, 10);
    if ((errno == ERANGE && (level == LONG_MIN || level == LONG_MAX))
      || (errno != 0 && level == 0)
      || *endptr != '\0'
      || (int)level != level)
        goto invalid;

    // Check level
    switch ((int)level) {
    case Z_DEFAULT_COMPRESSION:
    case Z_NO_COMPRESSION:
        break;
    default:
        if (level < Z_BEST_SPEED || level > Z_BEST_COMPRESSION)
            goto invalid;
        break;
    }

    // Store in buffer
    if ((levelp = malloc(sizeof(*levelp))) == NULL)
        warn("malloc");
    else
        *levelp = (int)level;
    return levelp;

invalid:
    warnx("invalid deflate compression level `%s'", string);
    return NULL;
}

static void
deflate_lfree(void *level)
{
    free(level);
}

/****************************************************************************
 *                                   ZSTD                                   *
 ****************************************************************************/

static int
zstd_compress(log_func_t *log, const void *input, size_t inlen, void **outputp, size_t *outlenp, void *levelp)
{
    u_long clen;
    void *cbuf;
    int level;
    int r;

    // Allocate buffer
    clen = ZSTD_compressBound(inlen);
    if ((cbuf = malloc(clen)) == NULL) {
        r = errno;
        (*log)(LOG_ERR, "malloc: %s", strerror(r));
        return r;
    }

    // Extract compression level
    level = levelp != NULL ? *(int *)levelp : ZSTD_defaultCLevel();

    // Compress data
    clen = ZSTD_compress(cbuf, clen, input, inlen, level);

    if(ZSTD_isError(clen)) {
        (*log)(LOG_ERR, "zstd compress: error, %s", ZSTD_getErrorName(clen));
        r = EIO;
    } else {
        *outputp = cbuf;
        *outlenp = clen;
        return 0;
    }

    // Fail
    free(cbuf);
    return r;
}

static int
zstd_decompress(log_func_t *log, const void *input, size_t inlen, void *output, size_t *outlenp)
{
    size_t code = ZSTD_decompress(output, *outlenp, input, inlen);
    if(ZSTD_isError(code)) {
        (*log)(LOG_ERR, "zstd uncompress: %s", ZSTD_getErrorName(code));
        return EIO;
    }
    *outlenp = code;
    return 0;
}

static void *
zstd_lparse(const char *string)
{
    char *endptr;
    long level;
    int *levelp;

    // Parse level
    errno = 0;
    level = strtol(string, &endptr, 10);
    if ((errno == ERANGE && (level == LONG_MIN || level == LONG_MAX))
      || (errno != 0 && level == 0)
      || *endptr != '\0'
      || (int)level != level)
        goto invalid;

    // Check level
    if( level < ZSTD_minCLevel() || level > ZSTD_maxCLevel())
        goto invalid;

    // Store in buffer
    if ((levelp = malloc(sizeof(*levelp))) == NULL)
        warn("malloc");
    else
        *levelp = (int)level;
    return levelp;

invalid:
    warnx("invalid zstd compression level `%s'", string);
    return NULL;
}

static void
zstd_lfree(void * level)
{
    free(level);
}
