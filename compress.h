
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
 * Compression function
 *
 * Returns 0 on success, otherwise (positive) error code.
 *
 * log - where to log errors
 * input - the data to compress
 * inlen - length of input
 * outputp - on successful return, points to compressed data in a malloc'd buffer
 * outlenp - on successful return, length of *outputp
 * level - compression level info from parse function, or NULL for default
 */
typedef int         comp_cfunc_t(log_func_t *log, const void *input, size_t inlen, void **outputp, size_t *outlenp, void *level);

/*
 * Decompression function
 *
 * Returns 0 on success, otherwise (positive) error code.
 *
 * log - where to log errors
 * input - the data to decompress
 * inlen - length of input
 * output - buffer for decompressed data, having length at least *outlenp
 * outlenp
 *      - on invocation, points to maximum possible/expected length of decompressed data
 *      - on successful return, points to the actual length of decompressed data
 */
typedef int         comp_dfunc_t(log_func_t *log, const void *input, size_t inlen, void *output, size_t *outlenp);

/*
 * Compression level parsing function.
 *
 * Returns opaque level information on success, otherwise prints error to stderr and returns NULL.
 *
 * This function is only invoked if a "--compress-level=xxx" flag is given on the command line. If so, then the "xxx" must
 * be parsed successfully by this function and a non-NULL result returned. The returned result (or NULL if no such command
 * line flag was given) is passed as the "level" parameter to the compression function.
 *
 * string - compression level string
 */
typedef void        *comp_lparse_t(const char *string);

// Compression algorithms
struct comp_alg {
    const char      *name;
    comp_cfunc_t    *cfunc;
    comp_dfunc_t    *dfunc;
    comp_lparse_t   *lparse;
};

// Globals
extern const size_t num_comp_algs;
extern const struct comp_alg comp_algs[];

// Functions
extern const struct comp_alg *comp_find(const char *name);
