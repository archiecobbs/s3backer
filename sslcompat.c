/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 *
 * Copyright 2008-2023 Archie L. Cobbs <archie.cobbs@gmail.com>
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

// Issue #64 OpenSSL 1.1.0 compatibility
#if OPENSSL_VERSION_NUMBER < 0x10100000L

/*
 * OpenSSL does not allow for HMAC_CTX or EVP_MD_CTX to be allocated on the
 * stack. Instead it provides a set of _new and _free functions for dynamic
 * allocation that do not exist in the older versions of the library. For
 * older OpenSSL versions we provide our own implementations of these missing
 * functions.
 */

HMAC_CTX *HMAC_CTX_new(void)
{
    HMAC_CTX *ctx = OPENSSL_malloc(sizeof(*ctx));
    if (ctx != NULL) {
        HMAC_CTX_init(ctx);
    }
    return ctx;
}

void HMAC_CTX_free(HMAC_CTX *ctx)
{
    if (ctx != NULL) {
        HMAC_CTX_cleanup(ctx);
        OPENSSL_free(ctx);
    }
}

EVP_MD_CTX *EVP_MD_CTX_new(void)
{
    EVP_MD_CTX *ctx = OPENSSL_malloc(sizeof(*ctx));
    if (NULL != ctx) {
        EVP_MD_CTX_init(ctx);
    }
    return ctx;
}

void EVP_MD_CTX_free(EVP_MD_CTX *ctx)
{
    if (ctx != NULL) {
        EVP_MD_CTX_cleanup(ctx);
        OPENSSL_free(ctx);
    }
}

#endif
