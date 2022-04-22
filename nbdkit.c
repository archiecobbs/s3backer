/* nbdkit plugin for s3backer
 *
 * Copyright (C) 2022 Nikolaus Rath <Nikolaus@rath.org>
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULA R PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 * In addition, as a special exception, the copyright holders give permission to
 * link the code of portions of this program with the OpenSSL library under
 * certain conditions as described in each individual source file, and
 * distribute linked combinations including the two.
 *
 * You must obey the GNU General Public License in all respects for all of the
 * code used other than OpenSSL. If you modify file(s) with this exception, you
 * may extend this exception to your version of the file(s), but you are not
 * obligated to do so. If you do not wish to do so, delete this exception
 * statement from your version. If you delete this exception statement from all
 * source files in the program, then also delete it here.
 */


#include <assert.h>
#include <config.h>
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "s3backer.h"
#include "block_cache.h"
#include "ec_protect.h"
#include "zero_cache.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"
#include "util.h"


#define NBDKIT_API_VERSION 2
#include <nbdkit-plugin.h>

/* Concurrent requests are supported */
#define THREAD_MODEL NBDKIT_THREAD_MODEL_PARALLEL

static char* bucket = NULL;
static char* region = NULL;
static char* conffile = NULL;
static struct s3b_config* config;
static const struct fuse_operations* fuse_ops;
static struct s3backer_store* s3b;
static struct fuse_ops_private* fuse_priv;

int s3backer_debug_internal;

/* Called for each key=value passed on the nbdkit command line */
static int
plugin_config(const char* key, const char* value)
{
  if (strcmp(key, "bucket") == 0) {
    bucket = strdup(value);
    if (!bucket)
      return -1;
  } else if (strcmp(key, "region") == 0) {
    region = strdup(value);
    if (!region)
      return -1;
  } else if (strcmp(key, "conffile") == 0) {
    conffile = nbdkit_realpath(value);
    if (!conffile)
      return -1;
  } else {
    nbdkit_error("unknown parameter '%s'", key);
    return -1;
  }

  return 0;
}

static void
nbdkit_logger(int level, const char* fmt, ...)
{
  if (!s3backer_debug_internal && level == LOG_DEBUG)
    return;

  const char* levelstr;
  switch (level) {
    case LOG_ERR:
      levelstr = "ERROR: ";
      break;
    case LOG_WARNING:
      levelstr = "WARNING: ";
      break;
    case LOG_NOTICE:
      levelstr = "NOTICE: ";
      break;
    case LOG_INFO:
      levelstr = "INFO: ";
      break;
    case LOG_DEBUG:
      levelstr = "DEBUG: ";
      break;
    default:
      levelstr = "<?>: ";
      break;
  }

  int len = strlen(fmt) + strlen(levelstr);
  char newfmt[len];
  strcpy(newfmt, levelstr);
  strcat(newfmt, fmt);
  va_list args;
  va_start(args, fmt);
  nbdkit_vdebug(newfmt, args);
  va_end(args);
}

static int
plugin_config_complete(void)
{
  if (bucket == NULL) {
    nbdkit_error("missing parameter: bucket");
    return -1;
  }

  /* Create fake s3backer command line and parse it.
   * Casts are needed to drop const qualifier */
  char* argv[8];
  int i = 0;

  /* strdup() is needed because literals are const chars */
  argv[i++] = strdup("s3backer");
  if (region) {
    const char *p = "--region=";
    int len = strlen(p) + strlen(region);
    argv[i] = malloc(len+1);
    strcpy(argv[i], p);
    strcat(argv[i++], region);
  }
  if (conffile) {
    const char *p = "--configFile=";
    int len = strlen(p) + strlen(conffile);
    argv[i] = malloc(len+1);
    strcpy(argv[i], p);
    strcat(argv[i++], conffile);
  }
  argv[i++] = strdup(bucket);
  argv[i++] = strdup("/tmp"); // mountpoint, not used
  argv[i] = NULL;
  assert(i <= sizeof(argv));

  if ((config = s3backer_get_config(i, argv)) == NULL) {
    return 1;
  }
  config->log = nbdkit_logger;

  for (i = 0; argv[i] != NULL; i++)
    free(argv[i]);

  return 0;
}

#define plugin_config_help                                                     \
  "bucket=<bucket>     S3 bucket name (required).\n"                           \
  "region=<region>     S3 region name.\n"                                      \
  "conffile=<path>     Configuration file with further s3backer options."

static int
plugin_get_ready(void)
{
  if ((s3b = s3backer_create_store(config)) == NULL) {
    nbdkit_error("error creating s3backer_store: %m");
    return -11;
  }

  if ((fuse_ops = fuse_ops_create(&config->fuse_ops, s3b)) == NULL) {
    (*s3b->shutdown)(s3b);
    (*s3b->destroy)(s3b);
    return -1;
  }
  return 0;
}

static int
plugin_after_fork(void)
{
  struct fuse_conn_info ci;
  fuse_priv = fuse_ops->init(&ci);
  return 0;
}

static void
plugin_unload(void)
{
  fuse_ops->destroy(fuse_priv);
  free(bucket);
  free(region);
  free(conffile);
}

static void*
plugin_open(int readonly)
{
  (void)readonly;
  return NBDKIT_HANDLE_NOT_NEEDED;
}

/* Size of the data we are going to serve. */
static int64_t
plugin_get_size(void* handle)
{
  return fuse_priv->file_size;
}

static int
plugin_pread(void* handle,
             void* bufp,
             uint32_t size,
             uint64_t offset,
             uint32_t flags)
{
  char* buf = bufp;
  const uint64_t mask = config->block_size - 1;
  s3b_block_t block_num;
  size_t num_blocks;
  int r;

  /* Read first block fragment (if any) */
  if ((offset & mask) != 0) {
    size_t fragoff = (size_t)(offset & mask);
    size_t fraglen = (size_t)config->block_size - fragoff;

    if (fraglen > size)
      fraglen = size;
    block_num = offset >> fuse_priv->block_bits;
    if ((r = (*fuse_priv->s3b->read_block_part)(
           fuse_priv->s3b, block_num, fragoff, fraglen, buf)) != 0) {
      nbdkit_error("Error reading block %d: %m", block_num);
      nbdkit_set_error(r);
      return -1;
    }
    buf += fraglen;
    offset += fraglen;
    size -= fraglen;
  }

  /* Get block number and count */
  block_num = offset >> fuse_priv->block_bits;
  num_blocks = size >> fuse_priv->block_bits;

  /* Read intermediate complete blocks */
  while (num_blocks-- > 0) {
    if ((r = (*fuse_priv->s3b->read_block)(
           fuse_priv->s3b, block_num++, buf, NULL, NULL, 0)) != 0) {

      nbdkit_error("Error reading block %d: %m", block_num - 1);
      nbdkit_set_error(r);
      return -1;
    }
    buf += config->block_size;
  }

  /* Read last block fragment (if any) */
  if ((size & mask) != 0) {
    const size_t fraglen = size & mask;

    if ((r = (*fuse_priv->s3b->read_block_part)(
           fuse_priv->s3b, block_num, 0, fraglen, buf)) != 0) {
      nbdkit_error("Error reading block %d: %m", block_num);
      nbdkit_set_error(r);
      return -1;
    }
  }
  return 0;
}

static int
plugin_pwrite(void* handle,
              const void* bufp,
              uint32_t size,
              uint64_t offset,
              uint32_t flags)
{
  const char* buf = bufp;
  const uint64_t mask = config->block_size - 1;
  s3b_block_t block_num;
  size_t num_blocks;
  int r;

  /* Write first block fragment (if any) */
  if ((offset & mask) != 0) {
    size_t fragoff = (size_t)(offset & mask);
    size_t fraglen = (size_t)config->block_size - fragoff;

    if (fraglen > size)
      fraglen = size;
    block_num = offset >> fuse_priv->block_bits;
    if ((r = (*fuse_priv->s3b->write_block_part)(
           fuse_priv->s3b, block_num, fragoff, fraglen, buf)) != 0) {
      nbdkit_error("Error writing block %d: %m", block_num);
      nbdkit_set_error(r);
      return -1;
    }
    buf += fraglen;
    offset += fraglen;
    size -= fraglen;
  }

  /* Get block number and count */
  block_num = offset >> fuse_priv->block_bits;
  num_blocks = size >> fuse_priv->block_bits;

  /* Write intermediate complete blocks */
  while (num_blocks-- > 0) {
    if ((r = (*fuse_priv->s3b->write_block)(
           fuse_priv->s3b, block_num++, buf, NULL, NULL, NULL)) != 0) {
      nbdkit_error("Error writing block %d: %m", block_num-1);
      nbdkit_set_error(r);
      return -1;
    }
    buf += config->block_size;
  }

  /* Write last block fragment (if any) */
  if ((size & mask) != 0) {
    const size_t fraglen = size & mask;

    if ((r = (*fuse_priv->s3b->write_block_part)(
           fuse_priv->s3b, block_num, 0, fraglen, buf)) != 0) {
      nbdkit_error("Error writing block %d: %m", block_num);
      nbdkit_set_error(r);
      return -1;
    }
  }

  return 0;
}

static int
plugin_trim(void* handle, uint32_t size, uint64_t offset, uint32_t flags)
{
  const uint64_t mask = config->block_size - 1;
  s3b_block_t block_num;
  size_t num_blocks;
  int r;

  /* Write first block fragment (if any) */
  if ((offset & mask) != 0) {
    size_t fragoff = (size_t)(offset & mask);
    size_t fraglen = (size_t)config->block_size - fragoff;

    if (fraglen > size)
      fraglen = size;
    block_num = offset >> fuse_priv->block_bits;
    if ((r = (*fuse_priv->s3b->write_block_part)(
           fuse_priv->s3b, block_num, fragoff, fraglen, zero_block)) != 0) {
      nbdkit_error("Error trimming block %d: %m", block_num);
      nbdkit_set_error(r);
      return -1;
    }
    offset += fraglen;
    size -= fraglen;
  }

  /* Get block number and count */
  block_num = offset >> fuse_priv->block_bits;
  num_blocks = size >> fuse_priv->block_bits;

  /* Write intermediate complete blocks */
  while (num_blocks-- > 0) {
    if ((r = (*fuse_priv->s3b->write_block)(
           fuse_priv->s3b, block_num++, NULL, NULL, NULL, NULL)) != 0) {
      nbdkit_error("Error trimming block %d: %m", block_num - 1);
      nbdkit_set_error(r);
      return -1;
    }
  }

  /* Write last block fragment (if any) */
  if ((size & mask) != 0) {
    const size_t fraglen = size & mask;

    if ((r = (*fuse_priv->s3b->write_block_part)(
           fuse_priv->s3b, block_num, 0, fraglen, zero_block)) != 0) {
      nbdkit_error("Error trimming block %d: %m", block_num);
      nbdkit_set_error(r);
      return -1;
    }
  }

  return 0;
}

static int can_multi_conn(void* handle) {
  /* Since we have no per-connection state, the same client may
   * open multiple connections. */
  return 1;
}

static struct nbdkit_plugin plugin = {
  .name = "s3backer",
  .version = PACKAGE_VERSION,
  .unload = plugin_unload,
  .magic_config_key = "bucket",
  .config = plugin_config,
  .config_complete = plugin_config_complete,
  .config_help = plugin_config_help,
  .get_ready = plugin_get_ready,
  .after_fork = plugin_after_fork,
  .open = plugin_open,
  .get_size = plugin_get_size,
  .pread = plugin_pread,
  .pwrite = plugin_pwrite,
  .trim = plugin_trim,
  .can_multi_conn = can_multi_conn
};

NBDKIT_REGISTER_PLUGIN(plugin)
