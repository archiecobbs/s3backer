
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
#include "block_cache.h"
#include "ec_protect.h"
#include "zero_cache.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"
#include "erase.h"

#define BLOCKS_PER_DOT          0x100
#define MAX_QUEUE_LENGTH        1000
#define NUM_ERASURE_THREADS     25

/* Erasure state */
struct erase_state {
    struct s3backer_store       *s3b;
    s3b_block_t                 queue[MAX_QUEUE_LENGTH];
    u_int                       qlen;
    pthread_t                   threads[NUM_ERASURE_THREADS];
    int                         quiet;
    int                         stopping;
    uintmax_t                   count;
    pthread_mutex_t             mutex;
    pthread_cond_t              thread_wakeup;
    pthread_cond_t              queue_not_full;
};

/* Internal functions */
static block_list_func_t erase_list_callback;
static void *erase_thread_main(void *arg);

int
s3backer_erase(struct s3b_config *config)
{
    struct erase_state state;
    struct erase_state *const priv = &state;
    char response[10];
    int num_threads;
    int ok = 0;
    int r;

    /* Double check with user */
    if (!config->force) {
        warnx("`--erase' flag given: erasing all blocks in %s", config->description);
        fprintf(stderr, "s3backer: is this correct? [y/N] ");
        *response = '\0';
        if (fgets(response, sizeof(response), stdin) != NULL) {
            while (*response && isspace(response[strlen(response) - 1]))
                response[strlen(response) - 1] = '\0';
        }
        if (strcasecmp(response, "y") != 0 && strcasecmp(response, "yes") != 0) {
            warnx("not confirmed");
            goto fail0;
        }
    }

    /* Initialize state */
    memset(priv, 0, sizeof(*priv));
    priv->quiet = config->quiet;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0) {
        warnx("pthread_mutex_init: %s", strerror(r));
        goto fail0;
    }
    if ((r = pthread_cond_init(&priv->thread_wakeup, NULL)) != 0) {
        warnx("pthread_cond_init: %s", strerror(r));
        goto fail1;
    }
    if ((r = pthread_cond_init(&priv->queue_not_full, NULL)) != 0) {
        warnx("pthread_cond_init: %s", strerror(r));
        goto fail2;
    }
    for (num_threads = 0; num_threads < NUM_ERASURE_THREADS; num_threads++) {
        if ((r = pthread_create(&priv->threads[num_threads], NULL, erase_thread_main, priv)) != 0)
            goto fail3;
    }

    /* Logging */
    if (!config->quiet) {
        fprintf(stderr, "s3backer: erasing non-zero blocks...");
        fflush(stderr);
    }

    /* Create temporary lower layer */
    if ((priv->s3b = config->test ? test_io_create(&config->test_io) : http_io_create(&config->http_io)) == NULL) {
        warnx(config->test ? "test_io_create" : "http_io_create");
        goto fail3;
    }

    /* Iterate over non-zero blocks */
    if ((r = (*(config->test ? test_io_list_blocks : http_io_list_blocks))(priv->s3b, erase_list_callback, priv)) != 0) {
        warnx("can't list blocks: %s", strerror(r));
        goto fail3;
    }

    /* Clear mount token */
    if ((r = (*priv->s3b->set_mount_token)(priv->s3b, NULL, 0)) != 0) {
        warnx("can't clear mount token: %s", strerror(r));
        goto fail3;
    }

    /* Success */
    ok = 1;

    /* Clean up */
fail3:
    pthread_mutex_lock(&priv->mutex);
    priv->stopping = 1;
    pthread_cond_broadcast(&priv->thread_wakeup);
    pthread_mutex_unlock(&priv->mutex);
    while (num_threads > 0) {
        if ((r = pthread_join(priv->threads[--num_threads], NULL)) != 0)
            warnx("pthread_join: %s", strerror(r));
    }
    if (priv->s3b != NULL) {
        if (ok && !config->quiet) {
            fprintf(stderr, "done\n");
            warnx("erased %ju non-zero blocks", priv->count);
        }
        (*priv->s3b->shutdown)(priv->s3b);
        (*priv->s3b->destroy)(priv->s3b);
    }
    pthread_cond_destroy(&priv->queue_not_full);
fail2:
    pthread_cond_destroy(&priv->thread_wakeup);
fail1:
    pthread_mutex_destroy(&priv->mutex);
fail0:
    return ok ? 0 : -1;
}

static int
erase_list_callback(void *arg, const s3b_block_t *block_nums, u_int num_blocks)
{
    struct erase_state *const priv = arg;

    pthread_mutex_lock(&priv->mutex);
    while (num_blocks-- > 0) {
        while (priv->qlen == MAX_QUEUE_LENGTH)
            pthread_cond_wait(&priv->queue_not_full, &priv->mutex);
        priv->queue[priv->qlen++] = *block_nums++;
    }
    pthread_cond_signal(&priv->thread_wakeup);
    pthread_mutex_unlock(&priv->mutex);
    return 0;
}

static void *
erase_thread_main(void *arg)
{
    struct erase_state *const priv = arg;
    s3b_block_t block_num;
    int r;

    /* Acquire lock */
    pthread_mutex_lock(&priv->mutex);

    /* Erase blocks until there are no more */
    while (1) {

        /* Is there a block to erase? */
        if (priv->qlen > 0) {

            /* Grab next bock */
            if (priv->qlen == MAX_QUEUE_LENGTH)
                pthread_cond_signal(&priv->queue_not_full);
            block_num = priv->queue[--priv->qlen];

            /* Do block deletion */
            pthread_mutex_unlock(&priv->mutex);
            r = (*priv->s3b->write_block)(priv->s3b, block_num, NULL, NULL, NULL, NULL);
            pthread_mutex_lock(&priv->mutex);

            /* Check for error */
            if (r != 0) {
                warnx("can't delete block %0*jx: %s", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, strerror(r));
                continue;
            }

            /* Update count and output a dot */
            if ((++priv->count % BLOCKS_PER_DOT) == 0 && !priv->quiet) {
                fprintf(stderr, ".");
                fflush(stderr);
            }

            /* Spin again */
            continue;
        }

        /* Are we done? */
        if (priv->stopping)
            break;

        /* Wait for something to do */
        pthread_cond_wait(&priv->thread_wakeup, &priv->mutex);
    }

    /* Done */
    pthread_mutex_unlock(&priv->mutex);
    return NULL;
}

