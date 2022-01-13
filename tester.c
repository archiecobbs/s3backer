
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
#include "util.h"

/* Definitions */
#define NUM_THREADS     10
#define DELAY_BASE      0
#define DELAY_RANGE     50
#define READ_FACTOR     2
#define ZERO_FACTOR     3

/* Block states */
struct block_state {
    u_int               writing;        // block is currently being written by a thread
    u_int               counter;        // counts writes to the block
    u_int               content;        // most recently written content
};

/* Internal functions */
static void *thread_main(void *arg);
static void logit(int id, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
static uint64_t get_time(void);

/* Internal variables */
static pthread_mutex_t mutex;
static struct s3b_config *config;
static struct s3backer_store *store;
static struct block_state *blocks;
static uint64_t start_time;

int
main(int argc, char **argv)
{
    s3b_block_t block_num;
    pthread_t thread;
    int i;
    int r;

    /* Get configuration */
    if ((config = s3backer_get_config(argc, argv)) == NULL)
        exit(1);
    if (config->block_size < sizeof(u_int))
        err(1, "block size too small");

    /* Open store */
    if ((store = s3backer_create_store(config)) == NULL)
        err(1, "s3backer_create_store");

    /* Allocate block states */
    if ((blocks = calloc(config->num_blocks, sizeof(*blocks))) == NULL)
        err(1, "calloc");

    /* Random initialization */
    srandom((u_int)time(NULL));
    
    if(pthread_mutex_init(&mutex, NULL)!=0)
        err(1, "lock initialization");
    
    start_time = get_time();

    /* Zero all blocks */
    for (block_num = 0; block_num < config->num_blocks; block_num++) {
        printf("zeroing block %0*jx\n", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
        if ((r = (*store->write_block)(store, block_num, zero_block, NULL, NULL, NULL)) != 0)
            err(1, "write error");
    }

    /* Create threads */
    for (i = 0; i < NUM_THREADS; i++)
        pthread_create(&thread, NULL, thread_main, (void *)(intptr_t)i);

    /* Run for a day */
    sleep(24 * 60 * 60);
    return 0;
}

static void *
thread_main(void *arg)
{
    const int id = (int)(intptr_t)arg;
    u_char data[config->block_size];
    s3b_block_t block_num;
    int millis;
    int r;

    /* Loop */
    while (1) {

        // Sleep
        millis = DELAY_BASE + (random() % DELAY_RANGE);
        usleep(millis * 1000);

        // Pick a random block
        block_num = random() % config->num_blocks;

        // Randomly read or write it
        if ((random() % READ_FACTOR) != 0) {
            struct block_state *const state = &blocks[block_num];
            struct block_state before;
            struct block_state after;

            // Snapshot block state
            pthread_mutex_lock(&mutex);
            memcpy(&before, state, sizeof(before));
            CHECK_RETURN(pthread_mutex_unlock(&mutex));

            // Do the read
            logit(id, "rd %0*jx START\n", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
            if ((r = (*store->read_block)(store, block_num, data, NULL, NULL, 0)) != 0) {
                logit(id, "****** READ ERROR: %s", strerror(r));
                continue;
            }

            // Snapshot block state again
            pthread_mutex_lock(&mutex);
            memcpy(&after, state, sizeof(before));
            CHECK_RETURN(pthread_mutex_unlock(&mutex));

            // Verify content, but only if no write occurred while we were reading
            if (before.writing == 0 && after.writing == 0 && before.counter == after.counter) {
                if (memcmp(data, &before.content, sizeof(before.content)) != 0) {
                    logit(id, "got wrong content block %0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
                    exit(1);
                }
            }
            logit(id, "rd %0*jx content=0x%02x%02x%02x%02x COMPLETE\n", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
              data[0], data[1], data[2], data[3]);
        } else {
            struct block_state *const state = &blocks[block_num];
            u_int content;

            // Update block state
            pthread_mutex_lock(&mutex);
            if (state->writing) {                   // only one writer at a time
                CHECK_RETURN(pthread_mutex_unlock(&mutex));
                continue;
            }
            state->writing = 1;
            CHECK_RETURN(pthread_mutex_unlock(&mutex));

            // Write block
            content = (random() % ZERO_FACTOR) != 0 ? 0 : (u_int)random();
            memcpy(data, &content, sizeof(content));
            memset(data + sizeof(content), 0, config->block_size - sizeof(content));
            logit(id, "wr %0*jx content=0x%02x%02x%02x%02x START\n", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
              data[0], data[1], data[2], data[3]);
            if ((r = (*store->write_block)(store, block_num, data, NULL, NULL, NULL)) != 0)
                logit(id, "****** WRITE ERROR: %s", strerror(r));
            logit(id, "wr %0*jx content=0x%02x%02x%02x%02x %s%s\n", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
              data[0], data[1], data[2], data[3], r != 0 ? "FAILED: " : "COMPLETE", r != 0 ? strerror(r) : "");

            // Update block state
            pthread_mutex_lock(&mutex);
            if (r == 0) {
                state->counter++;
                state->content = content;
            }
            state->writing = 0;
            CHECK_RETURN(pthread_mutex_unlock(&mutex));
        }
    }
}

static void
logit(int id, const char *fmt, ...)
{
    uint64_t timestamp = get_time() - start_time;
    va_list args;

    printf("%u.%03u [%02d] ", (u_int)(timestamp / 1000), (u_int)(timestamp % 1000), id);
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);
}


static uint64_t
get_time(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

