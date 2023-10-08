
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
#include "block_cache.h"
#include "ec_protect.h"
#include "zero_cache.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "test_io.h"
#include "s3b_config.h"
#include "util.h"

// Definitions
#define NUM_THREADS     10
#define DELAY_BASE      0
#define DELAY_RANGE     50
#define READ_FACTOR     2
#define ZERO_FACTOR     3

// Block states
struct block_state {
    u_int               writing;        // block is currently being written by a thread
    u_int               counter;        // counts writes to the block
    u_int               content;        // most recently written content
};

// Internal functions
static void *test_thread_main(void *arg);
static void logit(int id, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
static void catch_signal(int sig);
static uint64_t get_time(void);

// Internal variables
static pthread_mutex_t mutex;
static pthread_mutex_t log_mutex;
static struct s3b_config *config;
static struct s3backer_store *store;
static struct block_state *blocks;
static uint64_t start_time;
static volatile int stop_threads;

int
main(int argc, char **argv)
{
    s3b_block_t block_num;
    pthread_t threads[NUM_THREADS];
    sigset_t sigs;
    int sig;
    int i;
    int r;

    // Get configuration
    if ((config = s3backer_get_config(argc, argv, 0, 0)) == NULL)
        exit(1);
    if (config->block_size < sizeof(u_int))
        err(1, "block size too small");

    // Open store
    logit(-1, "creating s3backer store");
    if ((store = s3backer_create_store(config)) == NULL)
        err(1, "s3backer_create_store");

    // Startup background threads
    logit(-1, "starting background threads");
    if ((r = (*store->create_threads)(store)) != 0)
        err(1, "create_threads");

    // Allocate block states
    if ((blocks = calloc(config->num_blocks, sizeof(*blocks))) == NULL)
        err(1, "calloc");

    // Random initialization
    srandom((u_int)time(NULL));
    if (pthread_mutex_init(&mutex, NULL) != 0)
        err(1, "mutex init");
    if (pthread_mutex_init(&log_mutex, NULL) != 0)
        err(1, "mutex init");
    start_time = get_time();

    // Zero all blocks
    logit(-1, "started zeroing all blocks");
    for (block_num = 0; block_num < config->num_blocks; block_num++) {
        if ((r = (*store->write_block)(store, block_num, zero_block, NULL, NULL, NULL)) != 0)
            err(1, "write error");
        if (block_num % 1000 == 999)
            logit(-1, "zeroed %jd blocks", (uintmax_t)block_num);
    }
    logit(-1, "finished zeroing all blocks");

    // Create my threads
    logit(-1, "starting tester threads");
    for (i = 0; i < NUM_THREADS; i++)
        pthread_create(&threads[i], NULL, test_thread_main, (void *)(intptr_t)i);

    // Wait for signal
    logit(-1, "waiting for termination signal");
    signal(SIGHUP, catch_signal);
    signal(SIGINT, catch_signal);
    signal(SIGTERM, catch_signal);
    signal(SIGQUIT, catch_signal);
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGHUP);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    sigaddset(&sigs, SIGQUIT);
    if ((r = sigwait(&sigs, &sig)) != 0)
        err(1, "sigwait");
    logit(-1, "got termination signal");

    // Stop threads and wait for them to exit
    logit(-1, "stopping tester threads");
    stop_threads = 1;
    for (i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);

    // Done
    logit(-1, "done");
    return 0;
}

static void *
test_thread_main(void *arg)
{
    const int id = (int)(intptr_t)arg;
    u_char data[config->block_size];
    s3b_block_t block_num;
    int millis;
    int r;

    // Loop
    while (!stop_threads) {

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
            logit(id, "rd %0*jx START", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
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
            logit(id, "rd %0*jx content=0x%02x%02x%02x%02x COMPLETE", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
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
            logit(id, "wr %0*jx content=0x%02x%02x%02x%02x START", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
              data[0], data[1], data[2], data[3]);
            if ((r = (*store->write_block)(store, block_num, data, NULL, NULL, NULL)) != 0)
                logit(id, "****** WRITE ERROR: %s", strerror(r));
            logit(id, "wr %0*jx content=0x%02x%02x%02x%02x %s%s", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num,
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

    // Done
    return NULL;
}

static void
logit(int id, const char *fmt, ...)
{
    uint64_t timestamp = get_time() - start_time;
    va_list args;

    pthread_mutex_lock(&log_mutex);
    if (id == -1)
        printf("%u.%03u [--] ", (u_int)(timestamp / 1000), (u_int)(timestamp % 1000));
    else
        printf("%u.%03u [%02d] ", (u_int)(timestamp / 1000), (u_int)(timestamp % 1000), id);
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    printf("\n");
    fflush(stdout);
    va_end(args);
    CHECK_RETURN(pthread_mutex_unlock(&log_mutex));
}

static uint64_t
get_time(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

static void
catch_signal(int sig)
{
    // do nothing
}
