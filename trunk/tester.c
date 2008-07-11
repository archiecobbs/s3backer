
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

#include "s3backer.h"

/* Definitions */
#define NUM_THREADS     10
#define NUM_LOCKS       25
#define DELAY_BASE      500
#define DELAY_RANGE     150
#define READ_FACTOR     2
#define ZERO_FACTOR     3

/* Internal functions */
static void *thread_main(void *arg);

/* Internal variables */
static u_char *md5s;
static void *zero_block;
static u_char zero_md5[MD5_DIGEST_LENGTH];
static pthread_mutex_t locks[NUM_LOCKS];
static struct s3backer_conf *config;
static struct s3backer_store *store;

int
main(int argc, char **argv)
{
    pthread_t thread;
    MD5_CTX ctx;
    int i;
    int r;

    /* Get configuration */
    if ((config = s3backer_get_config(argc, argv)) == NULL)
        exit(1);

    /* Open store */
    if ((store = s3backer_create(config)) == NULL)
        err(1, "s3backer_create");

    /* Compute MD5 of zero block */
    if ((zero_block = calloc(1, config->block_size)) == NULL)
        err(1, "calloc");
    MD5_Init(&ctx);
    MD5_Update(&ctx, zero_block, config->block_size);
    MD5_Final(zero_md5, &ctx);
    fprintf(stderr, "ZERO: %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n",
      zero_md5[0], zero_md5[1], zero_md5[2], zero_md5[3], zero_md5[4], zero_md5[5], zero_md5[6], zero_md5[7],
      zero_md5[8], zero_md5[9], zero_md5[10], zero_md5[11], zero_md5[12], zero_md5[13], zero_md5[14], zero_md5[15]);

    /* Random initialization */
    srandom((u_int)time(NULL));
    for (i = 0; i < NUM_LOCKS; i++)
        pthread_mutex_init(&locks[i], NULL);
    if ((md5s = malloc(config->num_blocks * MD5_DIGEST_LENGTH)) == NULL)
        err(1, "malloc");
    for (i = 0; i < config->num_blocks; i++)
        memcpy(&md5s[i * MD5_DIGEST_LENGTH], zero_md5, MD5_DIGEST_LENGTH);

    /* Zero all blocks */
    for (i = 0; i < config->num_blocks; i++) {
	fprintf(stderr, "zeroing block #%u\n", i);
	if ((r = (*store->write_block)(store, i, zero_block)) != 0)
	    warnx("write error: %s", strerror(r));
    }

    /* Create threads */
    for (i = 0; i < NUM_THREADS; i++)
        pthread_create(&thread, NULL, thread_main, (void *)i);

    /* Run for a day */
    sleep(24 * 60 * 60);
    return 0;
}

static void *
thread_main(void *arg)
{
    const int id = (int)arg;
    struct timespec delay;
    u_char data[config->block_size];
    u_char md5[MD5_DIGEST_LENGTH];
    s3b_block_t block_num;
    MD5_CTX ctx;
    int millis;
    int lockid;
    int r;

    /* Loop */
    while (1) {

        // Sleep
        millis = DELAY_BASE + (random() % DELAY_RANGE);
        delay.tv_sec = millis / 1000;
        delay.tv_nsec = (millis % 1000) * 1000000;
        nanosleep(&delay, NULL);

        // Pick a random block
        block_num = random() % config->num_blocks;
        lockid = block_num % NUM_LOCKS;

        // Randomly read or write
        pthread_mutex_lock(&locks[lockid]);
        if ((random() % READ_FACTOR) != 0) {
            fprintf(stderr, "[%2d] rd #%u START\n", id, block_num);
            if ((r = (*store->read_block)(store, block_num, data)) != 0) {
                warnx("[%2d] read error: %s", id, strerror(r));
                continue;
            }
            MD5_Init(&ctx);
            MD5_Update(&ctx, data, config->block_size);
            MD5_Final(md5, &ctx);
            if (memcmp(md5, &md5s[block_num * MD5_DIGEST_LENGTH], MD5_DIGEST_LENGTH) != 0) {
                const u_char *g = md5;
                const u_char *e = &md5s[block_num * MD5_DIGEST_LENGTH];

                fprintf(stderr, "[%2d] EXP: %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n",
                  id, e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7], e[8], e[9], e[10], e[11], e[12], e[13], e[14], e[15]);
                fprintf(stderr, "[%2d] GOT: %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n",
                  id, g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7], g[8], g[9], g[10], g[11], g[12], g[13], g[14], g[15]);
                errx(1, "got wrong MD5 block #%u", block_num);
            }
            fprintf(stderr, "[%2d] rd #%u COMPLETE\n", id, block_num);
        } else {
            *((u_int *)data) = random() % ZERO_FACTOR != 0 ? 0 : (u_int)random();
            fprintf(stderr, "[%2d] wr #%u 0x%08x START\n", id, block_num, *(u_int *)data);
            MD5_Init(&ctx);
            MD5_Update(&ctx, data, config->block_size);
            MD5_Final(md5, &ctx);
            if ((r = (*store->write_block)(store, block_num, data)) != 0)
                warnx("[%2d] write error: %s", id, strerror(r));
            else
                memcpy(&md5s[block_num * MD5_DIGEST_LENGTH], md5, MD5_DIGEST_LENGTH);
            fprintf(stderr, "[%2d] wr #%u 0x%08x %s%s\n", id, block_num, *(u_int *)data,
              r != 0 ? "FAILED" : "COMPLETE", r != 0 ? strerror(r) : "");
        }
        pthread_mutex_unlock(&locks[lockid]);
    }
}

