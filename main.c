
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
 */

#include "s3backer.h"
#include "block_cache.h"
#include "ec_protect.h"
#include "fuse_ops.h"
#include "http_io.h"
#include "s3b_config.h"
#include "erase.h"
#include "reset.h"


/* Enable to debug app crashes */
#define COLLECT_STACK_TRACE         1

#if COLLECT_STACK_TRACE
#include <execinfo.h>
#include <signal.h>
struct s3b_config *crash_config;
void app_crash(int);
void app_crash(int sig) {
    void* callstack[128];
    int i, frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (i = 0; i < frames; ++i) {
        printf("s3backer crash: %s", strs[i]);
    }
    for (i = 0; i < frames; ++i) {
        fprintf(stderr, "s3backer crash: %s", strs[i]);
    }
    for (i = 0; i < frames; ++i) {
        (*crash_config->log)(LOG_INFO, "s3backer crash: %s", strs[i]);
    }
    free(strs);

    exit(1);
}
#endif

int
main(int argc, char **argv)
{
    const struct fuse_operations *fuse_ops;
    struct s3b_config *config;

    /* Get configuration */
    if ((config = s3backer_get_config(argc, argv)) == NULL)
        return 1;

#if COLLECT_STACK_TRACE
    crash_config = config;
    signal(SIGSEGV, app_crash);   // install handler
#endif


    /* Handle `--erase' flag */
    if (config->erase) {
        if (s3backer_erase(config) != 0)
            return 1;
        return 0;
    }

    /* Handle `--reset' flag */
    if (config->reset) {
        if (s3backer_reset(config) != 0)
            return 1;
        return 0;
    }

    /* Get FUSE operation hooks */
    fuse_ops = fuse_ops_create(&config->fuse_ops);

    /* Start */
    (*config->log)(LOG_INFO, "s3backer process %lu for %s started", (u_long)getpid(), config->mount);
    return fuse_main(config->fuse_args.argc, config->fuse_args.argv, fuse_ops, NULL);
}

