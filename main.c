
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
#include "reset.h"
#include "util.h"
#include "nbdkit.h"

#if NBDKIT
static void trampoline_to_nbd(int argc, char **argv);
#endif

int
main(int argc, char **argv)
{
    const struct fuse_operations *fuse_ops;
    struct s3backer_store *s3b;
    struct s3b_config *config;
    int nbd = 0;
    int i;

    // Look for "--nbd" flag
    for (i = 1; i < argc; i++) {
        const char *param = argv[i];
        if (*param != '-' || strcmp(param, "--") == 0)
            break;
        if (strcmp(param, "--nbd") == 0) {
            nbd = 1;
            break;
        }
    }

    // Handle `--nbd' flag
    if (nbd) {
#if NBDKIT
        trampoline_to_nbd(argc, argv);
        return 1;                                           // we should never get here; something went wrong
#else
        errx(1, "invalid flag \"--nbd\": NBDKit not installed");
#endif
    }

    // Get configuration
    if ((config = s3backer_get_config(argc, argv, 0, 0)) == NULL)
        return 1;
    if (config->nbd)
        errx(1, "the \"--nbd\" flag is not supported in config files (must be on the command line)");

    // Handle `--erase' flag
    if (config->erase) {
        if (s3backer_erase(config) != 0)
            return 1;
        return 0;
    }

    // Handle `--reset' flag
    if (config->reset) {
        if (s3backer_reset(config) != 0)
            return 1;
        return 0;
    }

    // Create backing store
    if ((s3b = s3backer_create_store(config)) == NULL)
        err(1, "error creating s3backer_store");

    // Start logging to syslog now
    if (!config->foreground)
        config->log = syslog_logger;

    // Setup FUSE operation hooks
    if ((fuse_ops = fuse_ops_create(&config->fuse_ops, s3b)) == NULL) {
        (*s3b->shutdown)(s3b);
        (*s3b->destroy)(s3b);
        return 1;
    }

    // Start
    (*config->log)(LOG_INFO, "s3backer process %lu for %s started", (u_long)getpid(), config->mount);
    if (fuse_main(config->fuse_args.argc, config->fuse_args.argv, fuse_ops, NULL) != 0) {
        (*config->log)(LOG_ERR, "error starting FUSE");
        fuse_ops_destroy();
        return 1;
    }

    // Done
    return 0;
}

#if NBDKIT
static void
trampoline_to_nbd(int argc, char **argv)
{
    struct string_array command_line;
    struct string_array nbd_flags;
    struct string_array nbd_params;
    struct s3b_config *config;
    char *bucket_param;
    char *address_param;
    int i;

    // Initialize
    memset(&command_line, 0, sizeof(command_line));
    memset(&nbd_flags, 0, sizeof(nbd_flags));
    memset(&nbd_params, 0, sizeof(nbd_params));

    // Find and extract any "--nbd", "--nbd-flag", and "--nbd-param" flags
    for (i = 1; i < argc; i++) {
        struct string_array *nbd_list;
        char *flag = argv[i];
        char *value;
        if (*flag != '-')
            break;
        if (strcmp(flag, "--") == 0) {
            i++;
            break;
        }
        if (strncmp(flag, "--nbd", 5) != 0)
            continue;
        memmove(argv + i, argv + i + 1, (--argc - i) * sizeof(*argv));          // squish it
        i--;
        if (strcmp(flag, "--nbd") == 0)                                         // the "--nbd" flag that got us here
            continue;
        if ((value = strchr(flag, '=')) == NULL) {
            warnx("invalid flag \"%s\"", flag);
            usage();
            return;
        }
        *value++ = '\0';
        if (strcmp(flag, "--nbd-flag") == 0)
            nbd_list = &nbd_flags;
        else if (strcmp(flag, "--nbd-param") == 0)
            nbd_list = &nbd_params;
        else {
            warnx("invalid flag \"%s\"", flag);
            usage();
            return;
        }
        if (add_string(nbd_list, "%s", value) == -1)
            err(1, "add_string");
    }

    // There should be either one or two remaining parameters
    switch (argc - i) {
    case 1:
        bucket_param = argv[i];
        address_param = NULL;
        break;
    case 2:
        bucket_param = argv[i];
        address_param = argv[i + 1];
        break;
    default:
        usage();
        return;
    }

    // Get configuration (parse only)
    if ((config = s3backer_get_config(argc, argv, 1, 1)) == NULL)
        return;

    // Initialize nbdkit(1) command line
    if (add_string(&command_line, "%s", NBDKIT_EXECUTABLE) == -1)
        err(1, "add_string");
    if (config->debug && add_string(&command_line, "--verbose") == -1)
        err(1, "add_string");
    if (config->foreground && add_string(&command_line, "--foreground") == -1)
        err(1, "add_string");
    if (config->fuse_ops.read_only && add_string(&command_line, "--read-only") == -1)
        err(1, "add_string");

    // Was an address specified?
    if (address_param != NULL) {
        int unix_socket;
        const char *s;

        // Did we get a UNIX socket file or IP address and port?
        unix_socket = 0;
        for (s = address_param; *s != '\0'; s++) {
            if (strchr(":.0123456789", *s) == NULL) {
                unix_socket = 1;
                break;
            }
        }

        // Add address, either UNIX socket or ipaddr:port
        if (unix_socket) {
            if (add_string(&command_line, "--unix") == -1
              || add_string(&command_line, "%s", address_param) == -1)
                err(1, "add_string");
        } else {
            char *ipaddr;
            char *port;

            // Split IP address and port as needed
            if ((port = strchr(address_param, ':')) != NULL) {
                *port++ = '\0';
                ipaddr = address_param;
            } else {
                port = address_param;
                ipaddr = NULL;
            }

            // Add (optional) IP address and port parameters
            if (ipaddr != NULL) {
                if (add_string(&command_line, "--ipaddr") == -1
                  || add_string(&command_line, "%s", ipaddr) == -1)
                    err(1, "add_string");
            }
            if (add_string(&command_line, "--port") == -1
              || add_string(&command_line, "%s", port) == -1)
                err(1, "add_string");
        }
    }

    // Add any custom "--nbd-flag" flags
    for (i = 0; i < nbd_flags.num_strings; i++) {
        if (add_string(&command_line, "%s", nbd_flags.strings[i]) == -1)
            err(1, "add_string");
    }

    // Add plugin name
    if (add_string(&command_line, "%s", PACKAGE) == -1)
        err(1, "add_string");

    // Add s3backer plugin parameters, converting "--foo bar" to "s3b_foo=bar" and "--foo" to "s3b_foo=true"
    for (i = 1; i < argc; i++) {
        char *param = argv[i];
        char *value;

        // Detect when we've seen the last flag
        if (*param != '-' || strcmp(param, "--") == 0)
            break;

        // Skip flags we've already handled
        if (strcmp(param, "-f") == 0 || strcmp(param, "-d") == 0)
            continue;

        // Only accept --doubleDashFlags from here on out
        if (param[1] != '-') {
            warnx("invalid flag \"%s\"", param);
            usage();
            return;
        }
        param += 2;

        // Get flag name and value (if any)
        if ((value = strchr(param, '=')) != NULL)
            *value++ = '\0';
        switch (is_valid_s3b_flag(param)) {
        case 1:
            if (value != NULL && strcasecmp(value, "true") != 0) {
                warnx("boolean flag \"--%s\" value must be \"true\"", param);
                usage();
                return;
            }
            break;
        case 2:
            if (value == NULL) {
                warnx("flag \"--%s\" requires a value", param);
                usage();
                return;
            }
            break;
        default:
            warnx("invalid flag \"--%s\"", param);
            usage();
            return;
        }

        // Add corresponding nbdkit parameter
        if (add_string(&command_line, "%s%s=%s", NBD_S3B_PARAM_PREFIX, param, value != NULL ? value : "true") == -1)
            err(1, "add_string");
    }

    // Add bucket[/subdir] param
    if (add_string(&command_line, "%s=%s", NBD_BUCKET_PARAMETER_NAME, bucket_param) == -1)
        err(1, "add_string");

    // Add any custom "--nbd-param" params
    for (i = 0; i < nbd_params.num_strings; i++) {
        if (add_string(&command_line, "%s", nbd_params.strings[i]) == -1)
            err(1, "add_string");
    }

    // Debug
    if (config->debug) {
        warnx("executing %s with these parameters:", NBDKIT_EXECUTABLE);
        for (i = 0; i < command_line.num_strings; i++)
            warnx("  [%02d] \"%s\"", i, command_line.strings[i]);
    }

    // Invoke nbkdit
    if (execve(NBDKIT_EXECUTABLE, command_line.strings, environ) == -1)
        err(1, "nbdkit");
}
#endif
