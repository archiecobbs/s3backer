
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

// Some definitions
#define NBD_CLIENT_BLOCK_SIZE                   4096
#define NBDKIT_STARTUP_WAIT_PAUSE               (long)50
#define MAX_NBDKIT_STARTUP_WAIT_MILLIS          (long)1000
#define NUM_CHILD_PROCESSES                     2

// Internal state
static const int forward_signals[] = { SIGHUP, SIGINT, SIGQUIT, SIGTERM };
static const int num_forward_signals = sizeof(forward_signals) / sizeof(*forward_signals);
static int child_pids[NUM_CHILD_PROCESSES];

// Internal functions
static int trampoline_to_nbd(int argc, char **argv);
static void kill_child_processes(int signal);
static void debug_exec(const char *executable, struct string_array *strings);
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
        if ((i = trampoline_to_nbd(argc, argv)) == 2) {
            usage();
            i = 1;
        }
        return i;
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
static int
trampoline_to_nbd(int argc, char **argv)
{
    struct string_array command_line;
    struct string_array nbd_flags;
    struct string_array nbd_params;
    struct s3b_config *config;
    struct sigaction act;
    struct timespec pause;
    char *bucket_param;
    char *device_param;
    char *unix_socket;
    long elapsed_millis;
    int file_created;
    struct stat sb;
    int wstatus;
    pid_t pid;
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
            return 2;
        }
        *value++ = '\0';
        if (strcmp(flag, "--nbd-flag") == 0)
            nbd_list = &nbd_flags;
        else if (strcmp(flag, "--nbd-param") == 0)
            nbd_list = &nbd_params;
        else {
            warnx("invalid flag \"%s\"", flag);
            return 2;
        }
        if (add_string(nbd_list, "%s", value) == -1)
            err(1, "add_string");
    }

    // There should be two remaining parameters
    switch (argc - i) {
    case 2:
        bucket_param = argv[i];
        device_param = argv[i + 1];
        break;
    default:
        return 2;
    }

    // Get info about /dev/nbd0 block device
    if (stat(device_param, &sb) == -1)
        errx(1, "%s", device_param);

    // Determine the UNIX socket file uniquely corresponding to the block device
    if (asprintf(&unix_socket, "%s/%0*jx_%0*jx", S3B_NBD_DIR,
      (int)(sizeof(dev_t) * 2), (uintmax_t)sb.st_dev, (int)(sizeof(ino_t) * 2), (uintmax_t)sb.st_ino) == -1)
        err(1, "asprintf");

    // Delete any leftover UNIX socket file, if any
    (void)unlink(unix_socket);

    // Verify we have sufficient privileges
    if (stat(unix_socket, &sb) == -1 && errno != ENOENT) {
        if (errno == EPERM || errno == EACCES)
            errx(1, "must be run as root when the \"--nbd\" flag is used");
        err(1, "%s", unix_socket);
    }

    // Get configuration (parse only)
    if ((config = s3backer_get_config(argc, argv, 1, 1)) == NULL)
        return 1;

    // Initialize nbdkit(1) command line
    if (add_string(&command_line, "%s", NBDKIT_EXECUTABLE) == -1
      || (config->debug && add_string(&command_line, "--verbose") == -1)
      || (config->foreground && add_string(&command_line, "--foreground") == -1)
      || (config->fuse_ops.read_only && add_string(&command_line, "--read-only") == -1)
      || add_string(&command_line, "--unix") == -1
      || add_string(&command_line, "%s", unix_socket) == -1)
        err(1, "add_string");

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
            return 2;
        }
        param += 2;

        // Get flag name and value (if any)
        if ((value = strchr(param, '=')) != NULL)
            *value++ = '\0';
        switch (is_valid_s3b_flag(param)) {
        case 1:
            if (value != NULL && strcasecmp(value, "true") != 0) {
                warnx("boolean flag \"--%s\" value must be \"true\"", param);
                return 2;
            }
            break;
        case 2:
            if (value == NULL) {
                warnx("flag \"--%s\" requires a value", param);
                return 2;
            }
            break;
        default:
            warnx("invalid flag \"--%s\"", param);
            return 2;
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

    // Fire up nbdkit
    if (config->debug)
        debug_exec(NBDKIT_EXECUTABLE, &command_line);
    if ((child_pids[0] = fork_off(NBDKIT_EXECUTABLE, command_line.strings)) == -1)
        err(1, "%s", NBDKIT_EXECUTABLE);
    free_strings(&command_line);

    // Wait for socket file to come into existence
    file_created = 0;
    for (elapsed_millis = 0; elapsed_millis <= MAX_NBDKIT_STARTUP_WAIT_MILLIS; elapsed_millis += NBDKIT_STARTUP_WAIT_PAUSE) {
        if (stat(unix_socket, &sb) == 0) {
            file_created = 1;
            break;
        }
        if (errno != ENOENT)
            err(1, "%s", unix_socket);
        pause.tv_sec = 0;
        pause.tv_nsec = NBDKIT_STARTUP_WAIT_PAUSE * (long)1000000;
        (void)nanosleep(&pause, NULL);
    }
    if (!file_created)
        errx(1, "%s failed to start within %lums", NBDKIT_EXECUTABLE, MAX_NBDKIT_STARTUP_WAIT_MILLIS);

    // Build nbd-client command line
    if (add_string(&command_line, "%s", NBD_CLIENT_EXECUTABLE) == -1
      || add_string(&command_line, "-unix") == -1
      || add_string(&command_line, "%s", unix_socket) == -1
      || add_string(&command_line, "-block-size") == -1
      || add_string(&command_line, "%u", NBD_CLIENT_BLOCK_SIZE) == -1
      || (config->foreground && add_string(&command_line, "-nofork") == -1)
      || add_string(&command_line, "%s", device_param) == -1)
        err(1, "add_string");

    // Fire up nbd-client
    if (config->debug)
        debug_exec(NBD_CLIENT_EXECUTABLE, &command_line);
    if ((child_pids[1] = fork_off(NBD_CLIENT_EXECUTABLE, command_line.strings)) == -1)
        err(1, "%s", NBD_CLIENT_EXECUTABLE);

    // Setup so if we get a death signal, we terminate our child processes (via SIGTERM)
    memset(&act, 0, sizeof(act));
    act.sa_handler = &kill_child_processes;
    act.sa_flags = SA_RESTART;
    for (i = 0; i < num_forward_signals; i++) {
        if (sigaction(forward_signals[i], &act, NULL) == -1)
            err(1, "sigaction");
    }

    // Wait for the first child process to exit
    while (1) {
        if ((pid = wait(&wstatus)) == -1) {
            if (errno == EINTR)             // interrupted by signal, try again
                continue;
            err(1, "waitpid");
        }
        assert(WIFEXITED(wstatus) || WIFSIGNALED(wstatus));
        break;
    }

    // Kill all others
    kill_child_processes((int)-pid);

    // Wait for all remaining child process(es) to exit
    while (1) {
        if ((pid = wait(&wstatus)) == -1) {
            if (errno == ECHILD)
                break;
            if (errno == EINTR)             // interrupted by signal, try again
                continue;
            err(1, "waitpid");
        }
        assert(WIFEXITED(wstatus) || WIFSIGNALED(wstatus));
    }

    // Delete UNIX socket file
    (void)unlink(unix_socket);

    // Done
    return 0;
}

// If signal is negative, then skip that PID
static void
kill_child_processes(int signal)
{
    int i;

    for (i = 0; i < NUM_CHILD_PROCESSES; i++) {
        if (signal < 0 && signal == (int)-child_pids[i])
            continue;
        (void)kill(child_pids[i], SIGTERM);
    }
}

static void
debug_exec(const char *executable, struct string_array *params)
{
    char **const argv = params->strings;
    int i;

    warnx("executing %s with these parameters:", executable);
    for (i = 0; argv[i] != NULL; i++)
        warnx("  [%02d] \"%s\"", i, argv[i]);
}
#endif
