
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

// Forward decl's
struct s3b_config;

// Bitmap type
typedef uintptr_t bitmap_t;

// A list of block numbers
struct block_list {
    s3b_block_t     *blocks;
    s3b_block_t     num_alloc;
    s3b_block_t     num_blocks;
};

// Block boundary condition handling info
struct boundary_edge {
    char            *data;
    s3b_block_t     block;
    u_int           offset;
    u_int           length;
};
struct boundary_info {

    // Header portion
    struct boundary_edge    header;

    // Center block-aligned portion
    char                    *mid_data;
    s3b_block_t             mid_block_start;
    size_t                  mid_block_count;

    // Footer portion
    struct boundary_edge    footer;
};

// A list of strings
struct string_array {
    char            **strings;
    size_t          num_alloc;
    size_t          num_strings;
};

// A child process
struct child_proc {
    const char  *name;
    pid_t       pid;
    int         wstatus;
};

// Globals
extern int log_enable_debug;
extern int daemonized;
extern const void *zero_block;

// Misc
extern int parse_size_string(const char *s, const char *description, u_int max_bytes, uintmax_t *valp);
extern void unparse_size_string(char *buf, int bmax, uintmax_t value);
extern void describe_size(char *buf, int bmax, uintmax_t value);
extern void syslog_logger(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
extern void stderr_logger(int level, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
extern int find_string_in_table(const char *const *table, const char *value);
extern int block_is_zeros(const void *data);
extern int snvprintf(char *buf, int bufsize, const char *format, ...) __attribute__ ((__format__ (__printf__, 3, 4)));
extern char *prefix_log_format(int level, const char *fmt);
extern void calculate_boundary_info(struct boundary_info *info, u_int block_size, const void *buf, size_t size, off_t offset);
extern int fsync_path(const char *path, int must_exist);
extern int add_string(struct string_array *array, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 2, 3)));
extern void free_strings(struct string_array *array);
extern int init_zero_block(u_int block_size);
extern void set_config_log(struct s3b_config *config, log_func_t *log);

// Versions of <err.h> that work properly even when daemonized
extern void daemon_debug(const struct s3b_config *config, const char *fmt, ...)
    __attribute__ ((__format__ (__printf__, 2, 3)));
extern void daemon_warn(const struct s3b_config *config, const char *fmt, ...)
    __attribute__ ((__format__ (__printf__, 2, 3)));
extern void daemon_warnx(const struct s3b_config *config, const char *fmt, ...)
    __attribute__ ((__format__ (__printf__, 2, 3)));
extern void daemon_err(const struct s3b_config *config, int value, const char *fmt, ...)
    __attribute__ ((__noreturn__, __format__ (__printf__, 3, 4)));
extern void daemon_errx(const struct s3b_config *config, int value, const char *fmt, ...)
    __attribute__ ((__noreturn__, __format__ (__printf__, 3, 4)));

// Forking and child process management
extern pid_t start_child_process(const struct s3b_config *config, const char *executable, struct string_array *params);
extern void kill_remaining_children(const struct s3b_config *config, pid_t except, int signal);
extern pid_t wait_for_child_to_exit(const struct s3b_config *config, struct child_proc *proc, int sleep_if_none, int expect_signal);

// Bitmaps
extern bitmap_t *bitmap_init(s3b_block_t num_blocks, int value);
extern void bitmap_free(bitmap_t **bitmapp);
extern size_t bitmap_size(s3b_block_t num_blocks);
extern int bitmap_test(const bitmap_t *bitmap, s3b_block_t block_num);
extern void bitmap_set(bitmap_t *bitmap, s3b_block_t block_num, int value);
extern void bitmap_and(bitmap_t *dst, const bitmap_t *src, s3b_block_t num_blocks);
extern void bitmap_or(bitmap_t *dst, const bitmap_t *src, s3b_block_t num_blocks);
extern void bitmap_not(bitmap_t *bitmap, s3b_block_t num_blocks);

// Block lists
extern void block_list_init(struct block_list *list);
extern int block_list_append(struct block_list *list, s3b_block_t block_num);
extern void block_list_free(struct block_list *list);

// Generic s3backer_store functions
extern int generic_bulk_zero(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks);
