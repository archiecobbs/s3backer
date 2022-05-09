Using the NBD plugin
--------------------

Instead of using s3backer to provide a FUSE file system with a single file that is then loop-mounted to provide a block device, it can also act as a Network Block Device (NBD) server. In this case, the kernel will directly provide a `/dev/nbdX` block device that is backed by s3backer. NBD is supported on Linux, FreeBSD, and other systems.

Architecturally, using a network block device makes more sense than using a FUSE file system since it is both simpler and better matches the intented use of either feature. In theory, NBD-mode should use less memory and give higher throughput and lower latency because:

 - The kernel no longer serializes write and read requests but issues them concurrently.
 - Read and write request size can exceed 128 kB
 - The system can still be reliably hibernated (a running FUSE daemon may prevent this)
 - Requests pass through the VFS only once, not twice
 - Data is present in the page cache only once, not twice

However, this mode of s3backer operation is still experimental. It is possible that in practice, performance is actually inferior due to implementation details in any of s3backer, nbdkit, FUSE, or NBD. Please report any improvements, degradations, or bugs that you observe.

To use NBD-mode, make sure you have [nbdkit](https://github.com/libguestfs/nbdkit) installed, then build and install s3backer normally. You can then run s3backer in NBD mode using the `--nbd` flag. In this mode, specify an NBD device such as `/dev/nbd0` instead of a mount point. Then `/dev/nbd0` can be used with regular filesystems commands (`mkfs` et al). You must run s3backer as root when using the `--nbd` flag.

To disconnect the block device, use:

```
$ nbd-client -d /dev/ndb0
```

The `ndbkit` server (and s3backer plugin) will still be running; to have it disconnect automatically when the client disconnects, add `--ndb-flags --filter=exitlast` to the `s3backer` command line.

You can also invoke the installed `s3backer` NBD plugin directly using `ndbkit(1)`. See the `s3backer(1)` and `nbdkit(1)` man pages for details.

Performance Tuning
------------------

- Set `/sys/block/nbdX/queue/max_sectors_kb` to the block size configured in s3backer (not smaller to avoid needless reading/writing of partial blocks, and not larger to maximize concurrency)

- Experiment with different numbers of threads (`--threads` option to nbdkit). Especially for small block sizes and slow connections, the default is probably not optimal.

- When using journaling filesystems (like ext4), disable journaling to prevent the same
  data being sent over the network twice.

- When using ZFS, assemble the zpool from two NBDs (reading two distinct S3 buckets): (1) a *special* vdev with a small s3backer block size, and (2) a regular (*disk*) vdev with a larger s3backer block size. Set the *special_small_blocks* property of your ZFS datasets to *recordsize* - 1. The small block size should well below *recordsize* (but no smaller than 2^*ashift*), and the large block size some multiple of *recordsize*.

- When using ZFS, disable synchronous requests (through the zfs `sync=disabled` property) or configure a separate *log* vdev for the zpool that is backed by local storage. In either case, this avoids data from synchronous requests being sent over the network twice.

- If you aren't using ZFS, try to avoid synchronous writes by other means. If this can't
  be done at the filesystem level, you can wrap userspace applications with
  [eatmydata](https://www.flamingspork.com/projects/libeatmydata/). If neither of this is
  possible (and only then), you can reduce the impact by enabling s3backer's on-disk cache
  (but see below).

- Avoid using the s3backer block cache (both in-memory and on-disk variants) unless you
  absolutely require it for cross-reboot cache persistence or to reduce the performance
  penalty of synchronous requests. For any other purpose, you should be able to get better
  results by adjusting the maximum size of NBD requests, the s3backer block size, the file
  system block size, and the page cache parameters (`/proc/sys/vm/*`).
