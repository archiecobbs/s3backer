#!/usr/bin/env python3
#
# This script creates an NBD-backed zpool. It needs to be run with root permissions.
#
# Some explanatory remarks on the procedure:
#
# Generally, the larger the s3backer block size the better we utilize the network
# bandwidth and the fewer we pay for HTTP requests. However, larger block sizes come for
# the price of read/write amplification (when partial blocks have to be read or written).
#
# To eliminate all read/write amplification, the s3backer block size must match the
# "modification unit" of the filesystem. Lowering it beyond this value has no further
# advantages.
#
# For ZFS, the block size varies between 2^ashift and *recordsize*. We also want the gap
# between these values to be reasonably large (otherwise compression efficiency will be
# limited). However, as far as s3backer is concerned, this means that any read/write could
# thus be for as little as a^ashift.
#
# Unfortunately, with a 4 kB block size (ashift=12), we're dealing not just with a large
# number of HTTP requests ($ 1.31 for PUT'ing 1 GB) but also with at least 10% request
# overhead (assuming 512 bytes of HTTP metadata) and at least 20x "time overhead" (latency
# of 30 ms is 20x higher than raw transmission time of 4 kB at 3 MB/s).
#
# In theory, we could could compensate for the extra latency by using a large number of
# parallel connections. In practice, this is difficult to accomplish because s3backer
# splits incoming write requests and processes them sequentially. Reducing the size of NBD
# requests to 4 kB would address this, but then we'd probably run into kernel-limits on
# the number of pending NBD requests. So instead we'd have to enable the s3backer on-disk
# cache - which then incurs its own overhead.
# 
# Furthermore, a large number of files will consist of full *recordsize* blocks, so
# processing those in 4 kB units adds needless overhead in many cases.
#
# Therefore, we almost certainly do not want to set the s3backer block size as low
# as 2^ashift.
#
# Luckily, for ZFS we can also do better than picking some intermediate value. We assemble
# the zpool from a regular (disk) vdev (backed by one S3 bucket) and a *special* vdev
# (backed by a second S3 bucket). The *special* vdev will be used for all metadata
# as well as file blocks above *special_small_blocks* in size.
#
# This means that the minimum modification unit for the regular vdev will be at least
# *special_small_blocks*, and we can set a corresponding s3backer block size, while
# smaller blocks will be written to the *special* vdev for which we can then use a smaller
# s3backer block size. To maximize the benefit, we set *special_small_blocks* to
# *recordsize-1* (128 kB-1), and the large s3 block size to a multiple of this (5x).
#
# For the small s3backer block size we use, somewhat arbitrarily, 32 kB. At this value,
# the HTTP overhead is 1.5%, and the the time to transfer the data (10 ms at 3 MB/s) is
# well below the overall Starlink connection latency (~30 ms). Therefore, read
# amplification is neglectable and write amplification will incur only the round-trip
# latency for the additional read().
#

KEYFILE = "/home/nikratio/lib/s3_zfs.key"
BUCKET_NAME = 'nikratio-backup'
BUCKET_REGION = 'eu-west-2'
ZPOOL_NAME = 's3backup'
DSET_NAME = 'vostro'

S3_BLOCKSIZE_SMALL_KB=32
S3_BLOCKSIZE_LARGE_KB=512
ZFS_BLOCKSIZE_THRESHOLD_KB=128
ZFS_RECORDSIZE_KB=128

# Where to mount the zpool
ZPOOL_DIR = '/zpools'


import subprocess
from contextlib import ExitStack
import os.path
import sys
import time
import shlex
import tempfile

if os.geteuid() != 0:
    print('This script must be run as root.', file=sys.stderr)
    sys.exit(3)

def wait_for(pred, *args, timeout=5):
    waited = 0
    while waited < timeout:
        if pred(*args):
            return
        time.sleep(1)
        waited += 1
    raise TimeoutError()


def run(cmdline, wait=True, **kwargs):
    print('Running', shlex.join(cmdline))
    if wait:
        return subprocess.run(cmdline, **kwargs, check=True)
    else:
        return subprocess.Popen(cmdline, **kwargs)


def find_unused_nbd():
    if not os.path.exists('/sys/block/nbd0'):
        return RuntimeError("Can't find NBDs - is the nbd module loaded?")
    
    for devno in range(20):
        if not os.path.exists(f'/sys/block/nbd{devno}/pid'):
            return devno

    raise RuntimeError("Can't find any available NBDs")


with ExitStack() as exit_stack:
    
    tempdir = exit_stack.enter_context(tempfile.TemporaryDirectory())

    nbdkits = {}
    def cleanup():
        for proc in nbdkits.values():
            try:
               proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.terminate()
    exit_stack.callback(cleanup)

    sockets = {}
    for kind in ('sb', 'lb'): # "small blocks" and "large blocks"
        socket_name = f'{tempdir}/nbd_socket_{kind}'
        cmdline = [
            'nbdkit', '--unix',  socket_name, '--foreground',
            '--filter=exitlast', '--threads', '16', 's3backer',
            f's3b_region={BUCKET_REGION}', 's3b_size=50G', 's3b_force=true',
            f'bucket={BUCKET_NAME}/{kind}']

        if kind == 'lb':
            cmdline.append(f's3b_blockSize={S3_BLOCKSIZE_LARGE_KB}K')
        else:
            assert kind == 'sb'
            cmdline.append(f's3b_blockSize={S3_BLOCKSIZE_SMALL_KB}K')
        nbdkits[kind] = run(cmdline, wait=False)
        sockets[kind] = socket_name

    devices = {}
    for (kind, socket) in sockets.items():
        wait_for(os.path.exists, socket)
        devno = find_unused_nbd()
        devname = f'/dev/nbd{devno}'
        run(['nbd-client', '-unix', socket, devname])
        devices[kind] = devname
        exit_stack.callback(run, ['nbd-client', '-d', devname])

        with open(f'/sys/block/nbd{devno}/queue/max_sectors_kb', 'w') as fh:
            if kind == 'lb':
                print(str(S3_BLOCKSIZE_LARGE_KB), file=fh)
            else:
                assert kind == 'sb'
                print(str(S3_BLOCKSIZE_SMALL_KB), file=fh)
    
    cmdline  = ['zpool', 'create', '-R', ZPOOL_DIR ]
    for arg in ('ashift=9', 'autotrim=on', 'failmode=continue'):
        cmdline.append('-o')
        cmdline.append(arg)
    for arg in ('acltype=posixacl', 'relatime=on', 'xattr=sa', 'compression=zstd-19',
                'dedup=on', 'sync=disabled', f'special_small_blocks={ZFS_BLOCKSIZE_THRESHOLD_KB*1024}',
                'redundant_metadata=most', f'recordsize={ZFS_RECORDSIZE_KB*1024}',
                'encryption=on', 'keyformat=passphrase', f'keylocation=file://{KEYFILE}'):
        cmdline.append('-O')
        cmdline.append(arg)
    cmdline += [ ZPOOL_NAME, devices['lb'], 'special', devices['sb'] ]
    run(cmdline)
    exit_stack.callback(run, ['zpool', 'export', ZPOOL_NAME])

    run(['zfs', 'create', f'{ZPOOL_NAME}/{DSET_NAME}'])
