# $Id$

#
# Copyright 2008 Archie L. Cobbs.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# 

Name:           s3backer
Version:        1.2.0
Release:        1
License:        GNU General Public License, Version 2
Summary:        FUSE-based single file backing store via Amazon S3
Group:          System/Filesystems
Source:         http://%{name}.googlecode.com/files/%{name}-%{version}.tar.gz
URL:            http://%{name}.googlecode.com/
BuildRoot:      %{_tmppath}/%{name}-%{version}-root
BuildRequires:  curl-devel
BuildRequires:  fuse-devel >= 2.5
BuildRequires:  openssl-devel
BuildRequires:  expat
BuildRequires:  pkgconfig

%description
s3backer is a filesystem that contains a single file backed by the
Amazon Simple Storage Service (Amazon S3).  As a filesystem, it is quite
small and simple: it provides a single normal file having a fixed size.
The file is divided up into blocks, and the content of each block is
stored in a unique Amazon S3 object.  In other words, what s3backer
provides is really more like an S3-backed virtual hard disk device,
rather than a filesystem.

In typical usage, a `normal' filesystem is mounted on top of the file
exported by the s3backer filesystem using a loopback mount (or disk
image mount on Mac OS X).

This arrangement has several benefits compared to more complete S3
filesystem implementations:

  * By not attempting to implement a complete filesystem, which is a
    complex undertaking and difficult to get right, s3backer can stay very
    lightweight and simple. Only three HTTP operations are used: GET, PUT,
    and DELETE.  All of the experience and knowledge about how to properly
    implement filesystems that already exists can be reused.

  * By utilizing existing filesystems, you get full UNIX filesystem
    semantics.  Subtle bugs or missing functionality relating to hard links,
    extended attributes, POSIX locking, etc. are avoided.

  * The gap between normal filesystem semantics and Amazon S3 ``eventual
    consistency'' is more easily and simply solved when one can interpret
    S3 objects as simple device blocks rather than filesystem objects
    (see below).

  * When storing your data on Amazon S3 servers, which are not under
    your control, the ability to encrypt data becomes a critical issue.
    s3backer provides this automati- cally because encryption support is
    already included in the Linux loopback mechanism.

  * Since S3 data is accessed over the network, local caching is also
    very important for performance reasons.  Since s3backer presents the
    equivalent of a virtual hard disk to the kernel, most of the filesystem
    caching can be done where it should be: in the kernel, via the kernel's
    page cache.  However s3backer also includes its own inter- nal block
    cache for increased performance, using asynchronous worker threads to
    take advantage of the parallelism inherent in the network.

Consistency Guarantees

Amazon S3 makes relatively weak guarantees relating to the timing
and consistency of reads vs. writes (collectively known as ``eventual
consistency'').  s3backer includes logic and configuration parameters to
work around these limitations, allowing the user to guarantee consistency
to whatever level desired, up to and including 100% detec- tion and
avoidance of incorrect data.  These are:

    1.  s3backer enforces a minimum delay between consecutive PUT or
        DELETE operations on the same block.  This ensures that Amazon S3
        doesn't receive these operations out of order.

    2.  s3backer maintains an internal block MD5 checksum cache, which
        enables automatic detection and rejection of `stale' blocks returned
        by GET operations.

This logic is configured by the following command line options:
--md5CacheSize, --md5CacheTime, --minWriteDelay, --initialRetryPause,
and --maxRetryPause, all of which have default values.

Zeroed Block Optimization

As a simple optimization, s3backer does not store blocks containing
all zeroes; instead, they are simply deleted.  Conversely, reads of
non-existent blocks will contain all zeroes.  In other words, the backed
file is always maximally sparse.

As a result, blocks do not need to be created before being used and no
special initialization is necessary when creating a new filesystem.

File and Block Size Auto-Detection

As a convenience, whenever the first block of the backed file is written,
s3backer includes as meta-data (in the ``x-amz-meta-s3backer-filesize''
header) the total size of the file.  Along with the size of the block
itself, this value can be checked and/or auto-detected later when the
filesystem is remounted, eliminating the need for the --blockSize
or --size flags to be explicitly provided and avoiding accidental
mis-interpretation of an existing filesystem.

Block Cache

s3backer includes support for an internal block cache to increase
performance.  The block cache cache is completely separate from the
MD5 cache which only stores MD5 check- sums transiently and whose sole
purpose is to mitigate ``eventual consistency''.  The block cache is a
traditional cache containing cached data blocks.  When enabled, reads of
cached blocks return immediately with no network traffic.  Writes to the
cache also return immediately and trigger an asynchronous write operation
to the network via a separate worker thread.  Because the kernel typically
writes blocks through FUSE filesystems one at a time, performing writes
asynchronously allows s3backer to take advan- tage of the parallelism
inherent in the network, vastly improving write performance.

The block cache is configured by the following command line options:
--blockCacheSize, --blockCacheThreads and --blockCacheWriteDelay.

Read-Only Access

An Amazon S3 account is not required in order to use s3backer.  Of course
a filesystem must already exist and have S3 objects with ACL's configured
for public read access (see --accessType below); users should perform
the looback mount with the read-only flag (see mount(8)) and provide
the --readOnly flag to s3backer.  This mode of operation facilitates
the creation of public, read-only filesystems.

Simultaneous Mounts

Although it functions over the network, the s3backer filesystem is not
a distributed filesystem and does not support simultaneous read/write
mounts.  (This is not something you would normally do with a hard-disk
partition either.)  s3backer does not detect this situation; it is up
to the user to ensure that it doesn't happen.

Statistics File

s3backer populates the filesystem with a human-readable statistics file.
See --statsFilename below.

Logging

In normal operation s3backer will log via syslog(3).  When run with the
-d or -f flags, s3backer will log to standard error.

%prep
%setup -q

%build
%{configure}
make

%install
rm -rf ${RPM_BUILD_ROOT}
%{makeinstall}

%files
%attr(0755,root,root) %{_bindir}/%{name}
%if 0%{?mandriva_version}
%attr(0644,root,root) %{_mandir}/man1/%{name}.1
%else
%attr(0644,root,root) %{_mandir}/man1/%{name}.1.gz
%endif
%defattr(0644,root,root,0755)
%doc %{_datadir}/doc/packages/%{name}-%{version}

