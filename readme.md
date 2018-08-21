
A filesystem with fuse and rocksdb.
    
Compile with:

    make

Usage:

    ./rocksdb-fuse [options] <mountpoint>
    options:
        --help|-h       Print this help message
        --dbpath=<s>    The path for database files.
        -o allow_other	Allow other users to access the files.
        -o allow_root	This option is similar to allow_other but file access is limited to the user mounting the filesystem and root."

Unmount:

    fusermount -u <mountpoint>
Depended:
* rocksdb
* fuse3.1 or upper
* boost