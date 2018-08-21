
A filesystem with fuse and rocksdb.
    
Compile with:
    make

Usage:

    ./rocksdb-fuse --dbpath=<path> <mountpoint>

Unmount:

    fusermount -u <mountpoint>
Depended:
* rocksdb
* fuse3.1 or upper
* boost