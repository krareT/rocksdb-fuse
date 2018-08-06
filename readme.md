
A filesystem with fuse and rocksdb.
    
Compile with:
    make

Usage:

    ./rocksdb-fuse --dbpath=<path> <mountpoint>\n

Depended:
* rocksdb
* fuse3.1 or upper
* boost