
A filesystem with fuse and rocksdb.

Compile with:

    make

Usage:

    ./rocksdb-fuse [options] <mountpoint>
    options:
        --help|-h       Print this help message
        --dbpath=<s>    The path for database files.
        -o allow_other  Allow other users to access the files.
        -o allow_root   This option is similar to allow_other but file access is limited to the user mounting the filesystem and root."
Note: If you want mount filesystem with option **allow_other** or **allow_root**, the configuration **user_allow_other** should be set in file /etc/fuse.conf 

Unmount:

    fusermount -u <mountpoint>
Depended:
* rocksdb 5.4 or upper (for PinnableSlice)
* fuse3.1 or upper
* boost
* linux-test-project/ltp (for testcases, optional)

Run testcases:
    
    mount filesystem with allow_root or allow_other
    cd LTPROOT
    sudo ./runltp -f /path/to/rocksdb-fuse/ltp-testcases -d <mountpoint>   -p -q \
    [-l <logfile>] [-C <fail test cases>] [-o <redirect test output file>] 
    
