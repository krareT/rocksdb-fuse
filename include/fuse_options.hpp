#pragma once
#include <string>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include "util.hpp"

namespace rocksfs
{
	struct RocksFs final
	{
		~RocksFs();
		RocksFs(const std::string& path);
		void* Init(fuse_conn_info* conn, fuse_config* cfg);
		int GetAttr(const std::string& path, struct stat* stbuf, fuse_file_info* fi);
		int ReadDir(void* buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info* fi, enum fuse_readdir_flags flags);
		int Open(const std::string& path, fuse_file_info* fi);
		int Read(char* buf, size_t size, off_t offset, fuse_file_info* fi);
		int Create(const std::string& path, mode_t mode, fuse_file_info* fi);
        int Release(struct fuse_file_info* fi);
        int Write(const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi);
        int Unlink(const std::string& path);
        int OpenDir(const std::string& path, fuse_file_info* fi);
        int ReleaseDir(fuse_file_info* fi);
        int Utimens(const std::string& path, const timespec tv[2], fuse_file_info* fi);
        int MkDir(const std::string& path, mode_t mode);
        int Rmdir(const std::string& path);
		int Rename(const std::string& oldpath, const std::string& newpath, unsigned flags);
		int Truncate(off_t offset, struct fuse_file_info *fi);
		int Link(const std::string& oldpath, const std::string& newpath);//创建硬链接
		int Flush(fuse_file_info* fi);
		int Chmod (const std::string& path, mode_t, struct fuse_file_info *fi);
		int Chown(const std::string& path, uid_t, gid_t, struct fuse_file_info *fi);
		rocksdb::ColumnFamilyHandle* hIndex;
		rocksdb::ColumnFamilyHandle* hData;
		rocksdb::ColumnFamilyHandle* hAttr;
		rocksdb::ColumnFamilyHandle* hDeleted;
		rocksdb::TransactionDB* db;
		std::atomic<int64_t> inodeCounter;//newt valid inode
        int Mount(int argc, char* argv[]);
	protected:
		FileIndex GetIndex(const std::string& path);
		FileIndex GetIndexAndLock(const std::string& path, std::unique_ptr<rocksdb::Transaction>& txn);
	};
}



