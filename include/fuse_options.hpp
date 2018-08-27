#pragma once
#include <string>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include "util.hpp"

namespace rocksfs
{

	struct FileSystemOptions
	{
		int Mount(int argc, char* argv[]);
		virtual ~FileSystemOptions();
	
		FileSystemOptions(const std::string& path);
		virtual void* Init(fuse_conn_info* conn, fuse_config* cfg);
		virtual int Access(const std::string& path, int mask);
		virtual int GetAttr(const std::string& path, struct stat* stbuf, fuse_file_info* fi);
		virtual int ReadDir(void* buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info* fi, enum fuse_readdir_flags flags);
		virtual int Open(const std::string& path, fuse_file_info* fi);
		virtual int Read(char* buf, size_t size, off_t offset, fuse_file_info* fi);
		virtual int Create(const std::string& path, mode_t mode, fuse_file_info* fi);
        virtual int Release(struct fuse_file_info* fi);
        virtual int Write(const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi);
        virtual int Unlink(const std::string& path);
        virtual int OpenDir(const std::string& path, fuse_file_info* fi);
        virtual int ReleaseDir(fuse_file_info* fi);
        virtual int Utimens(const std::string& path, const timespec tv[2], fuse_file_info* fi);
        virtual int MkDir(const std::string& path, mode_t mode);
        virtual int Rmdir(const std::string& path);
		virtual int Rename(const std::string& oldpath, const std::string& newpath, unsigned flags);
		virtual int Truncate(off_t offset, struct fuse_file_info *fi);
		virtual int Link(const std::string& oldpath, const std::string& newpath);//创建硬链接
		virtual int Flush(fuse_file_info* fi);
		virtual int Chmod (const std::string& path, mode_t, struct fuse_file_info *fi);
		virtual int Chown(const std::string& path, uid_t, gid_t, struct fuse_file_info *fi);
		virtual int SymLink(const std::string& filepath, const std::string& linkpath);
		virtual int ReadLink(const std::string& filepath, char* buf, size_t size);
		virtual int Mknod(const std::string& filepath, mode_t mode, dev_t dev);
#ifdef HAVE_SETXATTR
		virtual int SetXattr(const std::string& path, const std::string& name, const void* value, size_t size, int flags);
		virtual int GetXattr(const std::string& path, const std::string& name, char * buf, size_t size);
		virtual int ListXattr(const std::string& path, char* buf, size_t size);
		virtual int RemoveXattr(const std::string& path, const std::string& name);
#endif // HAVE_SETXATTR
	protected:
		rocksdb::ColumnFamilyHandle* hIndex;
		rocksdb::ColumnFamilyHandle* hData;
		rocksdb::ColumnFamilyHandle* hAttr;
		rocksdb::ColumnFamilyHandle* hDeleted;
		rocksdb::TransactionDB* db;
		std::atomic<int64_t> inodeCounter;//newt valid inode
		FileIndex GetIndex(const std::string& path) const;
		FileIndex GetIndexAndLock(const std::string& path, std::unique_ptr<rocksdb::Transaction>& txn) const;
	};
}



