#pragma once
#include <string>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include "util.hpp"

namespace rocksfs
{
	struct FileSystemOptionsBase
	{
		virtual ~FileSystemOptionsBase() = default;
		virtual void* Init(fuse_conn_info* conn, fuse_config* cfg) = 0;
		virtual int GetAttr(const std::string& path, struct stat* stbuf, fuse_file_info* fi) = 0;
		virtual int ReadDir(void* buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info* fi, enum fuse_readdir_flags flags) = 0;
		virtual int Open(const std::string& path, fuse_file_info* fi) = 0;
		virtual int Read(const std::string& path, char* buf, size_t size, off_t offset, fuse_file_info* fi) = 0;
		virtual int Create(const std::string& path, mode_t mode, fuse_file_info* fi) = 0;
		virtual int Release(struct fuse_file_info* fi) = 0;
		virtual int Write(const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi) = 0;
		virtual int Unlink(const std::string& path) = 0;
		virtual int OpenDir(const std::string& path, fuse_file_info* fi) = 0;
		virtual int ReleaseDir(fuse_file_info* fi) = 0;
		virtual int Utimens(const std::string& path, const timespec tv[2], fuse_file_info* fi) = 0;
		virtual int MkDir(const std::string& path, mode_t mode) = 0;
		virtual int Rmdir(const std::string& path) = 0;
		virtual int Rename(const std::string& oldpath, const std::string& newpath, unsigned flags) = 0;
		virtual int Truncate(off_t offset, struct fuse_file_info *fi) = 0;
		virtual int Link(const std::string& oldpath, const std::string& newpath) = 0;//创建硬链接
		virtual int Flush(fuse_file_info* fi) = 0;
		virtual int Chmod(const std::string& path, mode_t, struct fuse_file_info *fi) = 0;
		virtual int Chown(const std::string& path, uid_t, gid_t, struct fuse_file_info *fi) = 0;
#ifdef HAVE_SETXATTR
		virtual int SetXattr(const std::string& path, const std::string& name, const void* value, size_t size, int flags) = 0;
		virtual int GetXattr(const std::string& path, const std::string& name, char * buf, size_t size) = 0;
		virtual int ListXattr(const std::string& path, char* buf, size_t size) = 0;
		virtual int RemoveXattr(const std::string& path, const std::string& name) = 0;
#endif // HAVE_SETXATTR
		virtual int Mount(int argc, char* argv[]);
	protected:
		FileSystemOptionsBase() = default;
		rocksdb::ColumnFamilyHandle* hIndex;
		rocksdb::ColumnFamilyHandle* hData;
		rocksdb::ColumnFamilyHandle* hAttr;
		rocksdb::ColumnFamilyHandle* hDeleted;
		rocksdb::TransactionDB* db;
		std::atomic<int64_t> inodeCounter;//newt valid inode
		FileIndex GetIndex(const std::string& path);
		FileIndex GetIndexAndLock(const std::string& path, std::unique_ptr<rocksdb::Transaction>& txn);
	};
	struct FileSystemOptions
		:FileSystemOptionsBase
	{
		~FileSystemOptions();
		FileSystemOptions(const std::string& path);
		void* Init(fuse_conn_info* conn, fuse_config* cfg) override;
		int GetAttr(const std::string& path, struct stat* stbuf, fuse_file_info* fi) override;
		int ReadDir(void* buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info* fi, enum fuse_readdir_flags flags) override;
		int Open(const std::string& path, fuse_file_info* fi) override;
		int Read(const std::string& path, char* buf, size_t size, off_t offset, fuse_file_info* fi) override;
		int Create(const std::string& path, mode_t mode, fuse_file_info* fi) override;
        int Release(struct fuse_file_info* fi) override;
        int Write(const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi) override;
        int Unlink(const std::string& path) override;
        int OpenDir(const std::string& path, fuse_file_info* fi) override;
        int ReleaseDir(fuse_file_info* fi) override;
        int Utimens(const std::string& path, const timespec tv[2], fuse_file_info* fi) override;
        int MkDir(const std::string& path, mode_t mode) override;
        int Rmdir(const std::string& path) override;
		int Rename(const std::string& oldpath, const std::string& newpath, unsigned flags) override;
		int Truncate(off_t offset, struct fuse_file_info *fi) override;
		int Link(const std::string& oldpath, const std::string& newpath) override;//创建硬链接
		int Flush(fuse_file_info* fi) override;
		int Chmod (const std::string& path, mode_t, struct fuse_file_info *fi) override;
		int Chown(const std::string& path, uid_t, gid_t, struct fuse_file_info *fi) override;
#ifdef HAVE_SETXATTR
		int SetXattr(const std::string& path, const std::string& name, const void* value, size_t size, int flags) override;
		int GetXattr(const std::string& path, const std::string& name, char * buf, size_t size) override;
		int ListXattr(const std::string& path, char* buf, size_t size) override;
		int RemoveXattr(const std::string& path, const std::string& name) override;
#endif // HAVE_SETXATTR
	};
}



