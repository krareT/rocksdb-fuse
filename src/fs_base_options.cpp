#include <string>
#include <experimental/filesystem>
#include <rocksdb/utilities/transaction_db.h>
#include "fuse_options.hpp"
#include "util.hpp"

using namespace rocksdb;
using namespace rocksfs;
using namespace std;
namespace fs = std::experimental::filesystem;

FileIndex FileSystemOptionsBase::GetIndex(const std::string& path)
{
	if (path == "/")
	{
		FileIndex res(0, "/");
		res.inode = 0;
		return res;
	}
	FileIndex idx(0, "/");
	idx.inode = 0;
	auto file_path = fs::path(path);
	for (auto itor = ++file_path.begin(); itor != file_path.end(); ++itor)
	{
		idx.filename = itor->generic_string();
		idx.parentInode = idx.inode;
		string encodedIdx;
		const string tmp = idx.Index();
		Status s = db->Get(ReadOptions(), hIndex, tmp, &encodedIdx);
		if (!s.ok())
		{
			idx.filename.clear();
			if (s.IsNotFound())
				idx.inode = -ENOENT;
			else
				idx.inode = -EIO;
			return idx;
		}

		idx.inode = ReadBigEndian64(encodedIdx.c_str());
	}
	return idx;
}

FileIndex FileSystemOptionsBase::GetIndexAndLock(const std::string & path, std::unique_ptr<rocksdb::Transaction>& txn)
{
	if (path == "/")
	{
		FileIndex res(0, "/");
		res.inode = 0;
		return res;
	}
	fs::path file_path(path);
	auto parentIdx = GetIndex(file_path.parent_path().generic_string());
	if (parentIdx.Bad())
	{
		FileIndex res{ 0,"" };
		res.inode = parentIdx.inode;
		return res;
	}
	string encoded_idx;
	FileIndex res{ parentIdx.inode,file_path.filename().generic_string() };
	Status s = txn->GetForUpdate(ReadOptions(), hIndex, res.Index(), &encoded_idx);
	if (!s.ok())
	{
		res.filename = "";
		if (s.IsNotFound())
			res.inode = -ENOENT;
		else if (s.IsDeadlock())
			res.inode = -EBUSY;
		else
			res.inode = -EIO;
		return res;
	}
	res.inode = ReadBigEndian64(encoded_idx.c_str());
	return res;
}