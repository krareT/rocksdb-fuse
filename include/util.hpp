#pragma once
#include <string>
#include <mutex>
#include <ctime>
#include <iomanip>

namespace rocksfs
{
	timespec Now();

	struct FileIndex
	{
		FileIndex(int64_t parentInode, std::string filename);
		explicit FileIndex(const std::string& inializer);
		std::string GetFilename()const;
		//return encoded inode
		std::string Key()const;
		//return parent:filename
		std::string Index()const;
		int64_t parentInode;
		int64_t inode = -1;//negative for errno
		std::string filename;
		bool Bad()const { return filename.empty(); }
	};

	std::string Encode(int64_t inode);

	bool StartsWith(const std::string& mainstr, const std::string& substr);

}
