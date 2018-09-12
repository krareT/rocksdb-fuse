#include "util.hpp"
#include <chrono>
#include <stdexcept>
#include <cstring>

using namespace std;
using namespace rocksfs;

timespec rocksfs::Now()
{
    const auto now = std::chrono::system_clock::now();
    return timespec{
        std::chrono::duration_cast<std::chrono::seconds>(
            now.time_since_epoch()).count(),
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count() % 1'000'000'000LL
    };
}

FileIndex::FileIndex(int64_t parentInode, std::string filename)
	:parentInode(parentInode),filename(filename)
{}

FileIndex::FileIndex(const std::string& inializer)
{
	//file index: inode:filename
	constexpr auto wide = sizeof(int64_t);//size of inode
	assert(inializer.size() > wide && inializer[wide] == ':');
	
	parentInode = ReadBigEndian64(inializer.c_str());
	filename = inializer.substr(sizeof(int64_t));
}

std::string FileIndex::GetFilename() const
{
	return filename;
}

std::string FileIndex::Key()const
{
	return Encode(inode);
}

std::string FileIndex::Index() const
{

    return  Encode(parentInode) + ":" + filename;
}



std::string rocksfs::Encode(int64_t inode)
{
	union {
		char buf[8];
		int64_t val;
	}conv;
	conv.val = boost::endian::native_to_big(inode);
    return string(conv.buf, sizeof (int64_t));
}

bool rocksfs::StartsWith(const std::string& mainstr, const std::string& substr)
{
	return substr.length() <= mainstr.length()
		&& equal(substr.begin(), substr.end(), mainstr.begin());
}


