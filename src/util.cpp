#include "util.hpp"
#include <chrono>
#include <stdexcept>
#include <cstring>

using namespace std;
using namespace rocksfs;

timespec Now() {
	const auto now = std::chrono::system_clock::now();
	return timespec{
		std::chrono::duration_cast<std::chrono::seconds>(
			now.time_since_epoch()).count(),
		std::chrono::duration_cast<std::chrono::nanoseconds>(
			now.time_since_epoch()).count() % 1'000'000'000LL
	};
}

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
	constexpr auto wide = sizeof(int64_t);
	if (inializer.size() > wide || inializer[wide] != ':')
		throw std::runtime_error("the is not a valid data");
	int64_t serialized;
	memmove(&serialized, inializer.c_str(), sizeof(int64_t));
	parentInode = __bswap_64(serialized);
	filename = inializer.substr(sizeof(int64_t));
}

std::string FileIndex::GetFilename() const
{
	return filename;
}

std::string FileIndex::Key()const
{
    int64_t res = __bswap_64(inode);
    return std::string(reinterpret_cast<char*>(&res),sizeof res);
}

std::string FileIndex::Index() const
{

    return  Encode(parentInode) + ":" + filename;
}



std::string rocksfs::Encode(int64_t inode)
{
    auto res = __bswap_64(inode);
    return string(reinterpret_cast<char*>(&res), sizeof (int64_t));
}

bool rocksfs::StartsWith(const std::string& mainstr, const std::string& substr)
{
	return substr.length() <= mainstr.length()
		&& equal(substr.begin(), substr.end(), mainstr.begin());
}


