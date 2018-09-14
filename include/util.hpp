#pragma once
#include <string>
#include <mutex>
#include <ctime>
#include <iomanip>
#include <cstddef>
#include <boost/endian/conversion.hpp>
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
        bool Bad()const { return inode < 0; }
    };

    std::string Encode(int64_t inode);

    bool StartsWith(const std::string& mainstr, const std::string& substr);

    inline int64_t ReadBigEndian64(const void* p, size_t len = 8)
    {
        union {
            char bytes[8];
            uint64_t value;
        } c;
        c.value = 0;  // this is fix for gcc-4.8 union init bug
        memcpy(c.bytes + (8 - len), p, len);
        return boost::endian::big_to_native(c.value);
    }

}
