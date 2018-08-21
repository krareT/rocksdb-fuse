#include <string>
#include <cstring>
#include <stdexcept>
#include <cmath>
#include <boost/format.hpp>


#include <sys/fcntl.h>

#include "attr.hpp"
#include "fuse_options.hpp"

using namespace std;
using namespace rocksfs;

void Attr::Fill(struct stat* st) const
{

	st->st_atim = atime;
	st->st_ctim = ctime;
	st->st_mtim = mtime;
	st->st_mode = mode;
	st->st_uid = uid;
	st->st_gid = gid;
	st->st_size = size;
	st->st_nlink = nlink;
	st->st_blksize = 4096;
	st->st_ino = inode;
	//TODO 文件存在空洞时的块数维护支持，目前假装不支持空洞。
	st->st_blocks = (size + 4096 - 1) / 4096;

}

std::string Attr::Encode() const
{
	char buf[sizeof(Attr)];
	memmove(buf, this, sizeof(Attr));
	return std::string(buf, sizeof(Attr));
}

std::string Attr::ViewAccess() const
{
	std::string res;
	res.push_back((S_ISDIR(mode)) ? 'd' : '-');
	res.push_back((mode & S_IRUSR) ? 'r' : '-');
	res.push_back((mode & S_IWUSR) ? 'w' : '-');
	res.push_back((mode & S_IXUSR) ? 'x' : '-');
	res.push_back((mode & S_IRGRP) ? 'r' : '-');
	res.push_back((mode & S_IWGRP) ? 'w' : '-');
	res.push_back((mode & S_IXGRP) ? 'x' : '-');
	res.push_back((mode & S_IROTH) ? 'r' : '-');
	res.push_back((mode & S_IWOTH) ? 'w' : '-');
	res.push_back((mode & S_IXOTH) ? 'x' : '-');
	return res;
}


Attr::Attr(const string& attr)  // NOLINT
{
	assert(attr.size() == sizeof(Attr));

	memmove(this, attr.c_str(), sizeof(Attr));
}




Attr Attr::CreateNormal()
{
	Attr attr{};
	attr.atime = attr.mtime = attr.ctime = Now();
	attr.nlink = 1;
	attr.gid = fuse_get_context()->gid;
	attr.uid = fuse_get_context()->uid;
	attr.mode = S_IFREG | 0644;
	attr.size = 0;
	return attr;
}

Attr Attr::CreateDir(uint64_t inode)
{
	Attr attr{};
	attr.inode = inode;
	attr.atime = attr.mtime = attr.ctime = Now();
	attr.nlink = 2;
	attr.gid = fuse_get_context()->gid;
	attr.uid = fuse_get_context()->uid;
	attr.mode = S_IFDIR | 0755;
	attr.size = 4096;
	return attr;
}

bool Attr::Decode(const std::string& buf, Attr* attr)
{
    if(buf.size()!=sizeof(Attr))
        return false;

    memmove(attr, buf.data(), sizeof(Attr));
    return true;
}
namespace
{
    string Fmt(const timespec& tm)
    {
        char buf[200];
        const auto offset = std::strftime(buf, sizeof buf, "%D %T.", std::gmtime(&tm.tv_sec));
        const auto size = sprintf(buf + offset, "%ld", tm.tv_nsec);
        return string(buf, size + offset + 1);
    }
}
std::ostream& operator<<(std::ostream& o, const Attr& attr)
{
    auto fmt = boost::format(
        "Attr:\n "
        "atime: %s\n"
        "mtime: %s\n"
        "ctime: %s\n"
        "size:  %lld\n"
        "nlink: %lld\n"
        "uid:   %lld\n"
        "gid:   %lld\n"
        "mode:  %o\n"
    );

    fmt = fmt % Fmt(attr.atime) % Fmt(attr.mtime) % Fmt(attr.ctime) % attr.size%attr.nlink%attr.uid%attr.gid%attr.mode;
    o << fmt;

    return o;

}
