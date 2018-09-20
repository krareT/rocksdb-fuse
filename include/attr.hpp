#pragma once
#include <string>
#include <ctime>
#include <sys/fcntl.h>
#include <sys/stat.h>
struct Attr final
{
    explicit Attr(const std::string& attr);
    Attr() = default;

    timespec atime, mtime, ctime;
    size_t size;
    int64_t nlink;
    int64_t inode;
    uid_t uid;
    gid_t gid;
    mode_t mode;
public:
    void Fill(struct stat* st)const;
    std::string Encode()const;
    std::string ViewAccess()const;
    static Attr CreateNormal();
    static Attr CreateDir(uint64_t inode);
    //使用buf初始化attr,这里不用构造函数是为了返回状态，而使用异常过于笨重
    static bool Decode(const std::string& buf, Attr* attr);
    friend std::ostream& operator<<(std::ostream& o, const Attr& attr);
};
