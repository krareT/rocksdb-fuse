#include "fuse_options.hpp"
using namespace  rocksfs;

namespace
{
    void *s_init(struct fuse_conn_info* conn, fuse_config* cfg) {
        RocksFs *ctx = static_cast<RocksFs*>(fuse_get_context()->private_data);
        ctx->Init(conn, cfg);
        return ctx;
    }
    int s_getattr(const char *path, struct stat *statbuf, fuse_file_info* fi) {
        if(!fi)
            return static_cast<RocksFs*>(fuse_get_context()->private_data)->GetAttr(path, statbuf, fi);
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->GetAttr("", statbuf, fi);
    }

    int s_utimens(const char *path, const struct timespec tv[2], fuse_file_info* fi) {
        if (!fi)
            return static_cast<RocksFs*>(fuse_get_context()->private_data)->Utimens(path, tv, fi);
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Utimens("", tv, fi);
    }
    int s_open(const char *path, struct fuse_file_info *fi) {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Open(path, fi);
    }
    int s_read(const char *path, char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi) {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Read(buf, size, offset, fi);
    }
    int s_write(const char *path, const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi) {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Write(buf, size, offset, fi);
    }
    int s_release(const char *path, struct fuse_file_info *fi) {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Release(fi);
    }

    int s_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, fuse_readdir_flags flag) {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->ReadDir(buf, filler, offset, fi, flag);
    }

    int s_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Create(path, mode, fi);
    }
    int s_unlink(const char *path)
    {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Unlink(path);
    }
    int s_opendir(const char *path, fuse_file_info* fi)
    {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->OpenDir(path, fi);
    }
    int s_releasedir(const char *path, fuse_file_info* fi)
    {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->ReleaseDir(fi);
    }
    int s_mkdir(const char *path, mode_t mode)
    {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->MkDir(path, mode);
    }

    int s_rmdir(const char* path)
    {
        return static_cast<RocksFs*>(fuse_get_context()->private_data)->Rmdir(path);
    }

	int s_link(const char* oldpath, const char* newpath)
	{
		return static_cast<RocksFs*>(fuse_get_context()->private_data)->Link(oldpath, newpath);
	}
}

int RocksFs::Mount(int argc, char* argv[])
{
    fuse_operations res{};
    res.init = s_init;
    res.getattr = s_getattr;
    res.readdir = s_readdir;
    res.open = s_open;
    res.read = s_read;
    res.utimens = s_utimens;
    res.write = s_write;
    res.release = s_release;
    res.create = s_create;
    res.opendir = s_opendir;
    res.releasedir = s_releasedir;
    res.unlink = s_unlink;
    res.mkdir = s_mkdir;
    res.rmdir = s_rmdir;
	res.link = s_link;
    return fuse_main(argc, argv, &res, this);
}
