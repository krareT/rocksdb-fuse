#include <algorithm>
#include <iostream>
#include <fstream>
#include <thread>
#include <cmath>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/db.h>
#include <boost/format.hpp>
#include <experimental/filesystem>
#include <mutex>
#include <fcntl.h>
#include <cerrno>
#ifdef HAVE_SETXATTR
#include <attr/xattr.h>
#endif // HAVE_SETXATTR
#include "fuse_options.hpp"
#include "util.hpp"
#include "attr.hpp"

using namespace rocksdb;
using namespace rocksfs;
using namespace std;
namespace fs = std::experimental::filesystem;


using Txn = std::unique_ptr<rocksdb::Transaction>;

#define CheckStatus(s)\
			if (s.IsNotFound())\
				return -ENOENT;\
			if (s.IsTimedOut())\
				return -EBUSY;\
			if (!s.ok())\
				return -EIO;




namespace
{
	const int64_t kPageSize = 4096;
	const string kMaxEncodedIdx = Encode(std::numeric_limits<int64_t>::max());//大端表示的int64_t最大值
	struct Locker
	{
		void Open(int64_t inode);
		bool Release(int64_t inode);
		bool IsOpening(int64_t inode);
		std::map<int64_t, int> map;
		std::mutex mtx;
	}opening_files;

	//返回待写入的data的key
	string PageIndex(int64_t inode, int64_t offset)
	{
		return Encode(inode) + Encode(offset / kPageSize);
	}
	string PageIndex(const string& encoded_inode, int64_t offset)
	{
		return encoded_inode + Encode(offset / kPageSize);
	}

}

bool Locker::IsOpening(int64_t inode)
{
    lock_guard<mutex> l(mtx);
    return map.find(inode) != map.end();
}

void Locker::Open(int64_t inode)
{
	lock_guard<mutex> l(mtx);
	map[inode] += 1;
}
bool Locker::Release(int64_t inode)
{
	lock_guard<mutex> l(mtx);
	const auto counter = --map[inode] ;
	assert(counter >= 0);
	if(counter == 0)
		map.erase(inode);
	return counter == 0;
}




rocksfs::FileSystemOptions::~FileSystemOptions()
{
	db->DestroyColumnFamilyHandle(hIndex);
	db->DestroyColumnFamilyHandle(hData);
	db->DestroyColumnFamilyHandle(hAttr);
	db->DestroyColumnFamilyHandle(hDeleted);
	delete db;
}

FileSystemOptions::FileSystemOptions(const std::string& path)
{
	std::vector<ColumnFamilyDescriptor> descriptors;
	descriptors.emplace_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
	descriptors.emplace_back(ColumnFamilyDescriptor("Data", ColumnFamilyOptions()));
	descriptors.emplace_back(ColumnFamilyDescriptor("Index", ColumnFamilyOptions()));
	descriptors.emplace_back(ColumnFamilyDescriptor("Attr", ColumnFamilyOptions()));
	descriptors.emplace_back(ColumnFamilyDescriptor("DeletedFile", ColumnFamilyOptions()));

	DBOptions options;
	options.create_if_missing = true;
	options.create_missing_column_families = true;

	rocksdb::TransactionDB* dbPtr = nullptr;
	std::vector<ColumnFamilyHandle*> handles;
	Status s = TransactionDB::Open(options, TransactionDBOptions(), path, descriptors, &handles, &dbPtr);

	if (!s.ok())
	{
        delete dbPtr;
		printf("open database error at %s\nerror:%s", path.c_str(), s.getState());
		exit(0);
	}
	dbPtr->DestroyColumnFamilyHandle(handles[0]);
	hData = handles[1];
	hIndex = handles[2];
	hAttr = handles[3];
	hDeleted = handles[4];
	db = dbPtr;
	unique_ptr<Iterator> itor(db->NewIterator(ReadOptions(), hAttr));
	itor->SeekToLast();
	if (itor->Valid())//not a new database
	{
		int64_t counter;
		memmove(&counter, itor->key().data(), sizeof(int64_t));
		inodeCounter = ReadBigEndian64(itor->key().data()) + 1;
	}	
	else//shall init database
	{
		const string root_inode(sizeof(uint64_t), '\0');
		//batch.Put(hIndex, "/", string('0', sizeof(uint64_t)));不需要
		Attr attr{};
		attr.atime = attr.mtime = attr.ctime = Now();
		attr.nlink = 2;
		attr.gid = getegid();
		attr.uid = geteuid();
		attr.mode = S_IFDIR | 0755;
		attr.size = 4096;
		s = db->Put(WriteOptions(), hAttr, root_inode, attr.Encode());
		assert(s.ok());
		inodeCounter = 1;
	}

	itor.reset( db->NewIterator(ReadOptions(), hDeleted));
	for (itor->SeekToFirst(); itor->Valid(); itor->Next())//删除孤儿文件
	{
		db->Delete(WriteOptions(),hData, itor->key());//TODO 文件分块时需要删除range
		db->Delete(WriteOptions(),hAttr, itor->key());
		db->Delete(WriteOptions(),hDeleted, itor->key());
	}
}

void* FileSystemOptions::Init(fuse_conn_info* conn, fuse_config *cfg)
{
	(void)conn;
	cfg->kernel_cache = 1;
	cfg->hard_remove = 1;
	cfg->nullpath_ok = 1;
	cfg->remember = -1;
	return nullptr;
}

int FileSystemOptions::GetAttr(const std::string& path, struct stat* stbuf, fuse_file_info* fi)
{

	string buf;
    string key;
    if(fi)
    {
        key = Encode(fi->fh);
    }
    else
    {
        auto idx = GetIndex(path);
        if (idx.Bad())
            return -ENOENT;

        key = idx.Key();
    }
    Status s = db->Get(ReadOptions(), hAttr, key, &buf);
    CheckStatus(s);
	Attr attr{};
	if (!Attr::Decode(buf, &attr))
	{
		return -EIO;
	}
	attr.Fill(stbuf);
	return 0;
}

int FileSystemOptions::ReadDir(void* buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info* fi, enum fuse_readdir_flags flags)
{
	string encoded_inode = Encode(fi->fh);

	string encodedAttr;
	Status s = db->Get(ReadOptions(), hAttr, encoded_inode, &encodedAttr);
	Attr attr{};
	if(!Attr::Decode(encodedAttr,&attr))
	{
		return -EIO;
	}
	if(!S_ISDIR(attr.mode))
	{
		return -ENOTDIR;
	}
	constexpr auto zero = static_cast<fuse_fill_dir_flags>(0);
	filler(buf, ".", nullptr, 0, zero);
	filler(buf, "..", nullptr, 0, zero);
	unique_ptr<Iterator> itor(db->NewIterator(ReadOptions(), hIndex));

	for (itor->Seek(encoded_inode);itor->Valid() && itor->key().starts_with(encoded_inode);itor->Next())
	{
		constexpr auto offst = sizeof(uint64_t) + 1;
        
        const string filename = itor->key().ToString().substr(offst);
		string attrStr;
        const string fileinode = itor->value().ToString();
		s = db->Get(ReadOptions(), hAttr, fileinode, &attrStr);
		
		Attr fileattr{};
		struct stat stbuf {};
		if(s.ok() && Attr::Decode(attrStr, &fileattr))
		{
			fileattr.Fill(&stbuf);
			filler(buf, filename.data(), &stbuf, 0, zero);
		}
		else
		{
			filler(buf, filename.data(), nullptr, 0, zero);
		}
	}
	return 0;
}
bool Accessable(const Attr& attr, const unsigned mask)
{
	const auto euid = fuse_get_context()->uid;
	const auto egid = fuse_get_context()->gid;
	if (euid == 0)
		return true;
	unsigned flag;

	if (euid == attr.uid)
		flag = (attr.mode >> 6) & 7; //rwx------
	else if (egid == attr.gid)//同上
		flag = (attr.mode >> 3) & 7;//---rwx---	
	else
		flag = (attr.mode >> 0) & 7;//------rwx
	bool accessable = true;

	if (mask & R_OK)
		accessable = accessable && (flag & R_OK);
	if (mask & W_OK)
		accessable = accessable && (flag & W_OK);
	if (mask & X_OK)
		accessable = accessable && (flag & X_OK);

	return accessable;
}
int FileSystemOptions::Open(const std::string& path, fuse_file_info * fi)
{
	auto index = GetIndex(path);
	if(index.Bad())
	{
		return -ENOENT;
	}
	Txn txn{ db->BeginTransaction(WriteOptions()) };
	string attrbuf;
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, index.Key(), &attrbuf);
	CheckStatus(s);
	Attr attr{};
	Attr::Decode(attrbuf,&attr);
	if(S_ISDIR(attr.mode))
	{
		return -EISDIR;
	}

	unsigned open_flags = 0;
	switch(fi->flags)
	{
	case O_WRONLY:open_flags |= W_OK;break;
	case O_RDONLY:open_flags |= R_OK; break;
	case O_RDWR:open_flags |= (W_OK | R_OK); break;
	default: {}
	}

	if (!Accessable(attr, open_flags))
		return -EACCES;
	if (fi->flags & O_APPEND)
		fi->nonseekable = true;
	

	if ((fi->flags&O_ACCMODE) == O_WRONLY && !(fi->flags & O_APPEND))//如果不是追加(清零写)则需要修改文件属性
	{
		// merge
		attr.size = 0;
		attr.mtime = attr.ctime = Now();
		txn->Put(hAttr, index.Key(), attr.Encode());
		
		//TODO truncate的优化
		unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(), hData));
		
		//truncate
		for (itor->Seek(index.Key()); itor->Valid()&&itor->key().starts_with(index.Key()); itor->Next())
		{
			txn->Delete(itor->key());
		}
	}
	if (!txn->Commit().ok())
		return -EIO;

	fi->fh = index.inode;
	opening_files.Open(fi->fh);
	
	return 0;
}

int FileSystemOptions::Read(const std::string& path,char* buf, size_t size, off_t offset, fuse_file_info* fi)
{
	if (!fi)
		return -EIO;

	const string encoded_inode = Encode(fi->fh);	
	assert(encoded_inode != Encode(0)&&!encoded_inode.empty());
	Txn txn{ db->BeginTransaction(WriteOptions()) };

	string attr_buf, data;
	//锁文件,准备iterator
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, Encode(fi->fh), &attr_buf);


	Attr attr{};
	if(!Attr::Decode(attr_buf,&attr))
	{
		return -EIO;
	}
	if (offset >= attr.size)
		return 0;


	//TODO 可能的优化，考虑加入iterator_lower_bound和iterator_upper_bound
	std::unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(), hData));
	txn->UndoGetForUpdate(hAttr, Encode(fi->fh));
	//解锁

	//将size重置到可读范围
	const off_t file_size = attr.size;
	if (offset < file_size && offset + static_cast<off_t>(size) > file_size)
		size = file_size - offset;

	const auto read_size = size;
	while (size > 0)
	{
		assert(!encoded_inode.empty());
		PinnableSlice slice;
		auto s = txn->Get(ReadOptions(),hData, PageIndex(encoded_inode, offset), &slice);
		const size_t read_start = offset % kPageSize;
		const size_t read_end = std::min<size_t>(read_start + size, kPageSize);
		const size_t read_bytes = read_end - read_start;
		assert(!encoded_inode.empty());
		if (s.ok())//block存在
		{
			assert(!encoded_inode.empty());
			const int64_t zero_bytes = read_end - slice.size();

			assert(read_end <= kPageSize);
			if (read_start < slice.size())//起始处有slice数据
			{	
				assert(!encoded_inode.empty());
				if (zero_bytes <= 0)//全部在slice内读取
				{
					//|----------slice-----------|
					//start               slice.size()       4k
					//|__________________________|-----------|
					//      |-----size-----|
					//   read_start     read_end
					//      |--read_bytes--|
					memcpy(buf, slice.data() + offset % kPageSize, size);
					offset += read_bytes;
					buf += read_bytes;
					size -= read_bytes;
				}
				else//读取数据有一部分在slice之外(但小于4k)，zero_bytes > 0 
				{
					//|-------slice--------|
					//start               slice.size()              4k
					//|_______________________|---------------------|
					//      |--------------size------------------|
					//                        |---zero_bytes----|
					//      |------------read_bytes-------------|
					//   read_start                       read_end
					//      |-block_read_size-|
					assert(!encoded_inode.empty());
					const auto block_read_size = read_end - zero_bytes;
					memcpy(buf, slice.data() + read_start, block_read_size);
					buf += block_read_size;
					bzero(buf, zero_bytes);
					buf += zero_bytes;
					size -= read_bytes;
					offset += read_bytes;
				}
			}
			else//读取部分在块空洞中,offset%4k >= slize.size() 
			{
				assert(!encoded_inode.empty());
				//|-------Slice-------|
				//start        slice.size()       4k
				//|___________________|------------------|
				//                      |-----size-----|
				//                   offset%4k         |
				//                      |--read_bytes--|
				bzero(buf, read_bytes);
				offset += read_bytes;
				buf += read_bytes;
				size -= read_bytes;
			}
		}
		else//没有这个块，逻辑同上
		{
			assert(!encoded_inode.empty());
			const auto read_bytes = std::min<size_t>(kPageSize, offset%kPageSize + size) - offset % kPageSize;
			bzero(buf, read_bytes);

			offset += read_bytes;
			buf += read_bytes;
			size -= read_bytes;
		}
		assert(!encoded_inode.empty());
	}
	return static_cast<int>(read_size);
}
//TODO sticky
int FileSystemOptions::Create(const std::string& path, mode_t mode, fuse_file_info* fi)
{
	if (mode & (S_IFCHR | S_IFIFO | S_IFBLK))//不支持字符设备、管道以及块设备创建
		return -ENOTSUP;


	if (path == "/")
		return -EEXIST;


	Txn txn{ db->BeginTransaction(WriteOptions()) };
	auto parentIdx = GetIndexAndLock(fs::path(path).parent_path().generic_string(), txn);
	//父目录不存在
	if (parentIdx.Bad())
		return static_cast<int>(parentIdx.inode);//errno

	auto file_index = FileIndex(parentIdx.inode, fs::path(path).filename().generic_string());

	string par_attr_buf;
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, parentIdx.Key(), &par_attr_buf);
	CheckStatus(s);
	string ignore;
	s = txn->GetForUpdate(ReadOptions(), hIndex, file_index.Index(), &ignore);
	if (s.ok())//文件存在是否需要检测权限?
	{
		//Trunc file
		db->DeleteRange(WriteOptions(), hData, file_index.Index(), file_index.Index() + kMaxEncodedIdx);
	}
	else if (!s.IsNotFound())
		return -EIO;

	string parAttrBuf;
	txn->GetForUpdate(ReadOptions(), hAttr, parentIdx.Key(), &parAttrBuf);
	Attr parAttr{};
	if (!Attr::Decode(parAttrBuf, &parAttr))
	{
		return -EIO;
	}

	auto attr = Attr::CreateNormal();
	if (s.IsNotFound())
	{
		file_index.inode = inodeCounter++;
	}

	parAttr.atime = parAttr.ctime = Now();
	attr.mode = mode;
	attr.inode = file_index.inode;
	txn->Put(hIndex, file_index.Index(), file_index.Key());
	txn->Put(hAttr, file_index.Key(), attr.Encode());
	txn->Put(hAttr, parentIdx.Key(), parAttr.Encode());


	if (!txn->Commit().ok())
		return -EIO;

	fi->fh = file_index.inode;
	opening_files.Open(fi->fh);
    return 0;
}






//TODO delete 如果失败，那么可能会导致某些文件块泄露
//一个可行的办法是在将hDeleted中记录的key延后删除--仅当检测文件完全删除后才
//删除hDeleted
int FileSystemOptions::Release(struct fuse_file_info* fi)
{
    Txn txn{ db->BeginTransaction(WriteOptions()) };
    string attrBuf;
    txn->GetForUpdate(ReadOptions(), hAttr, Encode(fi->fh), &attrBuf);
    const bool all_closed = opening_files.Release(fi->fh);
    Attr attr{};
    if (all_closed && Attr::Decode(attrBuf, &attr))
    {
        if(attr.nlink == 0)
        {
			//TODO可以考虑这些删除动作异步执行,或者单独抽出删除data和attr的动作。
            txn->Delete(hIndex, Encode(fi->fh));
            txn->Delete(hDeleted, Encode(fi->fh));
            txn->Delete(hAttr, Encode(fi->fh));
			const auto encoded_inode = Encode(fi->fh);
			std::unique_ptr<Iterator> itor{ txn->GetIterator(ReadOptions(),hData) };
			for (itor->Seek(Encode(fi->fh)); itor->Valid() && itor->key().starts_with(encoded_inode);itor->Next())
			{
				txn->Delete(hData, itor->key());
			}
        }
    }

    if (txn->Commit().ok())
        return 0;
    return -EIO;
}
//TODO 检查readonly方式打开的文件能不能写。
int FileSystemOptions::Write(const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi)
{

	const auto write_bytes = size;//备份size,写入总数
	if (!fi)
		return -EIO;
	const string encoded_inode = Encode(fi->fh);
	Txn txn{ db->BeginTransaction(WriteOptions()) };
	string data_buf, arrt_buf;
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, encoded_inode, &arrt_buf);
	CheckStatus(s);

	Attr attr{};
	if (!Attr::Decode(arrt_buf, &attr))
	{
		return -EIO;
	}

	//TODO 检查一下这里的flags是否会被填充。
	if (fi->flags & O_APPEND)
		offset = attr.size;
	// page_start  write_start           write_end        page_end
	//  |__________|_________________________|________________|
	//             |<---block_write_bytes--->|
	//  or
	// page_start  write_start           write_end          page_end
	//  |__________|_________________________|________________|
	//             |<---block_write_bytes--->|
	// where: write_start = offset % 4k
	while (size > 0)
	{
		string data_block;
		auto block_index = PageIndex(fi->fh, offset);
		Status s = txn->GetForUpdate(ReadOptions(), hData, block_index, &data_block);
	
		const auto write_start = offset % kPageSize;
		const auto write_end = std::min<size_t>(write_start + size, kPageSize);
		const auto block_write_bytes = write_end - write_start;
		assert(block_write_bytes > 0 && block_write_bytes <= kPageSize);
		if (s.ok())//有这个块
		{
			if (data_block.size() < write_end)
				data_block.resize(write_end);//块不够大则扩容
		}
		else if (s.IsNotFound())
		{
			data_block = std::string(block_write_bytes, '\0');
			//TODO 这里可以维护block块数目。
		}
		else if (s.IsNoSpace())
		{
			return -ENOSPC;
		}
		else if (s.IsBusy())
		{
			return -EBUSY;
		}
		else
		{
			return -EIO;
		}
		//数据写入块
		assert(data_block.size() >= static_cast<size_t>(write_end));
		data_block.replace(write_start, block_write_bytes, buf);
		txn->Put(hData, block_index, data_block);
		buf += block_write_bytes;
		size -= block_write_bytes;
		offset += block_write_bytes;
	}

	attr.size = attr.size > offset ? attr.size : offset;//此时offset = offset(original) + size(original)
	attr.mtime = Now();

	txn->Put(hAttr, encoded_inode, attr.Encode());
	if (!txn->Commit().ok())
		return -EIO;

    return static_cast<int>(write_bytes);
}

//TODO 确定一下unlink是否需要检查对父目录的权限(FUSE会不会自动处理)
//TODO 检测sitcky位(限制仅能用户本人删除文件)
int rocksfs::FileSystemOptions::Unlink(const std::string& path)
{
    
    
    Txn txn{ db->BeginTransaction(WriteOptions()) }; 
	auto fidx = GetIndexAndLock(path,txn);//锁索引
    string attrBuf,parAttrBuf;
    txn->GetForUpdate(ReadOptions(), hAttr, Encode(fidx.parentInode), &parAttrBuf);
    txn->GetForUpdate(ReadOptions(), hAttr, fidx.Key(), &attrBuf);
    Attr attr{},parAttr{};
    if(!Attr::Decode(attrBuf,&attr))
    {
        return 0;
    }
    if(!Attr::Decode(parAttrBuf,&parAttr))
    {
        return 0;
    }
    if (S_ISDIR(attr.mode))
        return -EISDIR;

    attr.ctime = parAttr.mtime = parAttr.ctime = Now();
    txn->Delete(hIndex, fidx.Index());
    if(--attr.nlink == 0)
    {
		//由于上面已经将文件名锁住，因此，在本删除过程执行完之前，
		//其他线程无法打开待删除文件，所以不用加锁。
		if (opening_files.IsOpening(fidx.inode))//file is opening
        {
            txn->Put(hDeleted, fidx.Key(), "");
        }
        else
        {
            txn->Delete(hAttr, fidx.Key());
            //txn->Delete(hData, fidx.Key());
			//TODO 根据需求确定这里是否应该优化删除速度(阻塞？)
			unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(),hData));
			for (itor->Seek(fidx.Key()); itor->Valid() && itor->key().starts_with(fidx.Key()); itor->Next())
			{
				txn->Delete(hData, itor->key());
			}
        }
    }
    else//仍有硬链接
    {
        txn->Put(hAttr, fidx.Key(), attr.Encode());
    }
    txn->Put(hAttr, Encode(fidx.parentInode), parAttr.Encode());
    if (txn->Commit().ok())
        return 0;
    return -EIO;
}

int rocksfs::FileSystemOptions::OpenDir(const std::string& path, fuse_file_info* fi)
{
    auto idx = GetIndex(path);
    fi->fh = idx.inode;
    string attrBuf;
    Status s = db->Get(ReadOptions(),hAttr, idx.Key(), &attrBuf);
    CheckStatus(s);
    Attr attr{};
    if (!Attr::Decode(attrBuf, &attr))
    {
        return -EIO;
    }
   
    if (!Accessable(attr, R_OK))
        return -EACCES;

    return 0;
}
int FileSystemOptions::ReleaseDir(fuse_file_info* fi)
{
    //TODO for deleted dir.
    return 0;
}
int FileSystemOptions::Utimens(const std::string& path, const timespec tv[2], fuse_file_info* fi)
{
    string inode;
    if(fi)
        inode = Encode(fi->fh);
    else
    {
        auto idx = GetIndex(path);
        if (idx.Bad())
            return -ENOENT;
        inode = idx.Key();
    }


    Txn txn{ db->BeginTransaction(WriteOptions()) };
    string attr_buf;
    Status s = txn->GetForUpdate(ReadOptions(), hAttr, inode, &attr_buf);
    if (!s.ok())
        return -EIO;
    Attr attr{};
    if(!Attr::Decode(attr_buf,&attr))
    {
        return -EIO;
    }
    const auto now = Now();
    if (tv && tv[0].tv_nsec == UTIME_OMIT && tv[1].tv_nsec == UTIME_OMIT)//不修改
        return 0;
    //写权限检查
    if (!Accessable(attr, W_OK))
        return -EPERM;

    if(!tv)
    {
        attr.atime = attr.mtime = now;
    }
    else
    {
        if (tv[0].tv_nsec == UTIME_NOW)
            attr.atime = now;
        else if (tv[0].tv_nsec != UTIME_OMIT)
            attr.atime = tv[0];

        if (tv[1].tv_nsec == UTIME_NOW)
            attr.mtime = now;
        else if (tv[1].tv_nsec != UTIME_OMIT)
            attr.mtime = tv[1];
    }
    attr.ctime = now;
    txn->Put(hAttr, inode, attr.Encode());
    if (!txn->Commit().ok())
        return -EIO;
    return 0;
}

int FileSystemOptions::MkDir(const std::string& path, mode_t mode)
{
	fs::path dir_path(path);
	Txn txn{ db->BeginTransaction(WriteOptions()) };
	auto par_idx = GetIndexAndLock(dir_path.parent_path().generic_string(),txn);
	if (par_idx.Bad())
		return static_cast<int>(par_idx.inode);//errno

	auto file_idx = GetIndexAndLock(dir_path.generic_string(), txn);
	if (!file_idx.Bad())
		return -EEXIST;
	if (file_idx.inode != -ENOENT)
		return static_cast<int>(par_idx.inode);//errno

	string par_attr_buf;
	txn->GetForUpdate(ReadOptions(), hAttr, par_idx.Key(), &par_attr_buf);
	Attr par_attr{};
	Attr::Decode(par_attr_buf, &par_attr);
	if (!S_ISDIR(par_attr.mode))
		return -ENOTDIR;


	FileIndex dir_idx{ par_idx.inode,dir_path.filename().generic_string() };
	dir_idx.inode = inodeCounter++;
	Attr file_attr = Attr::CreateDir(dir_idx.inode);
	file_attr.inode = dir_idx.inode;
	par_attr.ctime = file_attr.ctime;
	par_attr.nlink += 1;

	txn->Put(hIndex, dir_idx.Index(), dir_idx.Key());
	txn->Put(hAttr, dir_idx.Key(), file_attr.Encode());
	txn->Put(hAttr, par_idx.Key(), par_attr.Encode());
	if (!txn->Commit().ok())
		return -EIO;

	return 0;
}


int FileSystemOptions::Rmdir(const std::string& path)
{
    //这样算unmount不？
    if (path == "/")
        return -EPERM;

    fs::path full_path(path);
    auto fidx = GetIndex(path);
    if (fidx.Bad())
        return -ENOENT;
    Txn txn{ db->BeginTransaction(WriteOptions()) };
    string parAttrBuf;

    txn->GetForUpdate(ReadOptions(), hAttr, Encode(fidx.parentInode), &parAttrBuf);//锁
    Attr parAttr{};
    Attr::Decode(parAttrBuf, &parAttr);
    if (!Accessable(parAttr , W_OK | X_OK))
        return -EPERM;
    string ignore;
    txn->GetForUpdate(ReadOptions(), hIndex, fidx.Index(), &ignore);//锁
    
    unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(),hIndex));
    itor->Seek(fidx.Key());//以本目录inode开始的文件一定是目录下的文件
    if (itor->Valid() && itor->key().starts_with(fidx.Key()))//目录不空
        return -ENOTEMPTY;
    parAttr.mtime = parAttr.ctime = Now();
    txn->Delete(hIndex, fidx.Index());
    txn->Delete(hAttr, fidx.Key());
    txn->Put(hAttr, Encode(fidx.parentInode), parAttr.Encode());
    if (!txn->Commit().ok())
        return -EIO;
    return 0;
}
//首先，两个文件名都不解引用
//
// 如果oldpath为目录，那么newpath不能是目录。
// 如果oldpath是目录，那么重命名这个目录。此时必须保证newpath不存在或者为空目录，否则出错。
// 如果oldpath是一个目录，newpath不能starts with oldpath
// 权限:
// newname如果存在，那么必须对newname有写权限(同delete)。
// 需要对oldname和newname以及包含二者的目录有写和执行权限。
// 调用进程需要对包含oldname和newname的目录具有写权限
// 二者都必须在统一文件系统
int FileSystemOptions::Rename(const std::string& oldpath, const std::string& newpath, unsigned flags)
{
	[[gnu::unused]]
	constexpr auto RENAME_EXCHANGE = (1 << 1);        //如果两者都存在，则交换
	if (oldpath == newpath)
		return 0;
	if (StartsWith(newpath, oldpath) || newpath == "/")
		return -EPERM;

	//可能死锁,因此添加死锁检测
	TransactionOptions op;
	op.deadlock_detect = true;
	Txn txn{ db->BeginTransaction(WriteOptions(), op) };
	
	fs::path src_path(oldpath), dest_path(newpath);
	fs::path src_par_path(src_path.parent_path()), dest_par_path(dest_path.parent_path());
 
	auto src_par_idx = GetIndexAndLock(src_par_path.generic_string(), txn);
	if (src_par_idx.Bad())
		return static_cast<int>(src_par_idx.inode);//errno

	auto src_idx = GetIndexAndLock(oldpath, txn);
	if (src_idx.Bad())
		return static_cast<int>(src_idx.inode);//errno
	//锁定目标的父目录
	auto dest_par_idx = GetIndexAndLock(fs::path(newpath).parent_path().generic_string(), txn);
	if (dest_par_idx.Bad())
		return static_cast<int>(dest_par_idx.inode);//errno

	//对目标目录需要有写和执行权限
	string src_par_attr_buf, dest_par_attr_buf;
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, src_idx.Key(), &src_par_attr_buf);
	CheckStatus(s);
	Attr src_par_attr{}, dest_par_attr{};
	Attr::Decode(src_par_attr_buf, &src_par_attr);
	if (!Accessable(src_par_attr, W_OK | X_OK))
		return -EACCES;

	s = txn->GetForUpdate(ReadOptions(), hAttr, dest_par_idx.Key(), &dest_par_attr_buf);
	CheckStatus(s);
	Attr::Decode(dest_par_attr_buf, &dest_par_attr);
	if (!Accessable(dest_par_attr, W_OK | X_OK))
		return -EACCES;
	

	//源文件
	string src_attr_buf;
	s = txn->GetForUpdate(ReadOptions(), hAttr, src_idx.Key(), &src_attr_buf);
	CheckStatus(s);
	Attr src_attr{};
	Attr::Decode(src_attr_buf, &src_attr);
	auto dest_idx = GetIndexAndLock(newpath, txn);//尝试锁定目标

	
	if (flags & RENAME_EXCHANGE)
	{
		if (dest_idx.inode == -ENOENT)
			return -ENOENT;//RENAME_EXCHANGE保证必须交换，并且二者同时存在
		
		src_par_attr.ctime = src_par_attr.mtime = dest_par_attr.ctime = dest_par_attr.mtime = Now();
		txn->Put(hAttr, src_par_idx.Key(), src_par_attr.Encode());
		txn->Put(hAttr, src_par_idx.Key(), dest_par_attr.Encode());
		//交换inode
		txn->Put(hIndex, src_idx.Index(), dest_idx.Key());
		txn->Put(hIndex, dest_idx.Index(), src_idx.Key());
		if (!txn->Commit().ok())
			return -EIO;
		return 0;
	}
	
	if(S_ISDIR(src_attr.mode))//目录
	{
		//目标必须不存在或者是空目录
		if(!dest_idx.Bad())//目标存在,dest_idx已经初始化
		{
			if (dest_idx.inode != -ENOENT)
				return static_cast<int>(dest_idx.inode);//errno


			string dest_attr_buf;
			s = txn->GetForUpdate(ReadOptions(),hAttr, dest_idx.Key(), &dest_attr_buf);
			Attr dest_attr{};
			Attr::Decode(dest_attr_buf, &dest_attr);
			if(S_ISDIR(dest_attr.mode))
			{
				unique_ptr<Iterator> itor (txn->GetIterator(ReadOptions(), hIndex));
				itor->Seek(dest_idx.Key());
				if (itor->Valid() && itor->key().starts_with(dest_idx.Key()))
					return -ENOTEMPTY;
				//空目录,直接覆盖(将目标指回这里)
				dest_idx.inode = src_idx.inode;

			}
			else//目标不是目录
			{
				return -ENOTDIR;
			}
		}
		else//目标不存在
		{
			if (dest_idx.inode != ENOENT)//目标锁定失败
				return static_cast<int>(dest_idx.inode);

			dest_idx.parentInode = dest_par_idx.inode;
			dest_idx.inode = src_idx.inode;
			dest_idx.filename = dest_path.filename().generic_string();
			txn->Put(hIndex, dest_idx.Index(), dest_idx.Key());
		}
		//减少源目录的引用
		src_par_attr.nlink -= 1;
	}
	else//文件
	{
		if(!dest_idx.Bad())//目标存在,dest_idx已经初始化
		{
			if (dest_idx.inode != -ENOENT)
				return static_cast<int>(dest_idx.inode);

			string dest_attr_buf;
			s = txn->GetForUpdate(ReadOptions(), hAttr, dest_idx.Key(), &dest_attr_buf);
			Attr dest_attr{};
			Attr::Decode(dest_attr_buf, &dest_attr);
			if (S_ISDIR(dest_attr.mode))//目标是目录
				return -EISDIR;

			--dest_attr.nlink;//删除目标
			dest_attr.ctime = Now();
			txn->Put(hAttr, dest_idx.Key(), dest_attr.Encode());
			if (dest_attr.nlink == 0)
				txn->Put(hDeleted, dest_idx.Key(), "");

			dest_idx.inode = src_idx.inode;
		}
		else
		{
			if (dest_idx.inode != -ENOENT)
				return static_cast<int>(dest_idx.inode);
			dest_idx.parentInode = dest_par_idx.inode;
			dest_idx.inode = src_idx.inode;
			dest_idx.filename = dest_path.filename().generic_string();
		}
	}
	src_attr.ctime = src_par_attr.ctime = dest_par_attr.ctime = Now();
	txn->Put(hIndex, dest_idx.Index(), src_idx.Key());
	//写入新目录
	//更新两者父目录的time
	//更新二者的time
	
	txn->Put(hAttr, src_par_idx.Key(), src_par_attr.Encode());
	txn->Put(hAttr, src_idx.Key(), src_attr.Encode());
	txn->Put(hAttr, dest_par_idx.Key(), dest_par_attr.Encode());
	txn->Delete(hIndex, src_idx.Index());//删除源目录入口

	if (!txn->Commit().ok())
		return -EIO;
	return 0;
}
int rocksfs::FileSystemOptions::Truncate(off_t offset, fuse_file_info * fi)
{
	if (!fi)
		return -EIO;//文件没有打开
	Txn txn{ db->BeginTransaction(WriteOptions()) };
	const auto encoded_inode = Encode(fi->fh);
	string attr_buf;
	txn->GetForUpdate(ReadOptions(), hAttr, encoded_inode, &attr_buf);
	Attr attr;
	const bool state = Attr::Decode(attr_buf, &attr);
	assert(state);
	if (offset < attr.size)//扩容不用管。
	{
		unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(), hData));
		itor->Seek(PageIndex(encoded_inode, offset));
		assert(itor->Valid());
		string block = itor->value().ToString();
		block.resize(offset%kPageSize);

		txn->Put(hData, itor->key(), block);
		for (itor->Next(); itor->Valid() && itor->key().starts_with(encoded_inode); itor->Next())
		{
			txn->Delete(hData, itor->key());
		}
	}
	attr.mtime = Now();
	attr.size = offset;
	txn->Put(hAttr, encoded_inode, attr.Encode());
	if (!txn->Commit().ok())
		return -EIO;

	return 0;
}
//TODO 父目录的时间更新(更新哪些时间)
//权限说明:
//To create the hard - link alice will need write + execute permissions on target - dir on all cases.
//The permissions needed on target.txt will vary :
//If fs.protected_hardlinks = 1 then alice needs either ownership of target.txt or at least read + write permissions on it.
//If fs.protected_hardlinks = 0 then any set of permissions will do; Even 000 is okay.

//https://unix.stackexchange.com/questions/233275/hard-link-creation-permissions
//Hardlinks:

//On systems that have user - writable directories on the same partition as system files, 
//a long - standing class of security issues is the hardlink - based time - of - check - time - of - use race, 
//most commonly seen in world - writable directories like / tmp.
//The common method of exploitation of this flaw is to cross privilege boundaries 
//when following a given hardlink(i.e.a root process follows a hardlink created by another user).
//Additionally, an issue exists where users can "pin" a potentially vulnerable setuid / setgid file 
//so that an administrator will not actually upgrade a system fully.

//The solution is to permit hardlinks to only be created when the user is already the existing file's owner, 
//or if they already have read/write access to the existing file.
//经测试,fuse会自动拦截对于目录建立硬链接
int rocksfs::FileSystemOptions::Link(const std::string& src_path, const std::string& newpath)
{


	const auto src_par_path = fs::path(src_path).parent_path().generic_string();
	const auto new_parent_path = fs::path(newpath).parent_path().generic_string();
	//TODO 将四个文件名排序，防止死锁，目前仅仅使用deadlock_detect
	TransactionOptions op;
	op.deadlock_detect = true;
	Txn txn{ db->BeginTransaction(WriteOptions(), op) };
	auto src_par_idx = GetIndexAndLock(src_par_path, txn);//锁旧父目录索引
	if(src_par_idx.Bad())
		return static_cast<int>(src_par_idx.inode);//errno

	auto src_idx = GetIndexAndLock(src_path, txn);//锁原目标索引
	if (src_idx.Bad())
		return static_cast<int>(src_idx.inode);//errno
	string buf;
	txn->GetForUpdate(ReadOptions(), hAttr, src_idx.Key(), &buf);
	Attr attr{};
	Attr::Decode(buf, &attr);
	if (S_ISDIR(attr.mode))//硬链接不能是目录
		return -EISDIR;
	//TODO 权限检测:文件的所有者，或者是对于文件有读写权限，
	//并且需要对于源目录和目标目录有写和执行权限。


	auto dest_par_idx = GetIndexAndLock(new_parent_path, txn);//锁目标目录索引
	if (dest_par_idx.Bad())
		return static_cast<int>(dest_par_idx.inode);//errno
	FileIndex newpath_idx(dest_par_idx.inode, fs::path(newpath).filename().generic_string());
	string ignore;
	Status s = txn->GetForUpdate(ReadOptions(), this->hIndex, newpath_idx.Index(), &ignore);//尝试锁目标索引
	if (!s.IsNotFound())
	{
		if (s.ok())
			return -EEXIST;
		if (s.IsBusy())
			return -EBUSY;
		return -EIO;
	}
	attr.nlink += 1;
	attr.ctime = Now();
	s = txn->Put(hAttr, src_idx.Key(), attr.Encode());//增加文件nlink数目
	CheckStatus(s);
	newpath_idx.inode = src_idx.inode;
	s = txn->Put(hIndex, newpath_idx.Index(), newpath_idx.Key());//写入文件
	CheckStatus(s);

	//更新源目录时间
	s = txn->GetForUpdate(ReadOptions(), hAttr, src_par_idx.Key(), &buf);
	bool decode_ok = Attr::Decode(buf, &attr);
	assert(decode_ok);
	attr.mtime = attr.ctime = Now();
	s = txn->Put(hAttr, src_par_idx.Key(), attr.Encode());
	CheckStatus(s);

	//更新目标目录时间(和源目录可能是同一个，所以必须分开处理)
	s = txn->GetForUpdate(ReadOptions(), hAttr,dest_par_idx.Key(), &buf);
	decode_ok = Attr::Decode(buf, &attr);
	assert(decode_ok);
	attr.mtime = attr.ctime = Now();
	s = txn->Put(hAttr, dest_par_idx.Key(), attr.Encode());
	CheckStatus(s);


	FileIndex new_idx(dest_par_idx.inode, fs::path(newpath).filename().generic_string());
	s = txn->Put(hIndex, new_idx.Index(), Encode(src_idx.inode));//添加文件名
	s = txn->Commit();
	if (!s.ok())
		return -EIO;

	return 0;
}

int FileSystemOptions::Flush(fuse_file_info* fi)
{
	return 0;
}

int FileSystemOptions::Chmod(const std::string& path, mode_t mode, fuse_file_info* fi)
{
	Txn txn(db->BeginTransaction(WriteOptions()));
	string attr_buf;
	string encoded_inode;
	if(!fi)
	{
		auto idx = GetIndexAndLock(path, txn);
		if (idx.Bad())
			return static_cast<int>(idx.inode);
		encoded_inode = idx.Key();
		txn->GetForUpdate(ReadOptions(), hAttr, idx.Key(),&attr_buf);
	}
	else
	{
		encoded_inode = Encode(fi->fh);
		txn->GetForUpdate(ReadOptions(), hAttr, encoded_inode, &attr_buf);
	}
	//TODO for symlnk
	Attr attr;
	if (!Attr::Decode(attr_buf, &attr))
		return -EIO;
	
	if (fuse_get_context()->gid != 0 && attr.uid != fuse_get_context()->gid)
		return -EPERM;
	
	attr.mode = mode;
	attr.ctime = Now();
	txn->Put(hAttr, encoded_inode, attr.Encode());
	if (!txn->Commit().ok())
		return -EIO;
	return 0;
}

int rocksfs::FileSystemOptions::Chown(const std::string& path, uid_t uid, gid_t gid, fuse_file_info * fi)
{
	Txn txn(db->BeginTransaction(WriteOptions()));
	string attr_buf;
	string encoded_inode;
	if (!fi)
	{
		auto idx = GetIndexAndLock(path, txn);
		if (idx.Bad())
			return static_cast<int>(idx.inode);
		encoded_inode = idx.Key();
		txn->GetForUpdate(ReadOptions(), hAttr, idx.Key(), &attr_buf);
	}
	else
	{
		encoded_inode = Encode(fi->fh);
		txn->GetForUpdate(ReadOptions(), hAttr, encoded_inode, &attr_buf);
	}
	//TODO for symlnk
	Attr attr;
	if (!Attr::Decode(attr_buf, &attr))
		return -EIO;
	if (fuse_get_context()->gid != 0 && attr.uid != fuse_get_context()->gid)
		return -EPERM;

	attr.uid = (uid == static_cast<uid_t>(-1) ? attr.uid : uid);
	attr.gid = (gid == static_cast<uid_t>(-1) ? attr.gid : gid);
	attr.ctime = Now();
	txn->Put(hAttr, encoded_inode, attr.Encode());
	if (!txn->Commit().ok())
		return -EIO;
	return 0;
}
#ifdef HAVE_SETXATTR
//TODO 权限检测
int rocksfs::FileSystemOptions::SetXattr(const std::string& path, const std::string& name, const void* value, size_t size, int flags)
{

	Txn txn;
	auto fidx = GetIndexAndLock(path, txn);
	if (fidx.Bad())
		return fidx.inode;
	Slice xattr_val(static_cast<const char*>(value), size);
	//TODO 权限检测
	txn->Put(hAttr, fidx.Key() + name, xattr_val);
	if (!txn->Commit().ok())
		return -EIO;

	return 0;
}

int rocksfs::FileSystemOptions::GetXattr(const std::string& path, const std::string& name, char* buf, size_t size)
{
	auto fidx = GetIndex(path);
	if (fidx.Bad())
		return fidx.inode;
	PinnableSlice slice;
	Status s = db->Get(ReadOptions(), hAttr, fidx.Key() + name, &slice);
	CheckStatus(s);
	if (size)
		memcpy(buf, slice.data(), std::min(size, slice.size()));

	return 0;
}
//TODO 权限检测
int rocksfs::FileSystemOptions::ListXattr(const std::string& path, char* buf, size_t size)
{

	auto idx = GetIndex(path);
	if (idx.Bad())
		return idx.inode;
	unique_ptr<Iterator> itor(db->NewIterator(ReadOptions(), hAttr));
	itor->Seek(path);
	const auto size_orig = size;
	for (itor->Next(); itor->key().starts_with(idx.Key()); itor->Next())
	{
		if (itor->value().size() >= size)
			return -ERANGE;
		memcpy(buf, itor->key().data(), itor->key().size());
		buf += itor->key().size();
		*buf = '\0';
		buf += 1;
		size -= itor->key().size() + 1;
	}
	return size_orig - size;
}
//TODO 权限检测
int rocksfs::FileSystemOptions::RemoveXattr(const std::string& path, const std::string & name)
{

	auto idx = GetIndex(path);
	if (idx.Bad())
		return idx.inode;
	unique_ptr<Iterator> itor(db->NewIterator(ReadOptions(), hAttr));
	PinnableSlice slice;
	Txn txn;
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, idx.Key() + name, &slice);
	if (s.IsNotFound())
		return -ENOATTR;
	txn->Delete(hAttr, idx.Key() + name);
	txn->Commit();
	return 0;
}
#endif // HAVE_SETXATTR



