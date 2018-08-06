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
#include "fuse_options.hpp"
#include "util.hpp"
#include "attr.hpp"
struct Guard;
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




rocksfs::RocksFs::~RocksFs()
{
	db->DestroyColumnFamilyHandle(hIndex);
	db->DestroyColumnFamilyHandle(hData);
	db->DestroyColumnFamilyHandle(hAttr);
	db->DestroyColumnFamilyHandle(hDeleted);
	delete db;
}

RocksFs::RocksFs(const std::string& path)
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
		inodeCounter = __bswap_64(counter) + 1;
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

void* RocksFs::Init(fuse_conn_info* conn, fuse_config *cfg)
{
	(void)conn;
	cfg->kernel_cache = 1;
	cfg->hard_remove = 1;
	cfg->nullpath_ok = 1;
	cfg->remember = -1;
	return nullptr;
}

int RocksFs::GetAttr(const std::string& path, struct stat* stbuf, fuse_file_info* fi)
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

int RocksFs::ReadDir(void* buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info* fi, enum fuse_readdir_flags flags)
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
int RocksFs::Open(const std::string& path, fuse_file_info * fi)
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
	if ((fi->flags&O_ACCMODE) == O_RDWR)//现在不允许rw方式打开，只允许追加
		return -EPERM;//操作不允许

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

	if ((fi->flags&O_ACCMODE) != O_RDONLY)
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
		txn->Put(hData, PageIndex(index.Key(),0), "");

	}
	if (!txn->Commit().ok())
		return -EIO;

	fi->fh = index.inode;
	opening_files.Open(fi->fh);

	return 0;
}

int RocksFs::Read(char * buf, size_t size, off_t offset, fuse_file_info * fi)
{
	if (!fi)
		return -EIO;

	string idx = Encode(fi->fh);


	
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
		size = file_size - offset;//offset > size ?
	const auto read_size = size;
	itor->Seek(PageIndex(fi->fh, offset));
	{
		//从offset开始，第一块剩余数据长度
		const auto first_page_left = (offset / kPageSize + 1)*kPageSize - offset;
		Slice page = itor->value();
		memcpy(buf, page.data(), first_page_left);
		buf += first_page_left;
		size -= first_page_left;
		itor->Next();
	}
	while (static_cast<ssize_t>(size )> 0)
	{
		assert(itor->Valid());
		Slice page = itor->value();
		if (size > kPageSize)
		{
			memcpy(buf, page.data(), kPageSize);
			buf += kPageSize;
			size -= kPageSize;
		}
		else
		{
			memcpy(buf, page.data(), size);
			break;
		}
		itor->Next();
	}
	return static_cast<int>(read_size);
}

int RocksFs::Create(const std::string& path, mode_t mode, fuse_file_info* fi)
{
	if (path == "/")
		return -EEXIST;


	Txn txn{ db->BeginTransaction(WriteOptions()) };
	auto parentIdx = GetIndexAndLock(fs::path(path).parent_path().generic_string(), txn);
	//父目录不存在
	if (parentIdx.Bad())
		return static_cast<int>(parentIdx.inode);//errno

	auto fIdx = FileIndex(parentIdx.inode, fs::path(path).filename().generic_string());
	
	string par_attr_buf;
	Status s = txn->GetForUpdate(ReadOptions(), hAttr, parentIdx.Key(), &par_attr_buf);
	CheckStatus(s);
	string ignore;
	s = txn->GetForUpdate(ReadOptions(), hIndex, fIdx.Index(), &ignore);
	if (s.ok())
		return -EEXIST;
	if (!s.IsNotFound())
		return -EIO;
	
	string parAttrBuf;
	txn->GetForUpdate(ReadOptions(), hAttr, parentIdx.Key(), &parAttrBuf);
	Attr parAttr{};
	if (!Attr::Decode(parAttrBuf, &parAttr))
	{
		return -EIO;
	}
	fIdx.inode = inodeCounter++;
	parAttr.atime = parAttr.ctime = Now();
	auto attr = Attr::CreateNormal();
	attr.inode = fIdx.inode;
	txn->Put(hIndex, fIdx.Index(), fIdx.Key());
	txn->Put(hAttr, fIdx.Key(), attr.Encode());
	txn->Put(hAttr, parentIdx.Key(), parAttr.Encode());
	txn->Put(hData, PageIndex(fIdx.inode,0), "");

	if (!txn->Commit().ok())
		return -EIO;

	fi->fh = fIdx.inode;
	opening_files.Open(fi->fh);
    return 0;
}

FileIndex RocksFs::GetIndex(const std::string& path)
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
		if(!s.ok())
		{
			idx.filename.clear();
			if (s.IsNotFound())
				idx.inode = -ENOENT;
			else
				idx.inode = -EIO;
			return idx;
		}

        idx.inode = __bswap_64(*reinterpret_cast<const int64_t*>(encodedIdx.c_str()));
    }
    return idx;
}

FileIndex RocksFs::GetIndexAndLock(const std::string & path, std::unique_ptr<rocksdb::Transaction>& txn)
{
	if(path == "/")
	{
		FileIndex res(0, "/");
		res.inode = 0;
		return res;
	}
	fs::path file_path(path);
	auto parentIdx = GetIndex(file_path.parent_path().generic_string());
	if(parentIdx.Bad())
	{
		FileIndex res{ 0,"" };
		res.inode = parentIdx.inode;
		return res;
	}
	string encoded_idx;
	FileIndex res{ parentIdx.inode,file_path.filename().generic_string() };
	Status s = txn->GetForUpdate(ReadOptions(),hIndex, res.Index(), &encoded_idx);
	if(!s.ok())
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
	res.inode = __bswap_64(*reinterpret_cast<const int64_t*>(encoded_idx.c_str()));
	return res;
}




//TODO delete 如果失败，那么可能会导致某些文件块泄露
//一个可行的办法是在将hDeleted中记录的key延后删除--仅当检测文件完全删除后才
//删除hDeleted
int RocksFs::Release(struct fuse_file_info* fi)
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
int RocksFs::Write(const char *buf, std::size_t size, off_t offset, struct fuse_file_info *fi)
{

	const auto write_bits = size;//备份size,写入总数
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
        

	//尝试将最后一个块填满
	{
		//itor生命周期
		unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(), hData));
		itor->SeekForPrev(Encode(fi->fh)+kMaxEncodedIdx);//最后一个key,##注意##这样没办法延迟删除


#ifndef NDEBUG
		const string key = itor->key().ToString();
		const string value = itor->key().ToString();
		const auto it = itor.get();
#endif // !NDEBUG


		const auto last_blk_left = 
			std::min<ssize_t>(kPageSize - itor->value().size(),size);//最后一个块剩余空间
		if (last_blk_left > 0)
		{
			auto blk = itor->value().ToString();
			blk.append(buf, last_blk_left);
			txn->Put(hData, itor->key(), blk);
			size -= last_blk_left;
			buf += last_blk_left;
			attr.size += last_blk_left;
			assert(blk.size() <= kPageSize);
		}
		
	}

	while (static_cast<ssize_t>( size > 0))
	{
		const auto bytes = std::min<int64_t>(size, kPageSize);//本次循环写入的字节数
		string blk{ buf,static_cast<uint>(bytes) };
		s = txn->Put(hData, encoded_inode + Encode(attr.size / kPageSize + 1), blk);
		attr.size += bytes;
		size -= bytes;
		buf += bytes;
		if (!s.ok())
			return -EIO;
		assert(blk.size() <= kPageSize);
	}
	attr.mtime = Now();

	txn->Put(hAttr, encoded_inode, attr.Encode());
	if (!txn->Commit().ok())
		return -EIO;

    return static_cast<int>(write_bits);
}

//TODO 确定一下unlink是否需要检查对父目录的权限(FUSE会不会自动处理)
//TODO 检测sitcky位(限制仅能用户本人删除文件)
int rocksfs::RocksFs::Unlink(const std::string& path)
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

int rocksfs::RocksFs::OpenDir(const std::string& path, fuse_file_info* fi)
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
int RocksFs::ReleaseDir(fuse_file_info* fi)
{
    //TODO for deleted dir.
    return 0;
}
int RocksFs::Utimens(const std::string& path, const timespec tv[2], fuse_file_info* fi)
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

int RocksFs::MkDir(const std::string& path, mode_t mode)
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


int RocksFs::Rmdir(const std::string& path)
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
int RocksFs::Rename(const std::string& oldpath, const std::string& newpath, unsigned flags)
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
int rocksfs::RocksFs::Truncate(off_t offset, fuse_file_info * fi)
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
	if (offset < attr.size)//扩容
	{
		//注:这里的最后一个key是可以通过attr.size计算的，这里懒得算了
		unique_ptr<Iterator> itor(txn->GetIterator(ReadOptions(), hData));

		ssize_t left_bytes = offset - attr.size;//待写入字符数
		itor->SeekForPrev(encoded_inode + kMaxEncodedIdx);
		assert(itor->Valid());
		string blk = itor->value().ToString();
		const ssize_t blk_size_orig = blk.size();//当前文件最末块的size
		blk.resize(std::min<size_t>(kPageSize,left_bytes));
		txn->Put(hData, itor->key(), blk);
		left_bytes -= blk.size() - blk_size_orig;
		assert(blk.size() <= kPageSize);

		uint64_t page_index;
		memcpy(&page_index, itor->key().data() + sizeof(int64_t), sizeof(int64_t));
		page_index += 1;//待写入page的index
		while(left_bytes > 0)
		{
			const auto this_page_size = std::min<ssize_t>(left_bytes, kPageSize);
			const string val = string(this_page_size, '\0');
			txn->Put(hData, PageIndex(encoded_inode, page_index), val);
			page_index -= this_page_size;
			page_index += 1;
			assert(val.size() <= kPageSize);
		}
	}
	else//缩小
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
int rocksfs::RocksFs::Link(const std::string& src_path, const std::string& newpath)
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
