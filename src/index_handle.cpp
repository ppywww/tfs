#include "common.h"
#include "index_handle.h"
#include <sstream>

static int debug = 1;

using namespace std;

namespace conway
{
	namespace largefile
	{
		IndexHandle::IndexHandle(const string& base_path, const uint32_t main_block_id)
		{
			//creat file_op_ handle object
			stringstream tmp_stream;
			tmp_stream << base_path << INDEX_DIR_PREFIX  << main_block_id;  // /root/martin/index/1
			
			string index_path;
			tmp_stream >> index_path;
			
			file_op_ = new MMapFileOperation(index_path, O_RDWR | O_LARGEFILE | O_CREAT);
			is_load_ = false;
		}
	
		IndexHandle::~IndexHandle()
		{
			if(file_op_)
			{
				delete file_op_;
				file_op_ = NULL;
			}
		}
		
		int IndexHandle::create(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption& map_option)
		{
			int ret = TFS_SUCCESS;
			
			if(debug) printf("create index, block id：%u, bucket size：%d, max_mmap_size：%d, \
				first mmap size：%d, per mmap size：%d\n", logic_block_id, bucket_size, 
				map_option.max_mmap_size_, map_option.first_mmap_size_, map_option.per_mmap_size_);

			if(is_load_)
			{
				return EXIT_INDEX_ALREADY_LOADED_ERROR;
			}
			
			int64_t file_size = file_op_->get_file_size();    //获取文件大小
			if(file_size < 0)
			{
				return TFS_ERROR;
			}
			else if(file_size == 0)  //empty file
			{
				IndexHeader i_header;
				i_header.block_info.block_id_ = logic_block_id;
				i_header.block_info.seq_no_ = 1;
				i_header.bucket_size_ = bucket_size;
				i_header.index_file_size_ = sizeof(IndexHeader) + bucket_size * sizeof(int32_t);
				
				//index header + total buckets
				char* init_data = new char[i_header.index_file_size_];         //free
				memcpy(init_data, &i_header, sizeof(IndexHeader));
				memset(init_data + sizeof(IndexHeader), 0, i_header.index_file_size_ - sizeof(IndexHeader));
				
				//wirte IndexHeader and buckets into index file
				ret = file_op_->pwrite_file(init_data, i_header.index_file_size_, 0);

				delete[] init_data;
				init_data = NULL;

				// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
				// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
				// 因此检查条件应从 != TFS_SUCCESS 改为 != i_header.index_file_size_
				if(ret != i_header.index_file_size_)
				{
					// 如果返回值是负数（错误码）或不完整写入，返回 TFS_ERROR
					return TFS_ERROR;
				}
				
				ret = file_op_->flush_file();
				
				if(ret != TFS_SUCCESS)
				{
					return ret;
				}
			}
			else    //file_size > 0, index file already exist
			{		
				// 打印错误信息
				fprintf(stderr, "Index file already exist. block id：%u\n", logic_block_id);
				return EXIT_META_UNEXPECT_FOUND_ERROR;
			}
			
			ret = file_op_->mmap_file(map_option);           //mmap_file
			if(ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			is_load_ = true;
			
			if(debug) printf("init block id：%d index successful. data file size：%d, index file size：%d, bucket_size：%d, \
				free head offset：%d, seq_no：%d, size：%d, file count：%d, del_size：%d, del_file_count：%d, version：%d\n", 
				logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_, index_header()->bucket_size_, 
				index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_t_, block_info()->file_count_, 
				block_info()-> del_size_, block_info()->del_file_count_, block_info()->version_);
			
			return TFS_SUCCESS;
		}
		
		int IndexHandle::load(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption& map_option)
		{
			int ret = TFS_SUCCESS;
			
			if(is_load_)
			{
				return EXIT_INDEX_ALREADY_LOADED_ERROR;
			}
			
			int64_t file_size = file_op_->get_file_size();
			if(file_size < 0)
			{
				return file_size;
			}
			else if(file_size == 0)  //empty file
			{
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			MMapOption tmp_map_option = map_option;
			
			if(file_size > tmp_map_option.first_mmap_size_ && file_size <= tmp_map_option.max_mmap_size_)
			{
				tmp_map_option.first_mmap_size_ = file_size;
			}
			
			ret = file_op_->mmap_file(tmp_map_option);               //mmap_file
			
			if(ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			//int32_t bct_size = bucket_size();
			if(0 == bct_size() || 0 == block_info()->block_id_)
			{
				fprintf(stderr, "Index corrupt error. block id：%u, bucket size：%d\n", block_info()->block_id_, bct_size());
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			//check file size
			int32_t index_file_size = sizeof(IndexHeader) + bct_size() * sizeof(int32_t);
			
			if(file_size < index_file_size)
			{
				fprintf(stderr, "Index corrupt error, block id：%u, bucket size：%d, file size：%" __PRI64_PREFIX"d, index file size：%d\n", 
					block_info()->block_id_, bct_size(), file_size, index_file_size);
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			//check block id
			if(logic_block_id != block_info()->block_id_)
			{
				fprintf(stderr, "block id conflict. block id ：%u, index block id：%u\n", logic_block_id, block_info()->block_id_);
				return EXIT_BLOCKID_CONFLICT_ERROR;
			}
			
			//check bucket size
			if(bucket_size != bct_size())
			{
				fprintf(stderr, "Index configure error, old bucket size：%d, new bucket size：%d", bucket_size, bct_size());
				return EXIT_BUCKET_CONFIGURE_ERROR;
			}
			
			is_load_ = true;
			
			if(debug) printf("load block id：%d index successful. data file size：%d, index file size：%d, bucket_size：%d, \
				free head offset：%d, seq_no：%d, size：%d, file count：%d, del_size：%d, del_file_count：%d, version：%d\n", 
				logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_, index_header()->bucket_size_, 
				index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_t_, block_info()->file_count_, 
				block_info()-> del_size_, block_info()->del_file_count_, block_info()->version_);
			if(debug) 
{ 
    // 打印桶的数量
    printf("Bucket count: %d\n", bct_size());
    
    // 遍历每个桶
    for(int i = 0; i <bct_size(); ++i) 
    { 
        int32_t slot_offset = bucket_slot()[i];
        if (slot_offset > 0) {
            // 读取桶的首节点元数据
            MetaInfo meta_info;
            int32_t ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), slot_offset);
            // 修复bug：pread_file 返回实际读取的字节数，不再是 TFS_SUCCESS
            // 修改原因：pread_file 返回实际读取的字节数，应检查是否等于 sizeof(MetaInfo)
            if (ret == sizeof(MetaInfo)) {
                printf("Bucket %d, first node offset: %d, file id: %ld, size: %d, next offset: %d\n", 
                       i, slot_offset, meta_info.get_file_id(), meta_info.get_size(), meta_info.get_next_meta_offset());
            }
        } else {
            printf("Bucket %d, first node offset: 0 (empty)\n", i);
        }
    } 
}
			
			return TFS_SUCCESS;
		}
		
		int IndexHandle::remove(const uint32_t logic_block_id)
		{
			if(is_load_)
			{		
				if(logic_block_id != block_info()->block_id_)
				{
					fprintf(stderr, "block id conflict. block id：%d, index block id：%d\n", logic_block_id, block_info()->block_id_);
					return EXIT_BLOCKID_CONFLICT_ERROR;
				}
			}
			
			int ret = file_op_->munmap_file();
			if(TFS_SUCCESS != ret)
			{
				return ret;
			}
			
			ret = file_op_->unlink_file();
			return ret;
		}
		
		int IndexHandle::flush()
		{
			int ret = file_op_->flush_file();
			if(TFS_SUCCESS != ret)
			{
				fprintf(stderr, "index flush fail, ret：%d, error desc：%s\n", ret, strerror(errno));
			}
			return ret;
		}
		
		int IndexHandle::update_block_info(const OperType oper_type, const uint32_t modify_size)
		{
			if(block_info()->block_id_ == 0)
			{
				return EXIT_BLOCKID_ZERO_ERROR;
			}
			
			if(oper_type == C_OPER_INSERT)
			{
				++block_info()->version_;
				++block_info()->file_count_;
				++block_info()->seq_no_;
				block_info()->size_t_ += modify_size;	
			}
			else if(oper_type == C_OPER_DELETE)
			{
				++block_info()->version_;
				--block_info()->file_count_;
				block_info()->size_t_ -= modify_size;
				++block_info()->del_file_count_;
				block_info()->del_size_ += modify_size;
			}
			
			if(debug) printf("update block info.blockid：%u, version：%u, file count：%u, size：%u, del file count：%u, del size：%u, seq no：%u, oper type：%d\n",
						block_info()->block_id_, block_info()->version_, block_info()->file_count_, block_info()->size_t_, block_info()->del_file_count_, block_info()->del_size_, 
						block_info()->seq_no_, oper_type);
			
			return TFS_SUCCESS;
		}
		
		int32_t IndexHandle::write_segment_meta(const uint64_t key, MetaInfo& meta)
		{
			int32_t current_offset = 0, previous_offset = 0;         //当前读取的偏移量   //当前读取的前一个的偏移量
			
			//* key 是否存在？存在->处理？  不存在->处理？
			//1.从文件哈希表中查找key是否存在   hash_find(key, current_offset, previous_offset);
			int32_t ret = hash_find(key, current_offset, previous_offset);
			
			if(TFS_SUCCESS == ret)           //查找到哈希链表中该key已经存在
			{
				return EXIT_META_UNEXPECT_FOUND_ERROR;
			}
			else if(EXIT_META_NOT_FOUND_ERROR != ret)     //not found key(状态)
			{
				return ret;
			}
				
			//2.如果不存在就写入meta到文件哈希表中 hash_insert(key, previous_offset, meta)
			ret = hash_insert(key, previous_offset, meta);
			
			return ret;
		}
		
		int32_t IndexHandle::read_segment_meta(const uint64_t key, MetaInfo& meta)
{
	int32_t current_offset = 0, previous_offset = 0;         //当前读取的偏移量   //当前读取的前一个的偏移量
	
	int32_t ret = hash_find(key, current_offset, previous_offset);
	
	if(TFS_SUCCESS == ret)      //exist
	{
		ret = file_op_->pread_file(reinterpret_cast<char*>(&meta), sizeof(MetaInfo), current_offset);
		if(ret == sizeof(MetaInfo))
		{
			return TFS_SUCCESS;
		}
		else if(ret < 0)
		{
			return ret;
		}
		else
		{
			return EXIT_DISK_OPER_INCOMPLETE;
		}
	}
	else
	{
		return ret;
				
	}
}
		
		int32_t IndexHandle::delete_segment_meta(const uint64_t key)
{
	int32_t current_offset = 0, previous_offset = 0;
	
	int32_t ret = hash_find(key, current_offset, previous_offset);
	
	if(ret != TFS_SUCCESS)
	{
		return ret;
	}
	
	MetaInfo meta_info;
	ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), current_offset);
	// 修复bug：需要检查是否读取了完整的 MetaInfo 大小
	// 修改原因：pread_file 返回实际读取的字节数，如果只读取了部分数据，使用不完整的 meta_info 会导致错误
	if(ret < 0)
	{
		return ret;
	}
	if(ret != sizeof(MetaInfo))
	{
		return EXIT_DISK_OPER_INCOMPLETE;
	}

	int32_t next_pos = meta_info.get_next_meta_offset();          //下一个节点在链表中的偏移量
	meta_info.set_key(-1);
	
	if(previous_offset == 0)//
	{
		int32_t slot = static_cast<int32_t>(key) % bct_size();
		bucket_slot()[slot] = next_pos;
		
	}
	else
	{
		MetaInfo pre_meta_info;
		ret = file_op_->pread_file(reinterpret_cast<char*>(&pre_meta_info), sizeof(MetaInfo), previous_offset);
		// 修复bug：需要检查是否读取了完整的 MetaInfo 大小
		// 修改原因：pread_file 返回实际读取的字节数，如果只读取了部分数据，使用不完整的 meta_info 会导致错误
		if(ret < 0)
		{
			return ret;
		}
		if(ret != sizeof(MetaInfo))
		{
			return EXIT_DISK_OPER_INCOMPLETE;
		}

		pre_meta_info.set_next_meta_offset(next_pos);

		ret = file_op_->pwrite_file(reinterpret_cast<char*>(&pre_meta_info), sizeof(MetaInfo), previous_offset);
		// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
		// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
		if(ret != sizeof(MetaInfo))
		{
			return ret;
		}


	}

	//把删除节点加入可重用节点链表
	//前插法
	meta_info.set_next_meta_offset(free_head_offset());      //index_header()->free_head_offset_
	ret = file_op_->pwrite_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), current_offset);
	// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
	// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
	if(ret != sizeof(MetaInfo))
	{
		return ret;
	}
	
	index_header()->free_head_offset_ = current_offset;
	
	if(debug) printf("delete_segment_meta - reuse metainfo, current_offset：%d\n", current_offset);
	
	update_block_info(C_OPER_DELETE, meta_info.get_size());
		
	return TFS_SUCCESS;
}
		
		int32_t IndexHandle::hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset)
{
	int ret = TFS_SUCCESS;
	MetaInfo meta_info;      //保存临时读到的metainfo
	
	current_offset = 0;          //当前读取的偏移量
	previous_offset = 0;         //当前读取的前一个的偏移量
	
	//1.确定key存放的桶（slot）的位置
	int32_t slot = static_cast<int32_t>(key) % bct_size();
	
	//2.读取首节点存储的第一个节点的偏移量，如果偏移量为零，直接返回 EXIT_META_NOT_FOUND_ERROR
	//3.根据偏移量读取存储的metainfo
	//4.与key进行比较，相等则设置current_offset 和 previous_offset并返回TFS_SUCCESS，否则继续执行5
	//5.从metainfo中取得下一个节点的在文件中的偏移量，如果偏移量位零，直接返回 EXIT_META_NOT_FOUND_ERROR，否则，跳转至3继续循环执行
	int32_t pos = bucket_slot()[slot];
	
	for(; pos != 0; )
	{
		ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), pos);
		// 修复bug：需要检查是否读取了完整的 MetaInfo 大小
		// 修改原因：pread_file 返回实际读取的字节数，如果只读取了部分数据，使用不完整的 meta_info 会导致错误
		if(ret < 0)
		{
			return ret;
		}
		if(ret != sizeof(MetaInfo))
		{
			return EXIT_DISK_OPER_INCOMPLETE;
		}

		if(hash_compare(key, meta_info.get_key()))        
		{
			current_offset = pos;
			return TFS_SUCCESS;
		}
		
		previous_offset = pos;
		pos = meta_info.get_next_meta_offset();
	}
	
	return EXIT_META_NOT_FOUND_ERROR;
}
		
		int32_t IndexHandle::hash_insert(const uint64_t key, int32_t previous_offset, MetaInfo& meta)
{
	int ret = TFS_SUCCESS;
	int32_t current_offset = 0;
	MetaInfo tmp_meta_info;    //保存临时读到的metainfo
	
	//1.确定key存放的桶（slot）的位置
	int32_t slot = static_cast<int32_t>(key) % bct_size();
	
	//2.确定meta节点存储在文件中的偏移量
	if(free_head_offset() != 0)
	{
		ret = file_op_->pread_file(reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), free_head_offset());
		// 修复bug：需要检查是否读取了完整的 MetaInfo 大小
		// 修改原因：pread_file 返回实际读取的字节数，如果只读取了部分数据，使用不完整的 meta_info 会导致错误
		if(ret < 0)
		{
			return ret;
		}
		if(ret != sizeof(MetaInfo))
		{
			return EXIT_DISK_OPER_INCOMPLETE;
		}

		current_offset = index_header()->free_head_offset_;
		
		if(debug) printf("reuse metainfo, current_offset：%d\n", current_offset);
		
		index_header()->free_head_offset_ = tmp_meta_info.get_next_meta_offset();
		
	}
	else
	{
		current_offset = index_header()->index_file_size_;
		index_header()->index_file_size_ += sizeof(MetaInfo);
	}
	
	//3.将meta节点写入索引文件中
	meta.set_next_meta_offset(0);

	ret = file_op_->pwrite_file(reinterpret_cast<const char*>(&meta), sizeof(MetaInfo), current_offset);
	// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
	// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
	if(ret != sizeof(MetaInfo))
	{
		index_header()->index_file_size_ -= sizeof(MetaInfo);
		return ret;
	}
	
	//4.将meta节点插入到哈希链表中
	if(0 != previous_offset)       //前一个节点已经存在
	{
		ret = file_op_->pread_file(reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset);
		// 修复bug：需要检查是否读取了完整的 MetaInfo 大小
		// 修改原因：pread_file 返回实际读取的字节数，如果只读取了部分数据，使用不完整的 meta_info 会导致错误
		if(ret < 0)
		{
			index_header()->index_file_size_ -= sizeof(MetaInfo);
			return ret;
		}
		if(ret != sizeof(MetaInfo))
		{
			index_header()->index_file_size_ -= sizeof(MetaInfo);
			return EXIT_DISK_OPER_INCOMPLETE;
		}

		tmp_meta_info.set_next_meta_offset(current_offset);
		ret = file_op_->pwrite_file(reinterpret_cast<const char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset);
		// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
		// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
		if(ret != sizeof(MetaInfo))
		{
			index_header()->index_file_size_ -= sizeof(MetaInfo);
			return ret;
		}
	}
	else        //不存在前一个节点，为首节点
	{
		bucket_slot()[slot] = current_offset;
	}
	
	return TFS_SUCCESS;
}
		
		int32_t IndexHandle::block_tidy(FileOperation* fo)                //整理块
		{
			//查看del_file_count
			//根据文件编号，逐步从头部开始写， hash_find 没有就continue，有就往前写
			//截断文件
			//更新索引文件信息
			
			if(!fo)          //块文件不存在
			{
				return EXIT_BLOCK_NOT_EXIST;
			}
			
			if(block_info()->del_file_count_ <= 0)        //块删除文件数量小于0
			{
				fprintf(stderr, "block id %u do not have del_file. del_file_count：%d\n", block_info()->block_id_, block_info()->del_file_count_);
				return EXIT_BLOCK_DEL_FILE_COUNT_LESSZERO;
			}
			
			if(block_info()->del_size_ <= 0)             //块删除文件大小小于0
			{
				fprintf(stderr, "block id %u do not have del_file_size. del_file_size：%d\n", block_info()->block_id_, block_info()->del_size_);
				return EXIT_BLOCK_DEL_SIZE_LESSZEOR;
			}
			
			int32_t file_count = block_info()->file_count_;        //文件数量
			int32_t ret = TFS_SUCCESS;
			int32_t over_write_offset = 0;       //整个文件写入块后的偏移量
			int32_t current_write_offset = 0;    //文件未写全，块中的偏移量
		
			int64_t residue_bytes = 0;          //写入后还剩下需要写的字节数
			uint64_t key = 1;         //保存文件编号
			
			
			//整理块
			for(int i = 1; i <= file_count; )
			{
				MetaInfo meta_info;            //保存临时读到的metainfo
				char buffer[4096] = { '0' };                 //保存的文件
				int nbytes = sizeof(buffer);        //该次需要写入的字节数
				
				ret = read_segment_meta(key, meta_info);
				
				if(debug) fprintf(stderr, "i：%d, file_id：%ld, key：%ld, ret：%d\n", i, meta_info.get_key(), key, ret);
				
				if(TFS_SUCCESS == ret)           //已经在哈希链表中读到
			{
				current_write_offset = meta_info.get_offset();   
				residue_bytes = meta_info.get_size(); 
				if(debug) fprintf(stderr, "处理文件 key：%ld, offset：%d, size：%d\n", key, current_write_offset, meta_info.get_size());
				
				// 检查元信息是否有效
				if(current_write_offset < 0 || residue_bytes < 0) {
					if(debug) fprintf(stderr, "无效的元信息，跳过文件 key：%ld\n", key);
					key++;
					continue;
				}
				
				// 检查文件偏移量是否有效
				int64_t file_size = fo->get_file_size();
				if(debug) fprintf(stderr, "文件大小：%ld, 计算结束位置：%ld\n", file_size, static_cast<int64_t>(current_write_offset) + static_cast<int64_t>(residue_bytes));
				if(file_size < 0) {
					if(debug) fprintf(stderr, "获取文件大小失败，跳过文件 key：%ld\n", key);
					key++;
					i++;
					continue;
				}
				if(static_cast<int64_t>(current_write_offset) >= file_size) {
					if(debug) fprintf(stderr, "文件偏移量超出范围，跳过文件 key：%ld\n", key);
					key++;
					i++;
					continue;
				}
				
				// 修复警告：将sizeof(buffer)转换为int32_t以避免有符号和无符号整数比较
				if(meta_info.get_size() <= static_cast<int32_t>(sizeof(buffer)))        //一次读完
				{
					if(debug) fprintf(stderr, "一次读取：size：%d, offset：%d\n", meta_info.get_size(), meta_info.get_offset());
					int32_t read_ret = fo->pread_file(buffer, meta_info.get_size(), meta_info.get_offset());
					if(debug) fprintf(stderr, "pread_file ret：%d, expected：%d\n", read_ret, meta_info.get_size());
					// 修复bug：pread_file 返回实际读取的字节数，不再是 TFS_SUCCESS
					// 修改原因：file_op.cpp 中 pread_file 返回实际读取的字节数
					if(read_ret > 0)    //文件读成功,将文件重新写入块中
					{
						if(debug) fprintf(stderr, "一次写入：size：%d, offset：%d\n", read_ret, over_write_offset);
						int32_t write_ret = fo->pwrite_file(buffer, read_ret, over_write_offset);
						if(debug) fprintf(stderr, "pwrite_file ret：%d, expected：%d\n", write_ret, read_ret);
						// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
						// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
						if(write_ret > 0)          //文件写入成功
						{
							over_write_offset += write_ret;
							if(debug) fprintf(stderr, "写入成功，新的 over_write_offset：%d\n", over_write_offset);
							key++;
							i++;
						}
						else         //文件未写成功 / 未写全
						{
							if(debug) fprintf(stderr, "写入失败，ret：%d\n", write_ret);
							key++;
							i++;
							continue;
						}
					}
					else if(read_ret < 0)           //文件读取错误
					{
						if(debug) fprintf(stderr, "读取失败，ret：%d\n", read_ret);
						key++;
						i++;
						continue;
					}
					else           //文件已读完
					{
						if(debug) fprintf(stderr, "文件已读完，跳过\n");
						key++;
						i++;
					}
				}
				else         //需要分多次读写
				{
					nbytes = sizeof(buffer);
					if(debug) fprintf(stderr, "分多次读写：总大小：%d, 每次读写：%d\n", meta_info.get_size(), nbytes);

					// 检查文件偏移量是否有效
					int64_t file_size = fo->get_file_size();
					if(debug) fprintf(stderr, "文件大小：%ld, 计算结束位置：%ld\n", file_size, static_cast<int64_t>(current_write_offset) + static_cast<int64_t>(residue_bytes));
					if(file_size < 0 || static_cast<int64_t>(current_write_offset) >= file_size) {
						if(debug) fprintf(stderr, "文件偏移量超出范围，跳过文件 key：%ld\n", key);
						key++;
						i++;
						continue;
					}

					bool read_error = false;
					while(residue_bytes>0)
					{
						if(debug) fprintf(stderr, "读取部分：size：%d, offset：%d\n", nbytes, current_write_offset);
						int32_t read_ret = fo->pread_file(buffer, nbytes, current_write_offset);
						if(debug) fprintf(stderr, "pread_file ret：%d, expected：%d\n", read_ret, nbytes);
						// 修复bug：pread_file 返回实际读取的字节数，不再是 TFS_SUCCESS
						// 修改原因：file_op.cpp 中 pread_file 返回实际读取的字节数
						if(read_ret > 0)    		//文件读成功,将部分文件重新写入块中
						{
							if(debug) fprintf(stderr, "写入部分：size：%d, offset：%d\n", read_ret, over_write_offset);
							int32_t write_ret = fo->pwrite_file(buffer, read_ret, over_write_offset);
							if(debug) fprintf(stderr, "pwrite_file ret：%d, expected：%d\n", write_ret, read_ret);
							// 修复bug：pwrite_file 返回实际写入的字节数，不再是 TFS_SUCCESS
							// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
							if(write_ret > 0)          //文件写入成功
							{
								current_write_offset += write_ret;
								over_write_offset += write_ret;
								residue_bytes -= write_ret;
								if(debug) fprintf(stderr, "写入成功，剩余字节：%ld, 新的 over_write_offset：%d\n", residue_bytes, over_write_offset);

								if(nbytes > residue_bytes)
								{
									nbytes = residue_bytes;
									if(debug) fprintf(stderr, "调整 nbytes 为：%d\n", nbytes);
								}

							}
							else         //文件未写成功 / 未写全
							{
								if(debug) fprintf(stderr, "写入失败，ret：%d\n", write_ret);
								read_error = true;
								break;
							}
						}
						else if(read_ret < 0)           //文件读取错误
						{
							if(debug) fprintf(stderr, "读取失败，ret：%d\n", read_ret);
							read_error = true;
							break;
						}
						else           //文件已读完
						{
							if(debug) fprintf(stderr, "文件已读完，跳过\n");
							break;
						}
					}
					key++;
					i++;
					if(read_error) {
						if(debug) fprintf(stderr, "读取或写入失败，跳过文件 key：%ld\n", key-1);
						continue;
					}
				}
			}
			else if(EXIT_META_NOT_FOUND_ERROR != ret)     //not found key(状态)
			{
				if(debug) fprintf(stderr, "读取元信息失败，跳过文件 key：%ld, ret：%d\n", key, ret);
				key++;
				i++;
				continue;
			}
			else if(EXIT_META_NOT_FOUND_ERROR == ret)     //哈希链表中没有找到,该文件已被删除
			{
				key++;
				i++;
				continue;
			}
		}
		
		if(debug) fprintf(stderr, "整理块成功，over_write_offset：%d\n", over_write_offset);

		ret = fo->flush_file();
		if(debug) fprintf(stderr, "flush_file ret：%d\n", ret);
		//截断文件
		// 修复bug：应该使用 over_write_offset 而不是 block_info()->size_t_
		// 修改原因：over_write_offset 是整理后实际写入的数据大小（不包含已删除文件）
		// 而 block_info()->size_t_ 是整理前的原始文件大小（包含已删除文件的空间）
		ret = fo->ftruncate_file(over_write_offset);
		if(debug) fprintf(stderr, "ftruncate_file ret：%d\n", ret);

		//更新索引文件信息
		// 修复bug：应该使用 over_write_offset 而不是 block_info()->size_t_
		index_header()->data_file_offset_ = over_write_offset;

		//更新block info
		// 修复bug：应该使用 over_write_offset 来更新块大小
		block_info()->size_t_ = over_write_offset;
		block_info()->del_file_count_ = 0;
		block_info()->del_size_ = 0;
		ret = flush();
		if(debug) fprintf(stderr, "flush ret：%d\n", ret);


		if(debug) 
		{ 
			// 打印桶的数量
			printf("Bucket count: %d\n", bct_size());
			
			// 遍历每个桶
			for(int i = 0; i < bct_size(); ++i) 
			{ 
				int32_t slot_offset = bucket_slot()[i];
				if (slot_offset > 0) {
					// 读取桶的首节点元数据
					MetaInfo meta_info;
					int32_t ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), slot_offset);
					// 修复bug：pread_file 返回实际读取的字节数，不再是 TFS_SUCCESS
					// 修改原因：file_op.cpp 中 pread_file 返回实际读取的字节数
					if (ret == sizeof(MetaInfo)) {
						printf("Bucket %d, first node offset: %d, file id: %ld, size: %d, next offset: %d\n", 
							   i, slot_offset, meta_info.get_file_id(), meta_info.get_size(), meta_info.get_next_meta_offset());
					}
				} else {
					printf("Bucket %d, first node offset: 0 (empty)\n", i);
				}
			} 
		}
		
		return TFS_SUCCESS;
	}
	}
}