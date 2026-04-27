#include "mmap_file_op.h"
#include "common.h"


static int debug = 1;

namespace conway
{
	namespace largefile
	{
		int MMapFileOperation::mmap_file(const MMapOption& mmap_option)
		{
			if(mmap_option.max_mmap_size_ < mmap_option.first_mmap_size_)
			{
				return TFS_ERROR;
			}
			
			if(mmap_option.max_mmap_size_ <= 0)
			{
				return TFS_ERROR;
			}
			
			int fd = check_file();
			if(fd < 0)
			{
				fprintf(stderr, "MMapFileOperation::mmap_file -checking file failed!");
				return TFS_ERROR;
			}
			
			if(!is_mapped_)
			{
				// 先 munmap 之前的映射
				// 修复bug：内层的 if(is_mapped_) 检查永远不会为真，因为外层已经保证 is_mapped_ 为 false
				// 修改原因：外层 if(!is_mapped_) 已保证 is_mapped_ 为 false，内层检查冗余
				// 正确逻辑：应该先检查并清理旧的映射，再创建新映射
				if(is_mapped_ && map_file_ != NULL)
				{
					munmap_file();
				}

				// 创建新的映射
				map_file_ = new MMapFile(mmap_option, fd);
				is_mapped_ = map_file_->map_file(true);
			}
			
			if(is_mapped_)
			{
				return TFS_SUCCESS;
			}
			else
			{
				// 如果映射失败，清理资源
				if(map_file_)
				{
					delete map_file_;
					map_file_ = NULL;
				}
				return TFS_ERROR;
			}		
		}
		
		int MMapFileOperation::munmap_file()
{
	if(is_mapped_ && map_file_ != NULL)
	{
		delete map_file_;    //调用析构函数 ~MMapFile()
		map_file_ = NULL;
		is_mapped_ = false;
	}
	
	return TFS_SUCCESS;
}
		
		void* MMapFileOperation::get_map_data() const
		{
			if(is_mapped_)
			{
				return map_file_->get_data();
			}
			
			return NULL;
		}
		
		int MMapFileOperation::pread_file(char* buf, const int32_t size, const int64_t offset)
		{
			//情况1，内存已经映射
			if(is_mapped_ && (offset + size) > map_file_->get_size())
			{
				if(debug) fprintf(stdout, "mmap_file_op pread, size：%d, offset：%" __PRI64_PREFIX"d, \
				map file size：%d. need mremap\n", size, offset, map_file_->get_size());
				map_file_->mremap_file();			
			}
			
			if(is_mapped_ && (offset + size) <= map_file_->get_size())
			{
				memcpy(buf, (char*)map_file_->get_data() + offset, size);
				return size;
			}
			
			//情况2，内存没有映射或是要读取的数据映射不全
			return FileOperation::pread_file(buf, size, offset);		
		}
	
		int MMapFileOperation::pwrite_file(const char* buf, const int32_t size, const int64_t offset)
		{
			//情况1，内存已经映射
			if(is_mapped_ && (offset + size) > map_file_->get_size())
			{
				if(debug) fprintf(stdout, "mmap_file_op pwrite, size：%d, offset：%" __PRI64_PREFIX"d, \
					map file size：%d. need mremap\n", size, offset, map_file_->get_size());
				map_file_->mremap_file();
			}
			
			if(is_mapped_ && (offset + size) <= map_file_->get_size())
			{
				memcpy((char*)map_file_->get_data() + offset, buf, size);
				// 修复bug：pwrite_file 应返回实际写入的字节数，与 FileOperation::pwrite_file 保持一致
				// 修改原因：file_op.cpp 中 pwrite_file 已修改为返回实际写入的字节数
				return size;
			}
			
			//情况2，内存没有映射或是要读取的数据映射不全
			return FileOperation::pwrite_file(buf, size, offset);
		}
		
		int MMapFileOperation::flush_file()
		{
			if(is_mapped_)
			{
				if(map_file_->sync_file())
				{
					return TFS_SUCCESS;
				}
				else
				{
					return TFS_ERROR;
				}
			}
			
			return FileOperation::flush_file();
		}
		
	}
}