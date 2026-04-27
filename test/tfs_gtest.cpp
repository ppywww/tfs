#include <gtest/gtest.h>
#include "common.h"
#include "file_op.h"
#include "mmap_file.h"
#include "mmap_file_op.h"
#include "index_handle.h"

using namespace conway::largefile;
using namespace std;

class CommonTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CommonTest, BlockInfoDefaultConstructor) {
    BlockInfo info;
    EXPECT_EQ(info.block_id_, 0u);
    EXPECT_EQ(info.version_, 0);
    EXPECT_EQ(info.file_count_, 0);
    EXPECT_EQ(info.size_t_, 0);
    EXPECT_EQ(info.del_file_count_, 0);
    EXPECT_EQ(info.del_size_, 0);
    EXPECT_EQ(info.seq_no_, 0u);
}

TEST_F(CommonTest, BlockInfoEquality) {
    BlockInfo info1;
    info1.block_id_ = 1;
    info1.version_ = 2;
    info1.file_count_ = 3;
    info1.size_t_ = 4;
    info1.del_file_count_ = 5;
    info1.del_size_ = 6;
    info1.seq_no_ = 7;
    
    BlockInfo info2;
    info2.block_id_ = 1;
    info2.version_ = 2;
    info2.file_count_ = 3;
    info2.size_t_ = 4;
    info2.del_file_count_ = 5;
    info2.del_size_ = 6;
    info2.seq_no_ = 7;
    
    EXPECT_TRUE(info1 == info2);
    
    info2.block_id_ = 2;
    EXPECT_FALSE(info1 == info2);
}

TEST_F(CommonTest, MetaInfoDefaultConstructor) {
    MetaInfo meta;
    EXPECT_EQ(meta.get_file_id(), 0u);
    EXPECT_EQ(meta.get_offset(), 0);
    EXPECT_EQ(meta.get_size(), 0);
    EXPECT_EQ(meta.get_next_meta_offset(), 0);
}

TEST_F(CommonTest, MetaInfoParameterizedConstructor) {
    MetaInfo meta(12345, 100, 50, 200);
    EXPECT_EQ(meta.get_file_id(), 12345u);
    EXPECT_EQ(meta.get_offset(), 100);
    EXPECT_EQ(meta.get_size(), 50);
    EXPECT_EQ(meta.get_next_meta_offset(), 200);
}

TEST_F(CommonTest, MetaInfoCopyConstructor) {
    MetaInfo meta1(12345, 100, 50, 200);
    MetaInfo meta2(meta1);
    EXPECT_EQ(meta1, meta2);
}

TEST_F(CommonTest, MetaInfoAssignmentOperator) {
    MetaInfo meta1(12345, 100, 50, 200);
    MetaInfo meta2;
    meta2 = meta1;
    EXPECT_EQ(meta1, meta2);
}

class FileOperationTest : public ::testing::Test {
protected:
    string test_file = "/tmp/test_file_op";
    
    void SetUp() override {
        unlink(test_file.c_str());
    }
    
    void TearDown() override {
        unlink(test_file.c_str());
    }
};

TEST_F(FileOperationTest, OpenCloseFile) {
    FileOperation fo(test_file, O_RDWR | O_CREAT | O_LARGEFILE);
    int ret = fo.open_file();
    EXPECT_GT(ret, 0);
    fo.close_file();
}

TEST_F(FileOperationTest, WriteAndReadFile) {
    FileOperation fo(test_file, O_RDWR | O_CREAT | O_LARGEFILE);
    int fd = fo.open_file();
    EXPECT_GT(fd, 0);
    
    const char* write_data = "Hello TFS!";
    int write_len = strlen(write_data);
    int ret = fo.write_file(write_data, write_len);
    EXPECT_EQ(ret, TFS_SUCCESS);
    fo.flush_file();
    
    char read_buf[100];
    memset(read_buf, 0, sizeof(read_buf));
    ret = fo.pread_file(read_buf, write_len, 0);
    EXPECT_EQ(ret, write_len);
    EXPECT_STREQ(read_buf, write_data);
    
    fo.close_file();
}

TEST_F(FileOperationTest, PWritePReadFile) {
    FileOperation fo(test_file, O_RDWR | O_CREAT | O_LARGEFILE);
    int fd = fo.open_file();
    EXPECT_GT(fd, 0);
    
    const char* data1 = "First";
    const char* data2 = "Second";
    
    int ret = fo.pwrite_file(data1, strlen(data1), 0);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    ret = fo.pwrite_file(data2, strlen(data2), 100);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    char buf[100];
    memset(buf, 0, sizeof(buf));
    ret = fo.pread_file(buf, strlen(data1), 0);
    EXPECT_EQ(ret, strlen(data1));
    EXPECT_STREQ(buf, data1);
    
    memset(buf, 0, sizeof(buf));
    ret = fo.pread_file(buf, strlen(data2), 100);
    EXPECT_EQ(ret, strlen(data2));
    EXPECT_STREQ(buf, data2);
    
    fo.close_file();
}

TEST_F(FileOperationTest, GetFileSize) {
    FileOperation fo(test_file, O_RDWR | O_CREAT | O_LARGEFILE);
    fo.open_file();
    
    const char* data = "Test data";
    fo.write_file(data, strlen(data));
    fo.flush_file();
    
    int64_t size = fo.get_file_size();
    EXPECT_EQ(size, strlen(data));
    
    fo.close_file();
}

TEST_F(FileOperationTest, FtruncateFile) {
    FileOperation fo(test_file, O_RDWR | O_CREAT | O_LARGEFILE);
    fo.open_file();
    
    const char* data = "Long long data";
    fo.write_file(data, strlen(data));
    fo.flush_file();
    
    int ret = fo.ftruncate_file(5);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    int64_t size = fo.get_file_size();
    EXPECT_EQ(size, 5);
    
    fo.close_file();
}

TEST_F(FileOperationTest, UnlinkFile) {
    FileOperation fo(test_file, O_RDWR | O_CREAT | O_LARGEFILE);
    fo.open_file();
    fo.close_file();
    
    int ret = fo.unlink_file();
    EXPECT_EQ(ret, TFS_SUCCESS);
}

class IndexHandleTest : public ::testing::Test {
protected:
    string base_path = "/tmp";
    uint32_t block_id = 100;
    MMapOption mmap_option;
    
    void SetUp() override {
        mmap_option.max_mmap_size_ = 1024 * 1024;
        mmap_option.first_mmap_size_ = 4096;
        mmap_option.per_mmap_size_ = 4096;
        
        // Create directories
        string index_dir = base_path + INDEX_DIR_PREFIX;
        string block_dir = base_path + MAINBLOCK_DIR_PREFIX;
        mkdir(index_dir.c_str(), 0755);
        mkdir(block_dir.c_str(), 0755);
        
        string index_file = base_path + INDEX_DIR_PREFIX + to_string(block_id);
        string block_file = base_path + MAINBLOCK_DIR_PREFIX + to_string(block_id);
        unlink(index_file.c_str());
        unlink(block_file.c_str());
    }
    
    void TearDown() override {
        string index_file = base_path + INDEX_DIR_PREFIX + to_string(block_id);
        string block_file = base_path + MAINBLOCK_DIR_PREFIX + to_string(block_id);
        unlink(index_file.c_str());
        unlink(block_file.c_str());
    }
};

TEST_F(IndexHandleTest, CreateAndLoad) {
    IndexHandle index(base_path, block_id);
    
    int ret = index.create(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    IndexHandle index2(base_path, block_id);
    ret = index2.load(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    EXPECT_EQ(index2.block_info()->block_id_, block_id);
}

TEST_F(IndexHandleTest, WriteAndReadMeta) {
    IndexHandle index(base_path, block_id);
    index.create(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    
    uint64_t file_id = 12345;
    MetaInfo meta_write(file_id, 100, 200, 0);
    int ret = index.write_segment_meta(file_id, meta_write);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    MetaInfo meta_read;
    ret = index.read_segment_meta(file_id, meta_read);
    EXPECT_EQ(ret, TFS_SUCCESS);
    EXPECT_EQ(meta_read, meta_write);
}

TEST_F(IndexHandleTest, DeleteMeta) {
    IndexHandle index(base_path, block_id);
    index.create(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    
    uint64_t file_id = 12345;
    MetaInfo meta(file_id, 100, 200, 0);
    index.write_segment_meta(file_id, meta);
    
    int ret = index.delete_segment_meta(file_id);
    EXPECT_EQ(ret, TFS_SUCCESS);
    
    MetaInfo meta_read;
    ret = index.read_segment_meta(file_id, meta_read);
    EXPECT_EQ(ret, EXIT_META_NOT_FOUND_ERROR);
}

TEST_F(IndexHandleTest, UpdateBlockInfo) {
    IndexHandle index(base_path, block_id);
    index.create(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    
    int file_size = 100;
    int ret = index.update_block_info(C_OPER_INSERT, file_size);
    EXPECT_EQ(ret, TFS_SUCCESS);
    EXPECT_EQ(index.block_info()->file_count_, 1);
    EXPECT_EQ(index.block_info()->size_t_, file_size);
    EXPECT_EQ(index.block_info()->version_, 1);
    EXPECT_EQ(index.block_info()->seq_no_, 2u);
    
    ret = index.update_block_info(C_OPER_DELETE, file_size);
    EXPECT_EQ(ret, TFS_SUCCESS);
    EXPECT_EQ(index.block_info()->del_file_count_, 1);
    EXPECT_EQ(index.block_info()->del_size_, file_size);
    EXPECT_EQ(index.block_info()->version_, 2);
}

TEST_F(IndexHandleTest, BlockDataOffset) {
    IndexHandle index(base_path, block_id);
    index.create(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    
    int initial_offset = index.get_block_data_offset();
    
    int file_size = 100;
    index.commit_block_data_offset(file_size);
    
    EXPECT_EQ(index.get_block_data_offset(), initial_offset + file_size);
}

TEST_F(IndexHandleTest, Remove) {
    IndexHandle index(base_path, block_id);
    index.create(block_id, DEFAULT_BUCKET_SIZE, mmap_option);
    
    int ret = index.remove(block_id);
    EXPECT_EQ(ret, TFS_SUCCESS);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
