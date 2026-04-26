CXX = g++
CXXFLAGS = -I.

all: block_init_test block_write_test block_read_test block_delete_test block_tidy_test block_stat mmap_file_op_test  main

block_init_test: block_init_test.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

block_write_test: block_write_test.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

block_read_test: block_read_test.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

block_delete_test: block_delete_test.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

block_tidy_test: block_tidy_test.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

block_stat: block_stat.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

mmap_file_op_test: mmap_file_op_test.cpp mmap_file.cpp file_op.cpp index_handle.cpp mmap_file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

op_test: op_test.cpp file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

main: main.cpp mmap_file.cpp file_op.cpp
		$(CXX) $(CXXFLAGS) -o $@ $^

clean:
		rm -f block_init_test block_write_test block_read_test block_delete_test block_tidy_test block_stat mmap_file_op_test  op_test main
