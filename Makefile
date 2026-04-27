CXX = g++
CXXFLAGS = -Iinclude -std=c++17 -Wall -Wextra
LDFLAGS = -lgtest -lgtest_main -pthread

SRCDIR = src
TESTDIR = test

SRCFILES = $(SRCDIR)/mmap_file.cpp $(SRCDIR)/file_op.cpp $(SRCDIR)/index_handle.cpp $(SRCDIR)/mmap_file_op.cpp

all: block_init_test block_write_test block_read_test block_delete_test block_tidy_test block_stat mmap_file_op_test main gtest

block_init_test: $(TESTDIR)/block_init_test.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

block_write_test: $(TESTDIR)/block_write_test.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

block_read_test: $(TESTDIR)/block_read_test.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

block_delete_test: $(TESTDIR)/block_delete_test.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

block_tidy_test: $(TESTDIR)/block_tidy_test.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

block_stat: $(TESTDIR)/block_stat.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

mmap_file_op_test: $(TESTDIR)/mmap_file_op_test.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

main: $(SRCDIR)/main.cpp $(SRCDIR)/mmap_file.cpp $(SRCDIR)/file_op.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

gtest: $(TESTDIR)/tfs_gtest.cpp $(SRCFILES)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

test: gtest
	./gtest

clean:
	rm -f block_init_test block_write_test block_read_test block_delete_test block_tidy_test block_stat mmap_file_op_test main gtest
