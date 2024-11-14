#include <cstring>
#include <iostream>
#include <skim.h>
#include <sparse_index.h>
#include <tantivy_search.h>
#include <blake3.h>

void test_blake3()
{
    const char * input = "Hello, world!";
    unsigned int size = strlen(input);
    unsigned char output[64]; // BLAKE3 的输出长度

    // 调用 Rust 函数
    char * error = blake3_apply_shim(input, size, output);

    if (error != nullptr)
    {
        std::cerr << "Error: " << error << std::endl;
        // 释放 Rust 端分配的错误字符串
        blake3_free_char_pointer(error);
    }
    else
    {
        std::cout << "BLAKE3 Hash: ";
        for (int i = 0; i < 64; ++i)
        {
            std::cout << std::hex << (int)output[i];
        }
        std::cout << std::endl;
    }
}

void test_skim()
{
    try
    {
        // 初始化 CxxString 和 CxxVector
        std::string prefix = "he";
        std::vector<std::string> words;
        words.push_back("hello");
        words.push_back("world");

        // 调用 Rust 函数
        auto result = skim(prefix, words);
        std::cout << "skim result: " << result.c_str() << std::endl;
    }
    catch (const std::exception & e)
    {
        std::cerr << "Exception occurred: " << e.what() << std::endl;
    }
}


void test_tantivy_search()
{
    auto res = TANTIVY::ffi_free_index_reader("tmp");
    std::cout << "sparse -> res is error: " << res.error.is_error << std::endl;
}

void test_sparse_index()
{
    auto res = SPARSE::ffi_load_index("tmp");
    std::cout << "sparse -> res is error: " << res.error.is_error << std::endl;
}
int main(int argc, char ** argv)
{
    test_blake3();
    test_tantivy_search();
    test_sparse_index();
    test_skim();

    return 0;
}