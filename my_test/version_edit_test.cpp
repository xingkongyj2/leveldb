#include <assert.h>
#include <string>
#include <iostream>
#include "db/version_edit.h"

  // Status DecodeFrom(const Slice& src);

  // std::string DebugString() const;

std::string read_manifest_file(const std::string& file_path) {
    FILE* fp = fopen(file_path.c_str(), "r");
    assert(fp != NULL);

    const int max_contents_len = 1024;
    char contents[max_contents_len];
    size_t contents_size = fread(contents, 1, max_contents_len, fp);
    std::cout << contents_size << std::endl;

    return std::string(contents, contents_size);
}

int main() {
    std::string contents = read_manifest_file("./data/new_sample.db/MANIFEST-000002");
    leveldb::VersionEdit version_edit;
    std::cout << contents << std::endl;

    version_edit.DecodeFrom(contents.c_str());
    std::cout << version_edit.DebugString() << std::endl;

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
