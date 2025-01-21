#include "../include/filesystem/operations.h"
#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                         const std::vector<std::string> &files_, std::string resultFile) {
    chfs_client = std::move(client);
    files = files_;
    outPutFile = std::move(resultFile);
}

void SequentialMapReduce::doWork() {

    // Your code goes here
    std::vector<std::vector<KeyVal>> kvs_list;
    kvs_list.reserve(files.size());
    std::unordered_map<std::string, std::vector<std::string>> kv_maper;

    for (auto &file: files) {
        auto inode_id = chfs_client->lookup(1, file);
        if (inode_id.is_err()) {
            std::cerr << "File not found" << std::endl;
            return;
        }

        auto attr = chfs_client->get_type_attr(inode_id.unwrap());

        if (attr.is_err()) {
            std::cerr << "File not found" << std::endl;
            return;
        }

        auto res = chfs_client->read_file(inode_id.unwrap(), 0, attr.unwrap().second.size);

        if (res.is_err()) {
            std::cerr << "File not found" << std::endl;
            return;
        }

        auto char_vec = res.unwrap();

        std::string content;
        content.resize(char_vec.size());

        memcpy(content.data(), char_vec.data(), char_vec.size());

        kvs_list.emplace_back(Map(content));
    }

    for (auto &kvs: kvs_list) {
        for (auto &kv: kvs) {
            kv_maper[kv.key].emplace_back(kv.val);
        }
    }

    size_t offset = 0;
    for (auto &kv: kv_maper) {
        auto res_create = chfs_client->lookup(1, outPutFile);
        if (res_create.is_err()) {
            std::cerr << "File not found" << std::endl;
            return;
        }

        auto inode_id = res_create.unwrap();
        std::string val = Reduce(kv.first, kv.second);
        std::vector<chfs::u8> vec(val.begin(), val.end());
//        std::cout << val << std::endl;

        chfs_client->write_file(inode_id, offset, vec);
        offset += vec.size();
    }
}

}// namespace mapReduce