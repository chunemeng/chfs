#include <fstream>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {

Worker::Worker(MR_CoordinatorConfig config) {
    mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
    outPutFile = config.resultFile;
    chfs_client = config.client;
    work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
    // Lab4: Your code goes here (Optional).
}

void Worker::doMap(int index, const std::string &filename) {
    auto node_id_res = chfs_client->lookup(1, filename);
    auto inode_id = node_id_res.unwrap();

    auto type_res = chfs_client->get_type_attr(inode_id);
    auto read_res = chfs_client->read_file(inode_id, 0, type_res.unwrap().second.size);
    auto char_vec = read_res.unwrap();

    std::string content;
    content.resize(char_vec.size());
    memcpy(content.data(), char_vec.data(), char_vec.size());


    // It's safe to directly write into index file
    // because this is a distributed system
    // even though the failed worker still write is also fine
    auto file_name = std::string("mr_") + std::to_string(index);

    auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, file_name);

    if (mknode_res.is_err()) {
        std::cerr << "Error in mknode" << std::endl;
        return;
    }

    auto kv = Map(content);

    std::sort(kv.begin(), kv.end(), [](const KeyVal &a, const KeyVal &b) {
        return a.key[0] < b.key[0];
    });

    std::vector<chfs::u8> content_vec;
    std::string content_str;
    for (int i = 0; i < kv.size(); i++) {
        if (i == kv.size() - 1) {
            content_str += kv[i].key + " " + kv[i].val;
        } else {
            content_str += kv[i].key + " " + kv[i].val + " ";
        }
    }

    content_vec.resize(content_str.size());
    memcpy(content_vec.data(), content_str.data(), content_str.size());

    auto write_res = chfs_client->write_file(mknode_res.unwrap(), 0, content_vec);

    if (write_res.is_err()) {
        std::cerr << "Error in write file" << std::endl;
        return;
    }

    doSubmit(mapReduce::mr_tasktype::MAP, index);
}

const char *alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz{";

void Worker::doReduce(int index, int nfiles, int nreduces) {
    if (index > 52) {
        return;
    }
    nreduces = std::min(nreduces, 52);

    int start = 52 * index / nreduces;
    int end = 52 * (index + 1) / nreduces;
    std::string start_char;
    start_char.resize(2);
    start_char[0] = ' ';
    start_char[1] = alphabet[start];
    std::string end_char;
    end_char.resize(2);
    end_char[0] = ' ';
    end_char[1] = alphabet[end];
    //    std::cout << start_char << " " << end_char << std::endl;

    std::unordered_map<std::string, std::vector<std::string>> kv_mapper;
    for (int i = 0; i < nfiles; i++) {
        auto file_name = std::string("mr_") + std::to_string(i);
        auto node_id_res = chfs_client->lookup(1, file_name);
        auto inode_id = node_id_res.unwrap();

        auto type_res = chfs_client->get_type_attr(inode_id);
        auto read_res = chfs_client->read_file(inode_id, 0, type_res.unwrap().second.size);
        if (read_res.is_err()) {
            std::cerr << "Error in read file" << std::endl;
            return;
        }

        auto char_vec = read_res.unwrap();
        std::string_view content((char *) char_vec.data(), char_vec.size());
        size_t start_pos = 0;
        if (start == 0) {
            start_pos = 0;
        } else {
            start_pos = content.find(start_char);
            start_pos++;
        }

        if (start_pos == std::string::npos) {
            continue;
        }


        auto end_pos = content.find(end_char, start_pos);

        if (end_pos == std::string::npos) {
            end_pos = content.size();
        }

        auto sub_content = content.substr(start_pos, end_pos - start_pos);
        //        std::cout << end_pos - start_pos << std::endl;
        size_t pos = 0;
        size_t last_pos = 0;
        while ((pos = sub_content.find(' ', last_pos)) != std::string_view::npos) {
            auto key = sub_content.substr(last_pos, pos - last_pos);

            last_pos = pos + 1;

            pos = sub_content.find(' ', last_pos);
            if (pos == std::string_view::npos) {
                pos = sub_content.size();
                kv_mapper[std::string(key)].emplace_back(sub_content.substr(last_pos, pos - last_pos));
                break;
            }


            auto val = sub_content.substr(last_pos, pos - last_pos);
            kv_mapper[std::string(key)].emplace_back(val);

            last_pos = pos + 1;
        }
    }

    auto file_name = std::string("mr_out_") + std::to_string(index);
    auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, file_name);

    if (mknode_res.is_err()) {
        std::cerr << "Error in mknode" << std::endl;
        return;
    }

    std::vector<chfs::u8> content_vec;
    std::string content_str;
    for (auto &kv: kv_mapper) {
        content_str += Reduce(kv.first, kv.second) + " ";
    }
    content_vec.resize(content_str.size());
    memcpy(content_vec.data(), content_str.data(), content_str.size());
    //    std::cout << content_str << std::endl;

    auto write_res = chfs_client->write_file(mknode_res.unwrap(), 0, content_vec);

    if (write_res.is_err()) {
        std::cerr << "Error in write file" << std::endl;
        return;
    } else {
        doSubmit(mapReduce::mr_tasktype::REDUCE, index);
    }
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
    // Lab4: Your code goes here.
    mr_client->call(SUBMIT_TASK, id, static_cast<int>(taskType), index);
}

void Worker::doSum(int nreduce) {
    size_t offset = 0;
    auto id_res = chfs_client->lookup(1, outPutFile);
    auto w_inode_id = id_res.unwrap();

    for (int i = 0; i < nreduce; i++) {
        auto file_name = std::string("mr_out_") + std::to_string(i);
        auto node_id_res = chfs_client->lookup(1, file_name);
        auto inode_id = node_id_res.unwrap();

        auto type_res = chfs_client->get_type_attr(inode_id);
        auto read_res = chfs_client->read_file(inode_id, 0, type_res.unwrap().second.size);
        if (read_res.is_err()) {
            std::cerr << "Error in read file" << std::endl;
            return;
        }
        auto char_vec = read_res.unwrap();
        size_t size = char_vec.size();

        auto write_res = chfs_client->write_file(w_inode_id, offset, std::move(char_vec));
        offset += size;
        if (write_res.is_err()) {
            doSubmit(mapReduce::mr_tasktype::SUM, -1);
            return;
        }
    }

    doSubmit(mapReduce::mr_tasktype::SUM, 0);
}

void Worker::stop() {
    shouldStop = true;
    work_thread->join();
}

void Worker::doWork() {
    id = std::hash<std::thread::id>{}(std::this_thread::get_id());
    while (!shouldStop) {
        auto res = mr_client->call(ASK_TASK, id);

        if (res.is_err()) {
            std::cerr << "Error in ask task" << std::endl;
            return;
        }

        auto task = res.unwrap()->as<std::tuple<int, int, std::string>>();
        auto task_type = static_cast<mr_tasktype>(std::get<0>(task));
        switch (task_type) {
            case MAP:
                doMap(std::get<1>(task), std::get<2>(task));
                break;
            case REDUCE: {
                int nfiles, nreduce;
                std::istringstream iss(std::get<2>(task));

                iss >> nfiles >> nreduce;

                doReduce(std::get<1>(task), nfiles, nreduce);
                break;
            }
            case SUM:
                doSum(std::get<1>(task));
                break;
            case NONE:
                break;
            default:
                std::cerr << "Unknown task" << std::endl;
                return;
        }


        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}
}// namespace mapReduce