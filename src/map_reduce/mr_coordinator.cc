#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {

std::tuple<int, int, std::string> Coordinator::askTask(size_t id) {
    std::unique_lock<std::mutex> lock(this->mtx);

    if (this->isFinished) {
        return std::make_tuple(NONE_TASK, -1, "");
    }

    if (!this->isMapDone) {
        for (size_t i = 0; i < this->map_task_done.size(); i++) {
            if (this->map_task_done[i] == 0) {
                if (this->map_task_assign[i].first == -1) {
                    this->map_task_assign[i] = std::make_pair(id, std::chrono::system_clock::now());
                } else {
                    if (this->is_timeout(this->map_task_assign[i].second)) {
                        this->map_task_assign[i] = std::make_pair(id, std::chrono::system_clock::now());
                    } else {
                        continue;
                    }
                }
                return std::make_tuple(MAP_TASK, i, files[i]);
            }
        }
        return std::make_tuple(NONE_TASK, -1, "");
    }

    if (!isReduceDone) {
        for (size_t i = 0; i < this->reduce_task_done.size(); i++) {
            if (this->reduce_task_done[i] == 0) {
                if (this->reduce_task_assign[i].first == -1) {
                    this->reduce_task_assign[i] = std::make_pair(id, std::chrono::system_clock::now());
                } else {
                    if (this->is_timeout(this->reduce_task_assign[i].second)) {
                        this->reduce_task_assign[i] = std::make_pair(id, std::chrono::system_clock::now());
                    } else {
                        continue;
                    }
                }
                return std::make_tuple(REDUCE_TASK, i, nf_nr);
            }
        }
        return std::make_tuple(NONE_TASK, -1, "");
    } else {
        if (!isSumDone) {
            if (this->assign_sum.first == -1 || this->is_timeout(this->assign_sum.second)) {
                this->assign_sum = std::make_pair(id, std::chrono::system_clock::now());
                return std::make_tuple(SUM_TASK, nReduce_, "");
            }
        }
    }
    return std::make_tuple(NONE_TASK, 0, "");
}

int Coordinator::submitTask(size_t id, int taskType, int index) {
    std::unique_lock<std::mutex> lock(this->mtx);
    auto type = static_cast<mr_tasktype>(taskType);

    switch (type) {
        case mr_tasktype::MAP: {
            if (this->map_task_assign[index].first == id) {
                this->map_task_done[index] = 1;
                if (std::all_of(this->map_task_done.begin(), this->map_task_done.end(), [](uint8_t i) { return i == 1; })) {
                    this->isMapDone = true;
                }
            }
            break;
        }
        case mr_tasktype::REDUCE: {
            if (this->reduce_task_assign[index].first == id) {
                this->reduce_task_done[index] = 1;
                if (std::all_of(this->reduce_task_done.begin(), this->reduce_task_done.end(), [](uint8_t i) { return i == 1; })) {
                    this->isReduceDone = true;
                }
            }
        } break;
        case mr_tasktype::SUM:
            if (index == -1) {
                isSumDone = false;
            } else if (this->assign_sum.first == id) {
                this->isSumDone = true;
                this->isFinished = true;
            }
            break;
        case NONE:
            break;
    }

    return 0;
}

// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
bool Coordinator::Done() {
    std::unique_lock<std::mutex> uniqueLock(this->mtx);
    return this->isFinished;
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
Coordinator::Coordinator(const MR_CoordinatorConfig &config, const std::vector<std::string> &files, int nReduce) {
    std::unique_lock<std::mutex> lock(this->mtx);
    this->files = files;
    this->isFinished = false;
    // Lab4: Your code goes here (Optional).
    map_task_done.resize(files.size(), 0);
    reduce_task_done.resize(files.size(), 0);

    map_task_assign.resize(files.size(), std::make_pair(-1, std::chrono::system_clock::now()));
    reduce_task_assign.resize(files.size(), std::make_pair(-1, std::chrono::system_clock::now()));

    this->nf_nr = std::to_string(files.size()) + " " + std::to_string(nReduce);
    nReduce_ = nReduce;

    rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
    rpc_server->bind(ASK_TASK, [this](size_t i) { return this->askTask(i); });
    rpc_server->bind(SUBMIT_TASK, [this](size_t id, int taskType, int index) { return this->submitTask(id, taskType, index); });
    rpc_server->run(true, 1);
}
}// namespace mapReduce