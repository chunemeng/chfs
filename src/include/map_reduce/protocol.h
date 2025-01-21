#include "distributed/client.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include <mutex>
#include <string>
#include <utility>
#include <vector>

//Lab4: Free to modify this file

namespace mapReduce {
struct KeyVal {
    KeyVal(std::string key, std::string val) : key(std::move(key)), val(std::move(val)) {}
    KeyVal() = default;
    std::string key;
    std::string val;
};

enum mr_tasktype {
    NONE = 0,
    MAP,
    REDUCE,
    SUM
};

std::vector<KeyVal> Map(const std::string &content);

std::string Reduce(const std::string &key, const std::vector<std::string> &values);

const std::string ASK_TASK = "ask_task";
const std::string SUBMIT_TASK = "submit_task";

struct MR_CoordinatorConfig {
    uint16_t port;
    std::string ip_address;
    std::string resultFile;
    std::shared_ptr<chfs::ChfsClient> client;

    MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                         std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                   resultFile(std::move(resultFile)), client(std::move(client)) {}
};

class SequentialMapReduce {
public:
    SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
    void doWork();

private:
    std::shared_ptr<chfs::ChfsClient> chfs_client;
    std::vector<std::string> files;
    std::string outPutFile;
};

class Coordinator {
private:
    using time_point = std::chrono::time_point<std::chrono::system_clock>;
    static constexpr int MAP_TASK = static_cast<int>(mr_tasktype::MAP);
    static constexpr int REDUCE_TASK = static_cast<int>(mr_tasktype::REDUCE);
    static constexpr int NONE_TASK = static_cast<int>(mr_tasktype::NONE);
    static constexpr int SUM_TASK = static_cast<int>(mr_tasktype::SUM);


    bool is_timeout(time_point t) {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - t).count() > 10;
    }


public:
    Coordinator(const MR_CoordinatorConfig &config, const std::vector<std::string> &files, int nReduce);
    std::tuple<int, int, std::string> askTask(size_t);
    int submitTask(size_t id, int taskType, int index);
    bool Done();

private:
    std::vector<std::string> files;
    std::mutex mtx;
    bool isFinished;
    bool isMapDone = false;
    bool isReduceDone = false;
    bool isSumDone = false;
    std::pair<size_t , time_point> assign_sum;


    // vector<bool> is bad :(
    std::vector<uint8_t> map_task_done;
    std::vector<uint8_t> reduce_task_done;
    std::vector<uint8_t> worker_idle;

    std::vector<std::pair<size_t, time_point>> map_task_assign;
    std::vector<std::pair<size_t, time_point>> reduce_task_assign;

    std::string nf_nr;
    int nReduce_;
    std::unique_ptr<chfs::RpcServer> rpc_server;
};

class Worker {
public:
    explicit Worker(MR_CoordinatorConfig config);
    void doWork();
    void stop();

private:
    void doMap(int index, const std::string &filename);
    void doReduce(int index, int nfiles,int nreduces);
    void doSubmit(mr_tasktype taskType, int index);
    void doSum(int nreduces);
    size_t id;

    std::string outPutFile;
    std::unique_ptr<chfs::RpcClient> mr_client;
    std::shared_ptr<chfs::ChfsClient> chfs_client;
    std::unique_ptr<std::thread> work_thread;
    bool shouldStop = false;
};
}// namespace mapReduce