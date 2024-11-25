#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

#define VERSION_BLOCK_ID 1

uint32_t get_cur_version(const std::shared_ptr<BlockManager> &bm, block_id_t block_id) {
    std::vector<u8> buffer;
    buffer.resize(bm->block_size());
    auto res = bm->read_block(VERSION_BLOCK_ID, buffer.data());

    if (!res.is_ok()) {
        return 0;
    }

    auto offset = (block_id - VERSION_BLOCK_ID) * sizeof(uint32_t);

    uint32_t version = 0;

    memcpy(&version, buffer.data() + offset, sizeof(version));
    version++;
    memcpy(buffer.data() + offset, &version, sizeof(version));
    res = bm->write_partial_block(VERSION_BLOCK_ID, buffer.data() + offset, offset, sizeof(version));

    if (!res.is_ok()) {
        return 0;
    }

    return version;
}

uint32_t get_version(const std::shared_ptr<BlockManager> &bm, block_id_t block_id) {
    std::vector<u8> buffer;
    buffer.resize(bm->block_size());
    auto res = bm->read_block(VERSION_BLOCK_ID, buffer.data());

    if (!res.is_ok()) {
        return 0;
    }

    auto offset = (block_id - VERSION_BLOCK_ID) * sizeof(uint32_t);

    uint32_t version = 0;

    memcpy(&version, buffer.data() + offset, sizeof(version));

    return version;
}

auto DataServer::initialize(std::string const &data_path) {
    /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
    bool is_initialized = is_file_exist(data_path);

    auto bm = std::shared_ptr<BlockManager>(
            new BlockManager(data_path, KDefaultBlockCnt));
    if (is_initialized) {
        block_allocator_ =
                std::make_shared<BlockAllocator>(bm, 0, false);
    } else {
        // We need to reserve some blocks for storing the version of each block
        block_allocator_ = std::shared_ptr<BlockAllocator>(
                new BlockAllocator(bm, 0, true));
        auto res = block_allocator_->allocate();
        std::cout << "Initializing the version block" << res.unwrap() << std::endl;
    }

    // Initialize the RPC server and bind all handlers
    server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                      usize len, version_t version) {
        return this->read_data(block_id, offset, len, version);
    });
    server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                       std::vector<u8> &buffer) {
        return this->write_data(block_id, offset, buffer);
    });
    server_->bind("alloc_block", [this]() { return this->alloc_block(); });
    server_->bind("free_block", [this](block_id_t block_id) {
        return this->free_block(block_id);
    });

    server_->bind("get_version", [this](block_id_t block_id) {
        return get_version(block_allocator_->bm, block_id);
    });

    // Launch the rpc server to listen for requests
    server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
    initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
    initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {

    auto ver = get_version(block_allocator_->bm, block_id);
    if (ver != version) {
        return {};
    }
    std::vector<u8> buffer;
    buffer.resize(block_allocator_->bm->block_size());
    auto result = block_allocator_->bm->read_block(block_id, buffer.data());
    if (!result.is_ok()) {
        return {};
    }

    return {buffer.begin() + offset, buffer.begin() + offset + len};
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
    std::vector<u8> block_data;
    block_data.resize(block_allocator_->bm->block_size());
    auto result = block_allocator_->bm->read_block(block_id, block_data.data());
    if (!result.is_ok()) {
        return false;
    }

    memcpy(block_data.data() + offset, buffer.data(), std::min(buffer.size(), static_cast<size_t>(4096 - offset)));

    result = block_allocator_->bm->write_block(block_id, block_data.data());

    if (!result.is_ok()) {
        return false;
    }

    return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
    auto result = block_allocator_->allocate();
    if (result.is_err()) {
        return {0, 0};
    }
    uint32_t version = get_cur_version(block_allocator_->bm, result.unwrap());

    if (version == 0) {
        return {0, 0};
    }


    return {result.unwrap(), version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
    auto result = block_allocator_->deallocate(block_id);
    if (result.is_ok()) {
        uint32_t version = get_cur_version(block_allocator_->bm, block_id);
        if (version == 0) {
            return false;
        }
        return true;
    }


    return false;
}
}// namespace chfs