#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
    server_->bind("mknode",
                  [this](u8 type, inode_id_t parent, std::string const &name) {
                      return this->mknode(type, parent, name);
                  });
    server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
        return this->unlink(parent, name);
    });
    server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
        return this->lookup(parent, name);
    });
    server_->bind("get_block_map",
                  [this](inode_id_t id) { return this->get_block_map(id); });
    server_->bind("alloc_block",
                  [this](inode_id_t id) { return this->allocate_block(id); });
    server_->bind("free_block",
                  [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                      return this->free_block(id, block, machine_id);
                  });
    server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
    server_->bind("get_type_attr",
                  [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
    /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
    bool is_initialed = is_file_exist(data_path);

    auto block_manager = std::shared_ptr<BlockManager>(nullptr);
    if (is_log_enabled_) {
        block_manager =
                std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
    } else {
        block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    }

    CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

    if (is_initialed) {
        auto origin_res = FileOperation::create_from_raw(block_manager);
        std::cout << "Restarting..." << std::endl;
        if (origin_res.is_err()) {
            std::cerr << "Original FS is bad, please remove files manually."
                      << std::endl;
            exit(1);
        }

        operation_ = origin_res.unwrap();
    } else {
        operation_ = std::make_shared<FileOperation>(block_manager,
                                                     DistributedMaxInodeSupported);
        std::cout << "We should init one new FS..." << std::endl;
        /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
        auto init_res = operation_->alloc_inode(InodeType::Directory);
        if (init_res.is_err()) {
            std::cerr << "Cannot allocate inode for root directory." << std::endl;
            exit(1);
        }

        CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
    }

    running = false;
    num_data_servers =
            0;// Default no data server. Need to call `reg_server` to add.

    if (is_log_enabled_) {
        if (may_failed_)
            operation_->block_manager_->set_may_fail(true);
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                 is_checkpoint_enabled_);
    }

    bind_handlers();

    /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
    server_ = std::make_unique<RpcServer>(port);
    init_fs(data_path);
    if (is_log_enabled_) {
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                 is_checkpoint_enabled);
    }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
    server_ = std::make_unique<RpcServer>(address, port);
    init_fs(data_path);
    if (is_log_enabled_) {
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                 is_checkpoint_enabled);
    }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
        -> inode_id_t {
    std::unique_lock lock(big_mutex);
    auto inode_type = static_cast<InodeType>(type);

    if (inode_type == InodeType::Unknown) {
        return 0;
    }
    if (!is_log_enabled_) {
        inode_id_t inode_id = 0;
        if (inode_type == InodeType::Directory) {
            auto res = operation_->mkdir(parent, name.c_str());

            if (res.is_err()) {
                return 0;
            }
            inode_id = res.unwrap();
        } else {
            auto res = operation_->mkfile(parent, name.c_str());

            if (res.is_err()) {
                return 0;
            }

            inode_id = res.unwrap();
        }

        return inode_id;
    } else {
        auto txn_id = commit_log->get_last_checkpoint_txn_id();
        bool is_err = false;
        std::vector<std::shared_ptr<BlockOperation>> ops;
        inode_id_t inode_id = 0;
        if (inode_type == InodeType::Directory) {
            auto res = operation_->mkdir(parent, name.c_str(), ops, is_err);
            if (res.is_err()) {
                return 0;
            }
            inode_id = is_err ? 0 : res.unwrap();
        } else {
            auto res = operation_->mkfile(parent, name.c_str(), ops, is_err);

            if (res.is_err()) {
                return 0;
            }
            inode_id = is_err ? 0 : res.unwrap();
        }

        commit_log->append_log(txn_id, std::move(ops));

        commit_log->commit_log(txn_id);


        return inode_id;
    }
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
        -> bool {
    std::unique_lock lock(big_mutex);
    std::vector<u8> buffer;
    bool is_err = false;

    if (!is_log_enabled_) {
        auto res = operation_->unlink_regular_file(parent, name.c_str(), buffer);
        if (!res.is_ok()) {
            return false;
        }
    } else {
        std::vector<std::shared_ptr<BlockOperation>> ops;
        auto res = operation_->unlink_regular_file(parent, name.c_str(), buffer, ops, is_err);
        if (!res.is_ok()) {
            return false;
        }

        auto txn_id = commit_log->get_last_checkpoint_txn_id();

        commit_log->append_log(txn_id, std::move(ops));
        commit_log->commit_log(txn_id);
    }


    std::vector<mac_id_t> data;
    std::vector<std::unique_lock<std::mutex>> locks;
    auto size = buffer.size() / sizeof(block_id_t);
    data.resize(size / 2);
    auto ptr = reinterpret_cast<block_id_t *>(buffer.data());

    for (int i = 0; i < size; i += 2) {
        data[i / 2] = ptr[i + 1];
    }

    std::sort(data.begin(), data.end());

    for (int i = 0; i < data.size(); i++) {
        auto resu = clients_[data[i]];
        locks.emplace_back(resu->latch_);
    }

    for (int i = 0; i < buffer.size(); i += 2) {
        auto block_id = buffer[i];
        auto mac_id = buffer[i + 1];
        auto resu = clients_[mac_id]->call("free_block", block_id);
        if (resu.is_err()) {
            return false;
        }
    }

    return !is_err;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
        -> inode_id_t {
    std::shared_lock lock(big_mutex);

    auto res = operation_->lookup(parent, name.c_str());
    if (res.is_ok()) {
        return res.unwrap();
    }

    return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
    std::shared_lock lock(big_mutex);
    std::vector<u8> result;
    auto res = operation_->read_as_regular_file(id, result);
    if (res.is_err()) {
        return {};
    }

    auto size = result.size() / sizeof(block_id_t);
    std::vector<BlockInfo> block_infos;
    auto ptr = reinterpret_cast<block_id_t *>(result.data());
    for (uint32_t i = 0; i < size; i += 2) {
        auto block_id = *reinterpret_cast<block_id_t *>(ptr + i);
        auto mac_id = *reinterpret_cast<mac_id_t *>(ptr + i + 1);
        auto version = clients_[mac_id]->call("get_version", block_id).unwrap()->as<uint32_t>();

        block_infos.emplace_back(block_id, mac_id, version);
    }

    return block_infos;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
    std::unique_lock lock(big_mutex);
    for (const auto &cli_info: clients_) {
        auto cli = cli_info.second;
        std::unique_lock<std::mutex> guard(cli->latch_);
        auto res = cli->call("alloc_block");
        guard.unlock();
        if (res.is_err()) {
            continue;
        }

        auto [block_id, version] = res.unwrap()->as<std::pair<block_id_t, version_t>>();
        std::vector<u8> buffer;
        buffer.resize(sizeof(block_id_t) * 2);
        auto ptr = reinterpret_cast<block_id_t *>(buffer.data());
        ptr[0] = block_id;
        ptr[1] = cli_info.first;

        auto resu = operation_->append_regular_file(id, block_id, cli_info.first);


        if (resu.is_err()) {
            std::cerr << "Failed to append block " << (int) resu.unwrap_error() << std::endl;
            return {};
        }

        return {block_id, cli_info.first, version};
    }


    return {};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
    std::unique_lock lock(big_mutex);
    auto mac_iter = clients_.find(machine_id);
    if (mac_iter == clients_.end()) {
        return false;
    }
    auto cli = mac_iter->second;

    std::unique_lock<std::mutex> guard(cli->latch_);

    auto res = cli->call("free_block", block_id);

    if (res.is_err()) {
        return false;
    }
    guard.unlock();

    std::vector<u8> buffer;

    auto resu = operation_->read_as_regular_file(id, buffer);

    if (resu.is_err()) {
        return false;
    }

    auto size = buffer.size();
    auto len = size / sizeof(block_id_t);
    auto ptr = reinterpret_cast<block_id_t *>(buffer.data());
    for (uint32_t i = 0; i < len; i += 2) {
        if (ptr[i] == block_id) {
            auto mac = ptr[i + 1];
            for (uint32_t j = i; j < len - 2; j += 2) {
                ptr[j] = ptr[j + 2];
                ptr[j + 1] = ptr[j + 3];
            }
            buffer.resize(size - 2 * sizeof(block_id_t));
            auto resddd = operation_->write_as_regular_file(id, buffer);

            if (resddd.is_err()) {
                return false;
            }

            cli = clients_[mac];
            std::unique_lock<std::mutex> guardf(cli->latch_);

            auto resff = cli->call("free_block", block_id);

            if (resff.is_err()) {
                return false;
            }

            return true;
        }
    }

    std::cout << "Failed to free block " << block_id << " in " << id << std::endl;
    return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
        -> std::vector<std::pair<std::string, inode_id_t>> {
    std::unique_lock lock(big_mutex);
    std::list<DirectoryEntry> list;
    auto res = read_directory(operation_, node, list);

    if (res.is_err()) {
        return {};
    }

    std::vector<std::pair<std::string, inode_id_t>> result;
    for (auto &entry: list) {
        result.emplace_back(entry.name, entry.id);
    }

    return result;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
        -> std::tuple<u64, u64, u64, u64, u8> {
    std::unique_lock lock(big_mutex);
    auto res = operation_->get_type_attr(id);
    if (res.is_err()) {
        return {};
    }

    auto resu = res.unwrap();
    auto type = resu.second;

    auto size = type.size;
    if (resu.first == InodeType::FILE) {
        size >>= 16;
        size = (size) / sizeof(block_id_t) / 2 * 4096;
    }

    return {size, type.atime, type.mtime, type.ctime, static_cast<u8>(resu.first)};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
    num_data_servers += 1;
    auto cli = std::make_shared<Client>(address, port, reliable);
    clients_.insert(std::make_pair(num_data_servers, cli));

    return true;
}

auto MetadataServer::run() -> bool {
    if (running)
        return false;

    // Currently we only support async start
    server_->run(true, num_worker_threads);
    running = true;
    return true;
}

}// namespace chfs