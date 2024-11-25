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

    auto inode_type = static_cast<InodeType>(type);

    if (inode_type == InodeType::Unknown) {
        return 0;
    }

    inode_id_t inode_id = 0;
    if (inode_type == InodeType::Directory) {
        auto res = operation_->mkdir(parent, name.c_str());
        std::cout << "Failed to create directory " << std::endl;

        if (res.is_err()) {
            std::cout << "Failed to create directory " << (int) res.unwrap_error() << std::endl;
            return 0;
        }
        std::cout << "Creating directory " << name << " in " << parent << res.unwrap() << std::endl;
        inode_id = res.unwrap();
    } else {
        auto res = operation_->mkfile(parent, name.c_str());

        if (res.is_err()) {
            return 0;
        }
        inode_id = res.unwrap();
    }

    std::cout << "Creating node " << inode_id << " in " << parent << std::endl;
    return inode_id;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
        -> bool {
    std::vector<block_id_t> buffer;
    auto res = operation_->unlink_regular_file(parent, name.c_str(), buffer);
    if (!res.is_ok()) {
        return false;
    }

    for (int i = 0; i < buffer.size(); i += 2) {
        auto block_id = buffer[i];
        auto mac_id = buffer[i + 1];
        auto resu = clients_[mac_id]->call("free_block", block_id);
        if (resu.is_err()) {
            return false;
        }
    }

    return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
        -> inode_id_t {
    auto res = operation_->lookup(parent, name.c_str());
    if (res.is_ok()) {
        return res.unwrap();
    }

    return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
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

    for (const auto &cli_info: clients_) {
        auto cli = cli_info.second;
        auto res = cli->call("alloc_block");
        if (res.is_err()) {
            continue;
        }

        auto [block_id, version] = res.unwrap()->as<std::pair<block_id_t, version_t>>();

        auto resu = operation_->append_regular_file(id, block_id, cli_info.first);

        assert(resu.is_ok());

        return {block_id, cli_info.first, version};
    }


    return {};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
    auto mac_iter = clients_.find(machine_id);
    if (mac_iter == clients_.end()) {
        return false;
    }
    auto cli = mac_iter->second;

    auto res = cli->call("free_block", block_id);
    if (res.is_err()) {
        return false;
    }

    std::vector<u8> buffer;
    auto resu = operation_->read_file(id, buffer);
    if (resu.is_err()) {
        return false;
    }
    auto size = buffer.size();
    auto len = size / sizeof(block_id_t);
    auto ptr = reinterpret_cast<block_id_t *>(buffer.data());
    for (uint32_t i = 0; i < len; i += 2) {
        if (ptr[i] == block_id) {
            for (uint32_t j = i; j < len - 2; j += 2) {
                ptr[j] = ptr[j + 2];
                ptr[j + 1] = ptr[j + 3];
            }
            return true;
        }
    }


    return false;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
        -> std::vector<std::pair<std::string, inode_id_t>> {
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

    auto res = operation_->get_type_attr(id);
    if (res.is_err()) {
        return {};
    }

    auto resu = res.unwrap();
    auto type = resu.second;
    return {type.size, type.atime, type.mtime, type.ctime, static_cast<u8>(resu.first)};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
    num_data_servers += 1;
    auto cli = std::make_shared<RpcClient>(address, port, reliable);
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