#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
    switch (type) {
        case ServerType::DATA_SERVER:
            num_data_servers += 1;
            data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                            address, port, reliable)});
            break;
        case ServerType::METADATA_SERVER:
            metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
            break;
        default:
            std::cerr << "Unknown Type" << std::endl;
            exit(1);
    }

    return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
    auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
    if (res.is_err()) {
        return ChfsResult<inode_id_t>(res.unwrap_error());
    }

    auto inode_id = res.unwrap()->as<inode_id_t>();
    if (inode_id == 0) {
        return {ErrorType::INVALID_ARG};
    }

    return inode_id;
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
        -> ChfsNullResult {
    auto res = metadata_server_->call("unlink", parent, name);
    if (res.is_err()) {
        return {res.unwrap_error()};
    }
    return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
        -> ChfsResult<inode_id_t> {
    auto res = metadata_server_->call("lookup", parent, name);
    if (res.is_err()) {
        return ChfsResult<inode_id_t>(res.unwrap_error());
    }

    return {res.unwrap()->as<inode_id_t>()};
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
        -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
    auto res = metadata_server_->call("readdir", id);
    if (res.is_err()) {
        return {res.unwrap_error()};
    }
    return {res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>()};
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
        -> ChfsResult<std::pair<InodeType, FileAttr>> {
    auto res = metadata_server_->call("get_type_attr", id);
    if (res.is_err()) {
        return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
    }
    auto data = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
    return {std::make_pair(static_cast<InodeType>(std::get<4>(data)),
                           FileAttr{std::get<1>(data),
                                    std::get<2>(data), std::get<3>(data), std::get<0>(data)})};
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
        -> ChfsResult<std::vector<u8>> {
    auto res = metadata_server_->call("get_block_map", id);
    auto block_map = res.unwrap()->as<std::vector<BlockInfo>>();
    std::vector<u8> data;
    data.reserve(size);
    size_t off = offset / 4096;

    size_t block_num = (offset % 4096 + size + 4095) / 4096;

    size_t read_offset = offset % 4096;
    size_t remaining = size;

    for (size_t t = 0; t < block_num; t++) {
        if (remaining == 0) {
            break;
        }

        if (t + off >= block_map.size()) {
            return {ErrorType::INVALID_ARG};
        }

        auto block_info = block_map[t + off];
        size_t read_size = std::min(4096 - read_offset, remaining);
        remaining -= read_size;
        auto resf = data_servers_[std::get<1>(block_info)]->call("read_data", std::get<0>(block_info), read_offset, read_size, std::get<2>(block_info));
        if (resf.is_err()) {
            return {resf.unwrap_error()};
        }
        auto buf = resf.unwrap()->as<std::vector<u8>>();
        if (t == 0) {
            read_offset = 0;
        }

        data.insert(data.end(), buf.begin(), buf.end());
    }
    return {data};
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
        -> ChfsNullResult {
    auto res = metadata_server_->call("get_block_map", id);
    auto block_map = res.unwrap()->as<std::vector<BlockInfo>>();
    std::vector<u8> buffer;
    size_t num = offset / 4096;
    size_t total = (offset + data.size() + 4095) / 4096;
    if (total >= block_map.size()) {
        size_t sz = block_map.size();
        for (size_t i = sz; i < total; i++) {
            auto reff = metadata_server_->call("alloc_block", id);
            if (reff.is_err()) {
                return {reff.unwrap_error()};
            }
            auto block_info = reff.unwrap()->as<BlockInfo>();
            block_map.push_back(block_info);
        }
    }

    size_t offs = offset % 4096;
    size_t sz = data.size();
    for (size_t i = num; i < total; i++) {
        auto block_info = block_map[i];
        auto resf = data_servers_[std::get<1>(block_info)]->call("write_data", std::get<0>(block_info), offs, data);
        data = std::vector<u8>(data.begin() + std::min(4096 - offs, sz), data.end());
        sz -= std::min(4096 - offs, sz);
        if (resf.is_err()) {
            return {resf.unwrap_error()};
        }
        offs = 0;
    }

    return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
    auto res = metadata_server_->call("free_block", id, block_id, mac_id);
    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    return KNullOk;
}

}// namespace chfs