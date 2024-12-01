#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {
    inode_id_t inode_id = static_cast<inode_id_t>(0);
    auto inode_res = ChfsResult<inode_id_t>(inode_id);

    auto bid = block_allocator_->allocate();

    inode_res = inode_manager_->allocate_inode(type, bid.unwrap());

    return inode_res;
}

auto FileOperation::alloc_inode(InodeType type, std::vector<std::shared_ptr<BlockOperation>> &ops, bool &is_err) -> ChfsResult<inode_id_t> {
    inode_id_t inode_id = static_cast<inode_id_t>(0);
    auto inode_res = ChfsResult<inode_id_t>(inode_id);

    auto bid = block_allocator_->allocate();

    inode_res = inode_manager_->allocate_inode(type, bid.unwrap(), ops, is_err);

    return inode_res;
}


auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
    return this->inode_manager_->get_attr(id);
}

auto FileOperation::get_type_attr(inode_id_t id)
        -> ChfsResult<std::pair<InodeType, FileAttr>> {
    return this->inode_manager_->get_type_attr(id);
}

auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
    return this->inode_manager_->get_type(id);
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
    return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                     u64 offset) -> ChfsResult<u64> {
    auto read_res = this->read_file(id);
    if (read_res.is_err()) {
        return ChfsResult<u64>(read_res.unwrap_error());
    }

    auto content = read_res.unwrap();
    if (offset + sz > content.size()) {
        content.resize(offset + sz);
    }
    memcpy(content.data() + offset, data, sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err()) {
        return ChfsResult<u64>(write_res.unwrap_error());
    }
    return ChfsResult<u64>(sz);
}

// {Your code here}
auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
        -> ChfsNullResult {
    auto error_code = ErrorType::DONE;
    const auto block_size = this->block_manager_->block_size();
    usize old_block_num = 0;
    usize new_block_num = 0;
    u64 original_file_sz = 0;

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    auto inlined_blocks_num = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        goto err_ret;
    } else {
        inlined_blocks_num = inode_p->get_direct_block_num();
    }

    if (content.size() > inode_p->max_file_sz_supported()) {
        std::cerr << "file size too large: " << content.size() << " vs. "
                  << inode_p->max_file_sz_supported() << std::endl;
        error_code = ErrorType::OUT_OF_RESOURCE;
        goto err_ret;
    }

    // 2. make sure whether we need to allocate more blocks
    original_file_sz = inode_p->get_size();
    old_block_num = calculate_block_sz(original_file_sz, block_size);
    new_block_num = calculate_block_sz(content.size(), block_size);

    if (new_block_num > old_block_num) {
        // If we need to allocate more blocks.
        for (usize idx = old_block_num; idx < new_block_num; ++idx) {
            if (inode_p->is_direct_block(idx)) {
                auto block_res = this->block_allocator_->allocate();

                if (block_res.is_err()) {
                    std::cerr << "allocate block error: " << (int) block_res.unwrap_error()
                              << std::endl;
                    error_code = block_res.unwrap_error();
                    goto err_ret;
                }

                inode_p->blocks[idx] = block_res.unwrap();
            } else {
                if (indirect_block.empty()) [[unlikely]] {
                    auto indirect_block_id =
                            inode_p->get_or_insert_indirect_block(block_allocator_);

                    if (indirect_block_id.is_err()) {
                        std::cerr << "allocate indirect block error: "
                                  << (int) indirect_block_id.unwrap_error() << std::endl;
                        error_code = indirect_block_id.unwrap_error();
                        goto err_ret;
                    }

                    indirect_block.resize(block_size);
                    auto res = block_manager_->read_block(indirect_block_id.unwrap(),
                                                          indirect_block.data());

                    if (res.is_err()) {
                        std::cerr << "read indirect block error: "
                                  << (int) res.unwrap_error() << std::endl;
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                }

                auto res = this->block_allocator_->allocate();
                if (res.is_err()) {
                    std::cerr << "allocate block indirect error: "
                              << (int) res.unwrap_error() << std::endl;
                    error_code = res.unwrap_error();
                    goto err_ret;
                }

                *reinterpret_cast<block_id_t *>(indirect_block.data() +
                                                (idx - inlined_blocks_num) *
                                                        sizeof(block_id_t)) = res.unwrap();
            }
        }
    } else {
        // We need to free the extra blocks.
        for (usize idx = new_block_num; idx < old_block_num; ++idx) {
            if (inode_p->is_direct_block(idx)) {
                auto res = this->block_allocator_->deallocate(inode_p->blocks[idx]);

                if (res.is_err()) {
                    std::cerr << "deallocate block error: " << (int) res.unwrap_error()
                              << std::endl;
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
            } else {
                if (indirect_block.empty()) [[unlikely]] {
                    auto indirect_block_id = inode_p->get_indirect_block_id();

                    indirect_block.resize(block_size);

                    auto res = block_manager_->read_block(indirect_block_id,
                                                          indirect_block.data());
                    if (res.is_err()) {
                        std::cerr << "read indirect block error: "
                                  << (int) res.unwrap_error() << std::endl;
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                }

                auto block_ptr = reinterpret_cast<block_id_t *>(
                        indirect_block.data() +
                        (idx - inlined_blocks_num) * sizeof(block_id_t));

                auto block_id = *block_ptr;

                *block_ptr = KInvalidBlockID;

                auto res = this->block_allocator_->deallocate(block_id);
                if (res.is_err()) {
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
            }
        }

        // If there are no more indirect blocks.
        if (old_block_num > inlined_blocks_num &&
            new_block_num <= inlined_blocks_num && true) {

            auto res =
                    this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
            if (res.is_err()) {
                error_code = res.unwrap_error();
                goto err_ret;
            }
            indirect_block.clear();
            inode_p->invalid_indirect_block_id();
        }
    }

    // 3. write the contents
    inode_p->inner_attr.size = content.size();
    inode_p->inner_attr.mtime = time(0);

    {
        auto block_idx = 0;
        u64 write_sz = 0;

        while (write_sz < content.size()) {
            auto sz = ((content.size() - write_sz) > block_size)
                              ? block_size
                              : (content.size() - write_sz);
            std::vector<u8> buffer(block_size);
            memcpy(buffer.data(), content.data() + write_sz, sz);

            auto block_id = KInvalidBlockID;
            if (inode_p->is_direct_block(block_idx)) {
                block_id = inode_p->blocks[block_idx];
            } else {
                if (indirect_block.empty()) [[unlikely]] {
                    auto indirect_block_id = inode_p->get_indirect_block_id();
                    auto res = block_manager_->read_block(indirect_block_id,
                                                          indirect_block.data());

                    if (res.is_err()) {
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                }

                block_id = *reinterpret_cast<block_id_t *>(
                        indirect_block.data() +
                        (block_idx - inlined_blocks_num) * sizeof(block_id_t));
            }

            auto res = block_manager_->write_block(block_id, buffer.data());

            if (res.is_err()) {
                error_code = res.unwrap_error();
                goto err_ret;
            }

            write_sz += sz;
            block_idx += 1;
        }
    }

    // finally, update the inode
    {
        inode_p->inner_attr.set_all_time(time(0));

        auto write_res =
                this->block_manager_->write_block(inode_res.unwrap(), inode.data());
        if (write_res.is_err()) {
            error_code = write_res.unwrap_error();
            goto err_ret;
        }
        if (indirect_block.size() != 0) {
            bool is_err = false;
            write_res =
                    inode_p->write_indirect_block(this->block_manager_, indirect_block, is_err);
            if (write_res.is_err()) {
                error_code = write_res.unwrap_error();
                goto err_ret;
            }
            if (is_err) {
                error_code = ErrorType::INVALID_ARG;
                goto err_ret;
            }
        }
    }

    return KNullOk;

err_ret:
    // std::cerr << "write file return error: " << (int)error_code <<
    // std::endl;
    return ChfsNullResult(error_code);
}

auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content, std::vector<std::shared_ptr<BlockOperation>> &ops, bool &is_err)
        -> ChfsNullResult {
    auto error_code = ErrorType::DONE;
    const auto block_size = this->block_manager_->block_size();
    usize old_block_num = 0;
    usize new_block_num = 0;
    u64 original_file_sz = 0;

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    auto inlined_blocks_num = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        goto err_ret;
    } else {
        inlined_blocks_num = inode_p->get_direct_block_num();
    }

    if (content.size() > inode_p->max_file_sz_supported()) {
        std::cerr << "file size too large: " << content.size() << " vs. "
                  << inode_p->max_file_sz_supported() << std::endl;
        error_code = ErrorType::OUT_OF_RESOURCE;
        goto err_ret;
    }

    // 2. make sure whether we need to allocate more blocks
    original_file_sz = inode_p->get_size();
    old_block_num = calculate_block_sz(original_file_sz, block_size);
    new_block_num = calculate_block_sz(content.size(), block_size);

    if (new_block_num > old_block_num) {
        // If we need to allocate more blocks.
        for (usize idx = old_block_num; idx < new_block_num; ++idx) {
            if (inode_p->is_direct_block(idx)) {
                auto block_res = this->block_allocator_->allocate();

                if (block_res.is_err()) {
                    std::cerr << "allocate block error: " << (int) block_res.unwrap_error()
                              << std::endl;
                    error_code = block_res.unwrap_error();
                    goto err_ret;
                }

                inode_p->blocks[idx] = block_res.unwrap();
            } else {
                if (indirect_block.empty()) [[unlikely]] {
                    auto indirect_block_id =
                            inode_p->get_or_insert_indirect_block(block_allocator_);

                    if (indirect_block_id.is_err()) {
                        std::cerr << "allocate indirect block error: "
                                  << (int) indirect_block_id.unwrap_error() << std::endl;
                        error_code = indirect_block_id.unwrap_error();
                        goto err_ret;
                    }

                    indirect_block.resize(block_size);
                    auto res = block_manager_->read_block(indirect_block_id.unwrap(),
                                                          indirect_block.data());

                    if (res.is_err()) {
                        std::cerr << "read indirect block error: "
                                  << (int) res.unwrap_error() << std::endl;
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                }

                auto res = this->block_allocator_->allocate();
                if (res.is_err()) {
                    std::cerr << "allocate block indirect error: "
                              << (int) res.unwrap_error() << std::endl;
                    error_code = res.unwrap_error();
                    goto err_ret;
                }

                *reinterpret_cast<block_id_t *>(indirect_block.data() +
                                                (idx - inlined_blocks_num) *
                                                        sizeof(block_id_t)) = res.unwrap();
            }
        }
    } else {
        // We need to free the extra blocks.
        for (usize idx = new_block_num; idx < old_block_num; ++idx) {
            if (inode_p->is_direct_block(idx)) {
                auto res = this->block_allocator_->deallocate(inode_p->blocks[idx]);

                if (res.is_err()) {
                    std::cerr << "deallocate block error: " << (int) res.unwrap_error()
                              << std::endl;
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
            } else {
                if (indirect_block.empty()) [[unlikely]] {
                    auto indirect_block_id = inode_p->get_indirect_block_id();

                    indirect_block.resize(block_size);

                    auto res = block_manager_->read_block(indirect_block_id,
                                                          indirect_block.data());
                    if (res.is_err()) {
                        std::cerr << "read indirect block error: "
                                  << (int) res.unwrap_error() << std::endl;
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                }

                auto block_ptr = reinterpret_cast<block_id_t *>(
                        indirect_block.data() +
                        (idx - inlined_blocks_num) * sizeof(block_id_t));

                auto block_id = *block_ptr;

                *block_ptr = KInvalidBlockID;

                auto res = this->block_allocator_->deallocate(block_id);
                if (res.is_err()) {
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
            }
        }

        // If there are no more indirect blocks.
        if (old_block_num > inlined_blocks_num &&
            new_block_num <= inlined_blocks_num && true) {

            auto res =
                    this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
            if (res.is_err()) {
                error_code = res.unwrap_error();
                goto err_ret;
            }
            indirect_block.clear();
            inode_p->invalid_indirect_block_id();
        }
    }

    // 3. write the contents
    inode_p->inner_attr.size = content.size();
    inode_p->inner_attr.mtime = time(0);

    {
        auto block_idx = 0;
        u64 write_sz = 0;

        while (write_sz < content.size()) {
            auto sz = ((content.size() - write_sz) > block_size)
                              ? block_size
                              : (content.size() - write_sz);
            std::vector<u8> buffer(block_size);
            memcpy(buffer.data(), content.data() + write_sz, sz);

            auto block_id = KInvalidBlockID;
            if (inode_p->is_direct_block(block_idx)) {
                block_id = inode_p->blocks[block_idx];
            } else {
                if (indirect_block.empty()) [[unlikely]] {
                    auto indirect_block_id = inode_p->get_indirect_block_id();
                    auto res = block_manager_->read_block(indirect_block_id,
                                                          indirect_block.data());

                    if (res.is_err()) {
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                }

                block_id = *reinterpret_cast<block_id_t *>(
                        indirect_block.data() +
                        (block_idx - inlined_blocks_num) * sizeof(block_id_t));
            }

            ops.emplace_back(std::make_shared<BlockOperation>(block_id, buffer));
            auto ress = block_manager_->write_block(block_id, buffer.data());
            if (ress.is_err()) {
                is_err = true;
            }

            write_sz += sz;
            block_idx += 1;
        }
    }

    // finally, update the inode
    {
        inode_p->inner_attr.set_all_time(time(0));
        ops.emplace_back(std::make_shared<BlockOperation>(inode_res.unwrap(), inode));
        auto ress = this->block_manager_->write_block(inode_res.unwrap(), inode.data());
        if (ress.is_err()) {
            is_err = true;
        }

        if (indirect_block.size() != 0) {
            ops.emplace_back(std::make_shared<BlockOperation>(inode_p->get_indirect_block_id(), indirect_block));
            inode_p->write_indirect_block(this->block_manager_, indirect_block, is_err);
        }
    }

    return KNullOk;

err_ret:
    // std::cerr << "write file return error: " << (int)error_code <<
    // std::endl;
    return ChfsNullResult(error_code);
}

// {Your code here}
auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
    auto error_code = ErrorType::DONE;
    std::vector<u8> content;

    const auto block_size = this->block_manager_->block_size();

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    u64 file_sz = 0;
    u64 read_sz = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        goto err_ret;
    }

    file_sz = inode_p->get_size();
    content.reserve(file_sz);

    // Now read the file
    while (read_sz < file_sz) {
        auto sz = ((inode_p->get_size() - read_sz) > block_size)
                          ? block_size
                          : (inode_p->get_size() - read_sz);
        std::vector<u8> buffer(block_size);

        block_id_t block_id = 0;

        // Get current block id.
        if (inode_p->is_direct_block(read_sz / block_size)) {
            block_id = inode_p->blocks[read_sz / block_size];
        } else {
            if (indirect_block.empty()) {
                auto indirect_block_id = inode_p->get_indirect_block_id();
                indirect_block.resize(block_size);
                auto res = block_manager_->read_block(indirect_block_id,
                                                      indirect_block.data());

                if (res.is_err()) {
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
            }
            block_id = *reinterpret_cast<block_id_t *>(
                    indirect_block.data() +
                    (read_sz / block_size - inode_p->get_direct_block_num()) *
                            sizeof(block_id_t));
        }

        auto res = block_manager_->read_block(block_id, buffer.data());

        if (res.is_err()) {
            error_code = res.unwrap_error();
            goto err_ret;
        }

        content.insert(content.end(), buffer.begin(), buffer.begin() + sz);

        read_sz += sz;
    }

    return ChfsResult<std::vector<u8>>(std::move(content));

err_ret:
    return ChfsResult<std::vector<u8>>(error_code);
}

auto FileOperation::read_file(inode_id_t id, std::vector<u8> &content) -> ChfsNullResult {
    auto error_code = ErrorType::DONE;

    const auto block_size = this->block_manager_->block_size();

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    u64 file_sz = 0;
    u64 read_sz = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        goto err_ret;
    }

    file_sz = inode_p->get_size();
    content.reserve(file_sz);

    // Now read the file
    while (read_sz < file_sz) {
        auto sz = ((inode_p->get_size() - read_sz) > block_size)
                          ? block_size
                          : (inode_p->get_size() - read_sz);
        std::vector<u8> buffer(block_size);

        block_id_t block_id = 0;

        // Get current block id.
        if (inode_p->is_direct_block(read_sz / block_size)) {
            block_id = inode_p->blocks[read_sz / block_size];
        } else {
            if (indirect_block.empty()) {
                auto indirect_block_id = inode_p->get_indirect_block_id();
                indirect_block.resize(block_size);
                auto res = block_manager_->read_block(indirect_block_id,
                                                      indirect_block.data());

                if (res.is_err()) {
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
            }
            block_id = *reinterpret_cast<block_id_t *>(
                    indirect_block.data() +
                    (read_sz / block_size - inode_p->get_direct_block_num()) *
                            sizeof(block_id_t));
        }

        auto res = block_manager_->read_block(block_id, buffer.data());

        if (res.is_err()) {
            error_code = res.unwrap_error();
            goto err_ret;
        }

        content.insert(content.end(), buffer.begin(), buffer.begin() + sz);

        read_sz += sz;
    }

    return KNullOk;

err_ret:
    return error_code;
}

auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
        -> ChfsResult<std::vector<u8>> {
    auto res = read_file(id);
    if (res.is_err()) {
        return res;
    }

    auto content = res.unwrap();
    return ChfsResult<std::vector<u8>>(
            std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
}

auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
    auto attr_res = this->getattr(id);
    if (attr_res.is_err()) {
        return ChfsResult<FileAttr>(attr_res.unwrap_error());
    }

    auto attr = attr_res.unwrap();
    auto file_content = this->read_file(id);
    if (file_content.is_err()) {
        return ChfsResult<FileAttr>(file_content.unwrap_error());
    }

    auto content = file_content.unwrap();

    if (content.size() != sz) {
        content.resize(sz);

        auto write_res = this->write_file(id, content);
        if (write_res.is_err()) {
            return ChfsResult<FileAttr>(write_res.unwrap_error());
        }
    }

    attr.size = sz;
    return ChfsResult<FileAttr>(attr);
}

auto FileOperation::read_as_regular_file(inode_id_t id, std::vector<u8> &content) -> ChfsNullResult {
    auto error_code = ErrorType::DONE;

    const auto block_size = this->block_manager_->block_size();

    // 1. read the inode
    std::vector<u8> inode(block_size);


    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        return {error_code};
    }
    auto inode_p = reinterpret_cast<Inode *>(inode.data());

    auto size = inode_p->inner_attr.size;
    auto block_num = size & 0xffff;
    auto content_num = size >> 16;
    content.resize(content_num);
    size_t s = 0;

    for (int i = 0; i < block_num; ++i) {
        if (content_num == 0 || s >= content.size()) {
            break;
        }

        auto bid = inode_p->blocks[i];
        std::vector<u8> buffer(block_size);
        auto res = this->block_manager_->read_block(bid, buffer.data());
        if (res.is_err()) {
            error_code = res.unwrap_error();
            return {error_code};
        }
        auto read_sz = std::min(content_num, (size_t) block_size);

        memcpy(content.data() + s, buffer.data(), read_sz);
        content_num -= read_sz;
        s += read_sz;
    }

    return KNullOk;
}

auto FileOperation::append_regular_file(inode_id_t id, block_id_t block_id, uint32_t mac_id) -> ChfsNullResult {
    auto error_code = ErrorType::DONE;
    const auto block_size = this->block_manager_->block_size();
    // 1. read the inode
    std::vector<u8> inode(block_size);


    auto inode_res = this->inode_manager_->read_inode(id, inode);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());

    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        return {error_code};
    }

    auto sz = inode_p->get_size();
    auto block_num = sz & 0xffff;
    auto content_num = sz >> 16;
    if (content_num % 4096 == 0) {
        auto block_res = this->block_allocator_->allocate();
        if (block_res.is_err()) {
            std::cerr << "allocate block error: " << (int) block_res.unwrap_error()
                      << std::endl;
            error_code = block_res.unwrap_error();
            return {error_code};
        }
        std::vector<u8> buffer(block_size);
        auto ptr = reinterpret_cast<block_id_t *>(buffer.data());
        ptr[0] = block_id;
        ptr[1] = mac_id;
        auto write_res = this->block_manager_->write_block(block_res.unwrap(), buffer.data());
        if (write_res.is_err()) {
            error_code = write_res.unwrap_error();
            return {error_code};
        }


        inode_p->blocks[block_num] = block_res.unwrap();

        block_num += 1;

    } else {
        auto bid = inode_p->blocks[block_num - 1];
        std::vector<u8> buffer(block_size);
        auto res = this->block_manager_->read_block(bid, buffer.data());
        if (res.is_err()) {
            error_code = res.unwrap_error();
            return {error_code};
        }
        auto offset = content_num % 4096 / sizeof(block_id_t);

        auto ptr = reinterpret_cast<block_id_t *>(buffer.data());

        ptr[offset] = block_id;
        ptr[offset + 1] = mac_id;

        auto write_res = this->block_manager_->write_block(bid, buffer.data());
        if (write_res.is_err()) {
            error_code = write_res.unwrap_error();
            return {error_code};
        }
    }
    inode_p->inner_attr.set_all_time(time(0));
    content_num += sizeof(block_id_t) * 2;
    inode_p->inner_attr.size = content_num << 16 | block_num;

    auto write_res =
            this->block_manager_->write_block(inode_res.unwrap(), inode.data());

    if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        return {error_code};
    }

    return KNullOk;
}

auto FileOperation::write_as_regular_file(inode_id_t id, const std::vector<u8> &content) -> ChfsNullResult {
    auto error_code = ErrorType::DONE;

    const auto block_size = this->block_manager_->block_size();

    // 1. read the inode
    std::vector<u8> inode(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err()) {
        error_code = inode_res.unwrap_error();
        // I know goto is bad, but we have no choice
        return {error_code};
    }
    auto size = inode_p->inner_attr.size;
    auto block_num = size & 0xffff;
    auto old_size = size >> 16;
    auto new_size = content.size();
    if (content.size() != old_size - 2 * sizeof(block_id_t)) {
        error_code = ErrorType::INVALID_ARG;
        return {error_code};
    }

    auto new_block_size = (new_size + block_size - 1) / block_size;
    if (new_block_size < block_num) {
        for (int i = new_block_size; i < block_num; ++i) {
            auto bid = inode_p->blocks[i];
            auto res = this->block_allocator_->deallocate(bid);
            if (res.is_err()) {
                error_code = res.unwrap_error();
                return {error_code};
            }
        }
    }

    auto new_sizz = new_size;
    for (size_t i = 0; i < new_block_size; ++i) {
        auto bid = inode_p->blocks[i];
        std::vector<u8> buffer;
        buffer.resize(block_size);
        auto sz = std::min((size_t) block_size, new_size);
        memcpy(buffer.data(), content.data() + i * block_size, sz);
        auto write_res = this->block_manager_->write_block(bid, buffer.data());
        if (write_res.is_err()) {
            error_code = write_res.unwrap_error();
            return {error_code};
        }
        new_size -= sz;
    }


    inode_p->inner_attr.size = new_sizz << 16 | new_block_size;

    inode_p->inner_attr.set_all_time(time(0));
    auto write_res =
            this->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        return {error_code};
    }

    return KNullOk;
}


}// namespace chfs
