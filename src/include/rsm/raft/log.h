#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include "rsm/raft/protocol.h"
#include <cstring>
#include <mutex>
#include <vector>

namespace chfs {

struct Entry {
    uint32_t term{};
    uint32_t offset{};
    Entry() = default;
    Entry(uint32_t term, uint32_t offset) : term(term), offset(offset) {}
};

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template<typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here */

    std::pair<uint32_t, uint32_t> get_least_log_state() {
        std::lock_guard<std::mutex> lock(mtx);
        return std::make_pair(last_log_index_, last_log_term_);
    }

    void append_entries(uint32_t prev_log_index, std::vector<entry> &&entries) {
        std::lock_guard<std::mutex> lock(mtx);
        if (entries.empty()) {
            return;
        }
        auto size = entries.size();
        auto last_log_index = last_log_index_;
        auto data_index = data_index_;
        size_t i = 0;
        for (; i < size; i++) {
            if (last_log_index == 0 || prev_log_index + i >= last_log_index) {
                break;
            }

            auto off = prev_log_index + i - block_offset_;

            // a different log
            if (buffer_entry_[off].term != entries[i].term) {
                auto offset = buffer_entry_[off].offset;
                memset(data_ + offset, 0, data_index_ - offset);
                data_index_ = offset;
                last_log_index_ = prev_log_index + i;
                buffer_entry_.resize(last_log_index_);
                break;
            }
        }

        // safely append
        for (; i < size; i++) {
            append_log_wo_aync(entries[i - block_offset_].term, std::move(entries[i - block_offset_].command), entries[i - block_offset_].size);
        }
        write_log_data();
    }

    void read_log(uint32_t index, uint32_t end_index, std::vector<entry> &entries) {
        std::lock_guard<std::mutex> lock(mtx);
        if (index < block_offset_) {
            return;
        }

        for (auto i = index; i < end_index; i++) {
            auto ind = i - block_offset_;
            auto offset = buffer_entry_[ind].offset;
            auto data_ptr = data_ + offset;
            auto sz = *reinterpret_cast<uint32_t *>(data_ptr) - meta_per_sz * 2;
            entry e;

            e.term = *(reinterpret_cast<uint32_t *>(data_ptr) + 1);

            e.command.resize(sz);
            e.size = sz;

            memcpy(e.command.data(), data_ptr + meta_per_sz * 2, sz);
            entries.emplace_back(std::move(e));
        }
    }

    uint32_t get_log_term(uint32_t index) {
        std::lock_guard<std::mutex> lock(mtx);
        if (index < block_offset_ || index - block_offset_ >= buffer_entry_.size()) {
            std::cerr << "index: " << index << " block_offset: " << block_offset_ << " buffer size: " << buffer_entry_.size() << std::endl;
            return 0;
        }
        return buffer_entry_[index - block_offset_].term;
    }


    void append_log(uint32_t term, std::vector<u8> &&command, uint32_t size) {
        std::lock_guard<std::mutex> lock(mtx);
        append_log_wo_aync(term, std::move(command), size);
        write_log_data();
    }


    bool contains(uint32_t index, uint32_t term) {
        std::lock_guard<std::mutex> lock(mtx);
        if (index < block_offset_ || index - block_offset_ >= buffer_entry_.size()) {
            return false;
        }
        return buffer_entry_[index - block_offset_].term == term;
    }

    void update_node_data(uint32_t term, uint32_t vote_for) {
        // because a big lock outside, we don't need to lock here
        //        std::lock_guard<std::mutex> lock(mtx);
        auto data_ptr = bm_->unsafe_get_block_ptr();
        auto ptr = reinterpret_cast<uint32_t *>(data_ptr);
        ptr += 10;
        *ptr = term;
        ptr++;
        *ptr = vote_for;
        bm_->sync_meat_data(meta_per_sz * 10, meta_per_sz * 2);
    }

    Command get_command(uint32_t index) {
        std::lock_guard<std::mutex> lock(mtx);
        CHFS_VERIFY(index < buffer_entry_.size(), "index out of range");
        auto offset = buffer_entry_[index].offset;
        auto data_ptr = data_ + offset;
        auto sz = *reinterpret_cast<uint32_t *>(data_ptr) - meta_per_sz * 2;
        std::vector<u8> command(sz);
        memcpy(command.data(), data_ptr + meta_per_sz * 2, sz);
        Command cmd;
        cmd.deserialize(std::move(command), sz);
        return cmd;
    }

    void reinit(int &current_term, int &vote_for) {
        std::lock_guard<std::mutex> lock(mtx);
        auto data_ptr = bm_->unsafe_get_block_ptr();
        auto ptr = reinterpret_cast<uint32_t *>(data_ptr);
        ptr += 10;
        current_term = *ptr;
        if (current_term == 0) {
            vote_for = -1;
        } else {
            ptr++;
            vote_for = *ptr;
        }
    }


    uint32_t last_log_term_{};
    uint32_t commit_index_{};
    uint32_t last_applied_{};
    uint32_t last_log_index_{};
    uint32_t block_offset_{};

private:
    void write_log_data() {
        auto ptr = reinterpret_cast<uint32_t *>(bm_->unsafe_get_block_ptr());
        *ptr = data_index_;
        ptr++;
        *ptr = last_log_index_;
        ptr++;
        *ptr = start_index_;
        bm_->sync_meat_data(0, meta_per_sz * 3);
    }

    void append_log_wo_aync(uint32_t term, std::vector<u8> &&command, uint32_t size) {
        buffer_entry_.emplace_back(term, data_index_);

        auto data_ptr = data_ + data_index_;
        uint32_t sz = size + 2 * meta_per_sz;
        memcpy(data_ptr, &sz, meta_per_sz);
        data_ptr += meta_per_sz;
        memcpy(data_ptr, &term, meta_per_sz);
        data_ptr += meta_per_sz;
        memcpy(data_ptr, command.data(), size);

        data_index_ += sz;
        last_log_index_++;
        last_log_term_ = term;
    }

    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    uint32_t data_index_{};
    uint32_t start_index_{};
    u8 *data_{};

    std::vector<Entry> buffer_entry_;

    constexpr static size_t meta_per_sz = sizeof(uint32_t);

    /* Lab3: Your code here */
};

template<typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm) {
    /* Lab3: Your code here */
    data_ = bm_->unsafe_get_block_ptr();
    auto ptr = reinterpret_cast<uint32_t *>(data_);
    data_index_ = *ptr;
    last_log_index_ = *(ptr + 1);
    data_ += bm_->block_size();
    buffer_entry_.resize(last_log_index_);
    auto offset = start_index_;
    for (auto i = start_index_; i < last_log_index_; i++) {
        auto size = *reinterpret_cast<uint32_t *>(data_ + offset);
        buffer_entry_[i].offset = offset;
        buffer_entry_[i].term = *(reinterpret_cast<uint32_t *>(data_ + offset + meta_per_sz));
        offset += size;
    }
    last_log_term_ = buffer_entry_.empty() ? 0 : buffer_entry_.back().term;
    CHFS_VERIFY(last_log_index_ >= buffer_entry_.size(), "log index not match");

    block_offset_ = last_log_index_ - buffer_entry_.size();

    data_ += start_index_;
}

template<typename Command>
RaftLog<Command>::~RaftLog() {
    /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */
