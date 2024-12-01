#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
    last_checkpoint_txn_id_ = 1;
    current_write_offset_ = 0;
    current_txn_id_in_disk_ = 0;
}

CommitLog::~CommitLog() = default;

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
//    std::shared_lock lock(log_mutex_);
    return current_txn_id_in_disk_;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
        -> void {
    usize log_size = current_write_offset_;
    auto ptr = bm_->log_data_ptr(current_write_offset_);

//    std::unique_lock lock(log_mutex_);
    for (const auto &op: ops) {
        auto *entry = reinterpret_cast<LogEntry *>(ptr);
        entry->txn_id = txn_id;
        entry->block_id = op->block_id_;
        memcpy(entry->new_block_data, op->new_block_state_.data(), DiskBlockSize);
        current_write_offset_ += sizeof(LogEntry) + DiskBlockSize;
        ptr += sizeof(LogEntry) + DiskBlockSize;
    }
    auto start_block = log_size / DiskBlockSize;
    auto end_block = (current_write_offset_ + DiskBlockSize - 1) / DiskBlockSize;
    for (auto i = start_block; i < end_block; i++) {
        bm_->sync_log(i);
    }
    current_txn_id_in_disk_++;

    if (is_checkpoint_enabled_ && (last_checkpoint_txn_id_ > 180 || current_write_offset_ > DiskBlockSize * 1020)) {
        checkpoint();
    }
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
    current_txn_id_in_disk_--;
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
    std::cerr << "checkpoint\n";
    last_checkpoint_txn_id_ = 1;
    current_write_offset_ = 0;
    auto ptr = bm_->log_data_ptr(0);
    memset(ptr, 0, DiskBlockSize * 1024);

    bm_->flush();
}

// {Your code here}
auto CommitLog::recover() -> void {
//    std::unique_lock lock(log_mutex_);
    auto ptr = bm_->log_data_ptr(0);
    auto end = bm_->log_data_ptr(1024 * DiskBlockSize);

    while (ptr < end) {
        auto entry = reinterpret_cast<LogEntry *>(ptr);
        if (entry->txn_id == 0) {
            break;
        }
        auto block_id = entry->block_id;
        bm_->write_block(block_id, entry->new_block_data);
        ptr += sizeof(LogEntry) + DiskBlockSize;
    }
    checkpoint();
}
};// namespace chfs