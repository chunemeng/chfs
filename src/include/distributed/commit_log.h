//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <bitset>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */


// I know is not part of the C++ standard.
// However, it is supported by GCC and Clang,
// and it is very easy to use.
struct LogEntry {
    txn_id_t txn_id;
    block_id_t block_id;
    u8 new_block_data[0];
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
    explicit CommitLog(std::shared_ptr<BlockManager> bm,
                       bool is_checkpoint_enabled);
    ~CommitLog();
    auto append_log(txn_id_t txn_id,
                    std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
    auto commit_log(txn_id_t txn_id) -> void;
    auto checkpoint() -> void;
    auto recover() -> void;
    auto get_log_entry_num() -> usize;

    auto get_last_checkpoint_txn_id() -> txn_id_t {
        return last_checkpoint_txn_id_.fetch_add(1);
    }

    bool is_checkpoint_enabled_;
    std::shared_ptr<BlockManager> bm_;

    int current_write_offset_;
    std::atomic<int> current_txn_id_in_disk_;
    std::atomic<txn_id_t> last_checkpoint_txn_id_;
    std::shared_mutex log_mutex_;
};

}// namespace chfs