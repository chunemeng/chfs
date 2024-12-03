#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
    MSGPACK_DEFINE(
            term,
            candidate_id,
            last_log_index,
            last_log_term)
};

struct RequestVoteReply {
    int term;
    bool vote_granted;

    MSGPACK_DEFINE(
            term,
            vote_granted)
};

struct entry {
    int term;
    int size;
    std::vector<u8> command;
    MSGPACK_DEFINE(
            term,
            size,
            command)
};

template<typename Command>
struct AppendEntriesArgs {
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    int leader_commit;
    std::vector<entry> entries;
};

struct RpcAppendEntriesArgs {
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    uint32_t leader_commit;
    std::vector<entry> entries;

    MSGPACK_DEFINE(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries)
};

template<typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {

    /* Lab3: Your code here */
    return RpcAppendEntriesArgs{arg.term, arg.leader_id, arg.prev_log_index, arg.prev_log_term, arg.leader_commit, std::move(arg.entries)};
}

template<typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
    /* Lab3: Your code here */
    return AppendEntriesArgs<Command>();
}

struct AppendEntriesReply {
    int current_term;
    bool success;
    MSGPACK_DEFINE(
            current_term,
            success)
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int last_included_index;
    int last_included_term;
    int offset;
    std::vector<u8> data;
    bool done;
    MSGPACK_DEFINE(
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset,
            data,
            done)
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    int term;
    MSGPACK_DEFINE(
            term)
};

} /* namespace chfs */