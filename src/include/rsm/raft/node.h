#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdarg.h>
#include <thread>
#include <unistd.h>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

namespace chfs {

  enum class RaftRole {
      Follower,
      Candidate,
      Leader
  };

  struct RaftNodeConfig {
      int node_id;
      uint16_t port;
      std::string ip_address;
  };

  template<typename StateMachine, typename Command>
  class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                                       \
    do {                                                                                                                             \
        auto now =                                                                                                                   \
                std::chrono::duration_cast<std::chrono::milliseconds>(                                                               \
                        std::chrono::system_clock::now().time_since_epoch())                                                         \
                        .count();                                                                                                    \
        char buf[512];                                                                                                               \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                                           \
    } while (0);

//#define LOG_DEBUG_RAFT

#ifdef LOG_DEBUG_RAFT
#define LOG(fmt, args...) RAFT_LOG(fmt, ##args)
#else
#define LOG(fmt, args...) \
    do {                  \
    } while (0);
#endif

      //#define CONVERT_DEBUG_RAFT

#ifdef CONVERT_DEBUG_RAFT
#define CON_DEBUG(fmt, args...) RAFT_LOG(fmt, ##args)
#else
#define CON_DEBUG(fmt, args...) \
    do {                        \
    } while (0);
#endif

      //#define NETWORK_DEBUG_RAFT

#ifdef NETWORK_DEBUG_RAFT
#define NET_DEBUG(fmt, args...) RAFT_LOG(fmt, ##args)
#else
#define NET_DEBUG(fmt, args...) \
    do {                        \
    } while (0);
#endif

  public:
      RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);

      ~RaftNode();

      /* interfaces for test */
      void set_network(std::map<int, bool> &network_availablility);

      void set_reliable(bool flag);

      int get_list_state_log_num();

      int rpc_count();

      std::vector<u8> get_snapshot_direct();

  private:
      constexpr static int HEARTBEAT_INTERVAL = 200;

      bool is_election_time_out() {
          return std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - last_heartbeat).count() > election_timeout;
      }

      /*
       * Start the raft node.
       * Please make sure all of the rpc request handlers have been registered before this method.
       */
      auto start() -> int;

      /*
       * Stop the raft node.
       */
      auto stop() -> int;

      /* Returns whether this node is the leader, you should also return the current term. */
      auto is_leader() -> std::tuple<bool, int>;

      /* Checks whether the node is stopped */
      auto is_stopped() -> bool;

      /*
       * Send a new command to the raft nodes.
       * The returned tuple of the method contains three values:
       * 1. bool:  True if this raft node is the leader that successfully appends the log,
       *      false If this node is not the leader.
       * 2. int: Current term.
       * 3. int: Log index.
       */
      auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

      /* Save a snapshot of the state machine and compact the log. */
      auto save_snapshot() -> bool;

      /* Get a snapshot of the state machine */
      auto get_snapshot() -> std::vector<u8>;

      void convert_follower() {
          role = RaftRole::Follower;

          log_for_commit_.clear();

          leader_id = -1;
      }


      /* Internal RPC handlers */
      auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;

      auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;

      auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

      /* RPC helpers */
      void send_request_vote(int target, RequestVoteArgs arg);

      void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

      void send_append_entries(int target, AppendEntriesArgs<Command> arg);

      void
      handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

      void send_install_snapshot(int target, InstallSnapshotArgs arg);

      void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

      /* background workers */
      void run_background_ping();

      void run_background_election();

      void run_background_commit();

      void run_background_apply();


      /* Data structures */
      bool network_stat; /* for test */

      std::mutex mtx;         /* A big lock to protect the whole data structure. */
      std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
      std::unique_ptr<ThreadPool> thread_pool;
      std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
      std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

      std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
      std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
      std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
      int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

      std::atomic_bool stopped;

      RaftRole role;
      int election_timeout;
      int current_term;
      int leader_id;
      int voted_for;
      int votes_;

      // store the index of log
      std::list<uint32_t> log_for_commit_;
      std::chrono::time_point<std::chrono::steady_clock> last_heartbeat;

      struct index_entry {
          uint32_t next_index_{};
          uint32_t match_index_{};

          index_entry() = default;

          index_entry(uint32_t next_index, uint32_t match_index) : next_index_(next_index), match_index_(match_index) {}
      };

      std::map<int, index_entry> next_index_map_;

      std::vector<u8> snapshot_data_;

      std::unique_ptr<std::thread> background_election;
      std::unique_ptr<std::thread> background_ping;
      std::unique_ptr<std::thread> background_commit;
      std::unique_ptr<std::thread> background_apply;
  };

  template<typename StateMachine, typename Command>
  RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs) : network_stat(true),
                                                                                                node_configs(std::move(
                                                                                                        configs)),
                                                                                                my_id(node_id),
                                                                                                stopped(true),
                                                                                                role(RaftRole::Follower),
                                                                                                current_term(0),
                                                                                                leader_id(-1) {
      auto my_config = node_configs[my_id];
      /* launch RPC server */
      rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

      /* Register the RPCs. */
      rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
      rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
      rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
      rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
      rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                       [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
      rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
      rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

      rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
      rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
      rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT,
                       [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });


      const std::string log_path = "/tmp/raft_log/node" + std::to_string(my_id);
      log_storage = std::make_unique<RaftLog<Command>>(std::make_shared<BlockManager>(log_path));
      log_storage->reinit(current_term, voted_for, snapshot_data_);
      thread_pool = std::make_unique<ThreadPool>(2);
      state = std::make_unique<StateMachine>();

      for (auto config: node_configs) {
          if (config.node_id == my_id) {
              continue;
          }
          next_index_map_[config.node_id] = {};
      }

      rpc_server->run(true, node_configs.size());
  }

  template<typename StateMachine, typename Command>
  RaftNode<StateMachine, Command>::~RaftNode() {
      stop();

      thread_pool.reset();
      rpc_server.reset();
      state.reset();
      log_storage.reset();
  }

/******************************************************************

                        RPC Interfaces

*******************************************************************/


  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::start() -> int {
      stopped.store(false);

      for (auto config: node_configs) {
          if (config.node_id == my_id) {
              continue;
          }
          auto client = std::make_unique<RpcClient>(config.ip_address, config.port, true);

          rpc_clients_map[config.node_id] = std::move(client);
      }

      background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
      background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
      background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
      background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

      return 0;
  }

  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::stop() -> int {
      stopped.store(true);
      background_commit->join();
      background_election->join();
      background_ping->join();
      background_apply->join();

      return 0;
  }

  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      return std::make_tuple(role == RaftRole::Leader, current_term);
  }

  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
      return stopped.load();
  }

  template<typename StateMachine, typename Command>
  auto
  RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int> {
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      if (role == RaftRole::Leader) {
          // append log
          log_for_commit_.push_front(log_storage->last_log_index_);

          log_storage->append_log(current_term, std::move(cmd_data), cmd_size);

          return std::make_tuple(true, current_term, log_storage->last_log_index_);
      }

      return std::make_tuple(false, -1, -1);
  }

  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
      std::unique_lock<std::mutex> lock(mtx);

      if (log_storage->last_applied_ == 0) {
          return false;
      }

      snapshot_data_ = std::move(state->snapshot());
      log_storage->save_snapshot(snapshot_data_);
      log_storage->truncate_log(log_storage->last_applied_ - 1);
      log_storage->commit_index_ = log_storage->last_log_index_;
      log_storage->last_applied_ = log_storage->commit_index_;

      return true;
  }

  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      return state->snapshot();
  }

/******************************************************************

                         Internal RPC Related

*******************************************************************/


  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply {
      std::pair<uint32_t, uint32_t> vote_log_states = std::make_pair(args.last_log_index, args.last_log_term);
      std::unique_lock<std::mutex> lock(mtx);
      LOG("Node %d receive vote request from %d %d", my_id, args.candidate_id, election_timeout);

      if (args.term < current_term) {
          return RequestVoteReply{current_term, false};
      }

      if (args.term > current_term) {
          current_term = args.term;
          CON_DEBUG("Node %d convert to follower cause other vote", my_id);
          convert_follower();

          bool is_log_up_to_date = log_storage->get_least_log_state() <= vote_log_states;

          //        LOG("Node %d vote for %d vote %d %d local %d %d", my_id, is_log_up_to_date, vote_log_states.first, vote_log_states.second, log_storage->last_log_index_, log_storage->last_log_term_);

          voted_for = is_log_up_to_date ? args.candidate_id : -1;


          log_storage->update_node_data(current_term, voted_for);

          // NOTE: only you vote it should reset the election timeout
          if (is_log_up_to_date) {
              last_heartbeat = std::chrono::steady_clock::now();
          }

          return RequestVoteReply{current_term, is_log_up_to_date};
      }

      if (voted_for == -1) {
          voted_for = args.candidate_id;
          bool is_log_up_to_date = log_storage->get_least_log_state() <= vote_log_states;

          voted_for = is_log_up_to_date ? args.candidate_id : -1;

          log_storage->update_node_data(current_term, voted_for);
          if (is_log_up_to_date) {
              last_heartbeat = std::chrono::steady_clock::now();
          }
          return RequestVoteReply{current_term, is_log_up_to_date};
      }

      return RequestVoteReply{current_term, false};
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                                                  const RequestVoteReply reply) {
      std::unique_lock<std::mutex> lock(mtx);
      if (current_term < reply.term) {
          current_term = reply.term;
          CON_DEBUG("Node %d convert to follower cause other vote reply", my_id);
          convert_follower();
          voted_for = -1;
          log_storage->update_node_data(current_term, voted_for);
      } else {
          if (role != RaftRole::Candidate || current_term != arg.term) {
              LOG("Node %d is not candidate or term not match", my_id);
              return;
          }

          if (reply.vote_granted) {
              LOG("Node %d get vote from %d", my_id, target);
              votes_ += reply.vote_granted;

              auto majority = (node_configs.size()) / 2 + 1;
              if (votes_ >= majority) {
                  role = RaftRole::Leader;
                  LOG("Node %d become leader", my_id);

                  for (auto &client: rpc_clients_map) {
                      if (client.first == my_id) {
                          continue;
                      }

                      next_index_map_[client.first].next_index_ = log_storage->last_log_index_ + 1;
                      next_index_map_[client.first].match_index_ = 0;
                  }
                  log_for_commit_.clear();
                  leader_id = my_id;
              }
          }
      }
  }

  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      if (rpc_arg.term < current_term) {
          return {current_term, false};
      }

      CON_DEBUG("Node %d convert to follower cause other append %d term %d", my_id, rpc_arg.leader_id, rpc_arg.term);
      if (current_term < rpc_arg.term) {
          current_term = rpc_arg.term;
          convert_follower();
          leader_id = rpc_arg.leader_id;
          voted_for = -1;
          log_storage->update_node_data(current_term, voted_for);
      } else {
      }

      last_heartbeat = std::chrono::steady_clock::now();


      // index 0 is empty should always return true for logics
      // and entries index is also 0
      if (rpc_arg.prev_log_index == 0) {
          if (!rpc_arg.entries.empty()) {
              log_storage->append_entries(rpc_arg.prev_log_index, std::move(rpc_arg.entries));
          }

          log_storage->commit_index_ = std::min(rpc_arg.leader_commit, log_storage->last_log_index_);


          return {current_term, true};
      }


      if (!log_storage->contains(rpc_arg.prev_log_index - 1, rpc_arg.prev_log_term)) {
          return {current_term, false};
      }

      log_storage->append_entries(rpc_arg.prev_log_index, std::move(rpc_arg.entries));

      log_storage->commit_index_ = std::min(rpc_arg.leader_commit, log_storage->last_log_index_);

      return {current_term, true};
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg,
                                                                    const AppendEntriesReply reply) {
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      if (reply.current_term > current_term) {
          current_term = reply.current_term;
          CON_DEBUG("Node %d convert to follower cause other append reply", my_id);
          convert_follower();
          voted_for = -1;
          log_storage->update_node_data(current_term, voted_for);
          return;
      }

      if (role != RaftRole::Leader || reply.current_term != arg.term) {
          return;
      }

      auto &pair = next_index_map_[node_id];
      if (reply.success) {
          if (arg.entries.empty()) {
              return;
          }
          pair.next_index_ = arg.prev_log_index + arg.entries.size() + 1;
          pair.match_index_ = pair.next_index_ - 1;
          LOG("Node %d append log to %d success", my_id, pair.next_index_);
      } else {
          pair.next_index_ = std::max(1u, pair.next_index_ - 1);
      }

      if (log_for_commit_.empty()) {
          return;
      }

      auto majority = (node_configs.size()) / 2 + 1;
      auto size = 1;
      for (auto iter = log_for_commit_.begin(); iter != log_for_commit_.end(); iter++) {
          size = 1;
          for (auto &client: rpc_clients_map) {
              if (client.first == my_id) {
                  continue;
              }
              if (next_index_map_[client.first].match_index_ >= (*iter + 1)) {
                  size++;
              }
          }
          if (size >= majority) {
              log_storage->commit_index_ = *iter + 1;
              LOG("Node %d commit log %d , votes %d", my_id, *iter, size);
              //        RAFT_LOG("commit log %d , votes %d", *iter, size);
              log_for_commit_.erase(iter, log_for_commit_.end());
              break;
          }
      }
  }


  template<typename StateMachine, typename Command>
  auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply {
      std::unique_lock<std::mutex> lock(mtx);
      if (args.term < current_term) {
          return {current_term};
      }

      if (args.term > current_term) {
          convert_follower();
          voted_for = -1;
          current_term = args.term;
          leader_id = args.leader_id;
          log_storage->update_node_data(current_term, voted_for);
      }


      last_heartbeat = std::chrono::steady_clock::now();

      if (args.last_included_index + 1 <= log_storage->commit_index_) {
          return {current_term};
      } else {

          log_storage->commit_index_ = std::max(log_storage->commit_index_,
                                                static_cast<uint32_t>(args.last_included_index) + 1);
          log_storage->last_applied_ = log_storage->commit_index_;


          state->apply_snapshot(args.data);

          this->snapshot_data_ = args.data;

          log_storage->update_for_snapshot(args.last_included_index, args.last_included_term);
          return {current_term};
      }
  }


  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg,
                                                                      const InstallSnapshotReply reply) {
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(mtx);
      if (reply.term > current_term) {
          current_term = reply.term;
          CON_DEBUG("Node %d convert to follower cause other install snapshot reply", my_id);
          convert_follower();
          voted_for = -1;
          log_storage->update_node_data(current_term, voted_for);
      }

      if (role != RaftRole::Leader || reply.term != arg.term) {
          return;
      }

      auto &pair = next_index_map_[node_id];

      pair.next_index_ = arg.last_included_index + 2;
      pair.match_index_ = pair.next_index_ - 1;
      LOG("Node %d install snapshot to %d success", node_id, pair.match_index_);

      auto majority = (node_configs.size()) / 2 + 1;
      auto size = 1;
      for (auto iter = log_for_commit_.begin(); iter != log_for_commit_.end(); iter++) {
          size = 1;
          for (auto &client: rpc_clients_map) {
              if (client.first == my_id) {
                  continue;
              }
              if (next_index_map_[client.first].match_index_ >= (*iter + 1)) {
                  size++;
              }
          }
          if (size >= majority) {
              log_storage->commit_index_ = *iter + 1;
              LOG("Node %d commit log %d , votes %d", my_id, *iter, size);
              //        RAFT_LOG("commit log %d , votes %d", *iter, size);
              log_for_commit_.erase(iter, log_for_commit_.end());
              break;
          }
      }


      return;
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg) {
      std::unique_lock<std::mutex> clients_lock(clients_mtx);
      if (rpc_clients_map[target_id] == nullptr ||
          rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
          NET_DEBUG("Node %d rpc client %d not connected", my_id, target_id);
          return;
      }

      auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
      clients_lock.unlock();
      if (res.is_ok()) {
          handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
      } else {
      }
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg) {
      std::unique_lock<std::mutex> clients_lock(clients_mtx);
      if (rpc_clients_map[target_id] == nullptr ||
          rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
          return;
      }

      RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
      auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
      clients_lock.unlock();
      if (res.is_ok()) {
          handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
      } else {
          // RPC fails
      }
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg) {
      std::unique_lock<std::mutex> clients_lock(clients_mtx);
      if (rpc_clients_map[target_id] == nullptr ||
          rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
          return;
      }

      auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
      clients_lock.unlock();
      if (res.is_ok()) {
          handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
      } else {
          // RPC fails
      }
  }


/******************************************************************

                        Background Workers

*******************************************************************/

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::run_background_election() {
      // Periodly check the liveness of the leader.

      // Work for followers and candidates.

      // include the leader itself
      /* Uncomment following code when you finish */
      last_heartbeat = std::chrono::steady_clock::now();
      std::mt19937 generator(std::random_device{}());

      election_timeout = HEARTBEAT_INTERVAL * 2 + (generator() % HEARTBEAT_INTERVAL) * 1.5;

      while (true) {
          if (is_stopped()) {
              return;
          }
          {
              std::unique_lock<std::mutex> lock(mtx);

              if (role == RaftRole::Follower) {

                  // NOTE: There is no need to check the liveness of the leader
                  // if leader is not sending heartbeats,
                  // the follower will become candidate and start a new election.

                  // Check the election timeout.
                  if (is_election_time_out()) {
                      auto gen = generator();
                      election_timeout = HEARTBEAT_INTERVAL * 2 + (gen % HEARTBEAT_INTERVAL) * 1.5;
                      LOG("Node %d election time out %d", my_id, election_timeout);
                      role = RaftRole::Candidate;
                  }
              }
              if (role == RaftRole::Candidate && is_election_time_out()) {
                  LOG("Node %d start a new election", my_id);
                  // Start a new election.
                  current_term++;
                  voted_for = my_id;

                  //                auto gen = generator();
                  //                election_timeout = HEARTBEAT_INTERVAL + (gen % (HEARTBEAT_INTERVAL)) * 3;
                  //                LOG("Node %d election time out %d", my_id, election_timeout);

                  votes_ = 1;
                  log_storage->update_node_data(current_term, voted_for);
                  last_heartbeat = std::chrono::steady_clock::now();


                  for (const auto &client: rpc_clients_map) {
                      if (client.first == my_id) {
                          continue;
                      }
                      thread_pool->enqueue([this, target_id = client.first]() {
                          send_request_vote(target_id,
                                            RequestVoteArgs{current_term, my_id, log_storage->last_log_index_,
                                                            log_storage->last_log_term_});
                      });
                  }
              }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL / 8));
      }
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::run_background_commit() {
      // Periodly send logs to the follower.

      // Only work for the leader.

      /* Uncomment following code when you finish */
      while (true) {
          if (is_stopped()) {
              return;
          }
          {
              std::unique_lock<std::mutex> lock(mtx);
              if (role == RaftRole::Leader) {
                  // send logs to the followers
                  for (auto &client: rpc_clients_map) {
                      if (client.first == my_id) {
                          continue;
                      }

                      auto pair = next_index_map_[client.first];

                      if (pair.next_index_ > log_storage->last_log_index_ || log_storage->last_log_index_ == 0) {
                          continue;
                      }

                      auto prev_log_index = pair.next_index_ - 1;

                      if (pair.next_index_ == 0 || prev_log_index >= log_storage->block_offset_) {
                          std::vector<entry> entries;
                          entries.reserve(log_storage->last_log_index_ - pair.next_index_);


                          auto prev_log_term = prev_log_index == 0 ? 0 : log_storage->get_log_term(prev_log_index);

                          log_storage->read_log(prev_log_index, log_storage->last_log_index_, entries);


                          // FIXME: is this necessary?
                          // should use term to store the current term
                          // otherwise, the term will be changed by the background_election
                          thread_pool->enqueue(
                                  [this, target_id = client.first, term = current_term, commit_index = log_storage->commit_index_,
                                          prev_log_index, prev_log_term, entries_m = std::move(entries)] {
                                      send_append_entries(target_id, AppendEntriesArgs<Command>{
                                              term,
                                              my_id,
                                              prev_log_index,
                                              prev_log_term,
                                              commit_index,
                                              std::move(entries_m)});
                                  });


                      } else {
                          thread_pool->enqueue([this, target_id = client.first, term = current_term,
                                                       commit_index = log_storage->commit_index_, block_offset =
                                  log_storage->block_offset_ - 1,
                                                       last_include_term = log_storage->last_include_term_, snapshot = snapshot_data_] {
                              send_install_snapshot(target_id,
                                                    InstallSnapshotArgs{term, my_id, block_offset, last_include_term, 0,
                                                                        snapshot, true});
                          });
                      }
                  }
              }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL / 20));
      }
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::run_background_apply() {
      // Periodly apply committed logs the state machine

      // Work for all the nodes.

      /* Uncomment following code when you finish */

      constexpr int APPLY_INTERVAL = HEARTBEAT_INTERVAL / 4;
      while (true) {
          if (is_stopped()) {
              return;
          }
          {
              std::unique_lock<std::mutex> lock(mtx);
              while (log_storage->commit_index_ > log_storage->last_applied_) {
                  Command cmd = log_storage->get_command(log_storage->last_applied_);
                  log_storage->last_applied_++;
                  LOG("Node %d apply log %d", my_id, cmd.value)
                  //                RAFT_LOG("apply %d log %d", log_storage->last_applied_ - 1, cmd.value);
                  state->apply_log(cmd);
              }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::run_background_ping() {
      // Periodly send empty append_entries RPC to the followers.

      // Only work for the leader.

      /* Uncomment following code when you finish */
      while (true) {
          if (is_stopped()) {
              return;
          }
          {
              std::unique_lock<std::mutex> lock(mtx);

              if (role == RaftRole::Leader) {
                  for (auto &client: rpc_clients_map) {
                      if (client.first == my_id) {
                          continue;
                      }

                      auto pair = next_index_map_[client.first];
                      auto prev_log_index = pair.next_index_ - 1;
                      if (prev_log_index == 0 || log_storage->block_offset_ <= prev_log_index) {
                          auto prev_log_term = prev_log_index == 0 ? 0 : log_storage->get_log_term(prev_log_index);
                          thread_pool->enqueue([this, term = current_term, commit_index = log_storage->commit_index_,
                                                       prev_log_index, prev_log_term, target_id = client.first]() {
                              send_append_entries(target_id,
                                                  AppendEntriesArgs<Command>{term, my_id, prev_log_index, prev_log_term,
                                                                             commit_index, {}});
                          });
                      } else {
                          CHFS_ASSERT(log_storage->block_offset_ > 0, "block offset should not be 0");
                          thread_pool->enqueue([this, target_id = client.first, term = current_term,
                                                       commit_index = log_storage->commit_index_, block_off =
                                  log_storage->block_offset_ - 1,
                                                       last_include_term = log_storage->last_include_term_, snapshot = snapshot_data_] {
                              send_install_snapshot(target_id,
                                                    InstallSnapshotArgs{term, my_id, block_off, last_include_term, 0,
                                                                        snapshot, true});
                          });
                      }
                  }
              }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL));
      }
  }

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability) {
      std::unique_lock<std::mutex> clients_lock(clients_mtx);


      /* turn off network */
      if (!network_availability[my_id]) {
          for (auto &&client: rpc_clients_map) {
              if (client.second != nullptr)
                  client.second.reset();
          }

          return;
      }

      for (auto node_network: network_availability) {
          int node_id = node_network.first;
          bool node_status = node_network.second;

          if (node_status && rpc_clients_map[node_id] == nullptr) {
              RaftNodeConfig target_config;
              for (auto config: node_configs) {
                  if (config.node_id == node_id)
                      target_config = config;
              }

              rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port,
                                                                     true);
          }

          if (!node_status && rpc_clients_map[node_id] != nullptr) {
              rpc_clients_map[node_id].reset();
          }
      }
  }

  template<typename StateMachine, typename Command>
  void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
      std::unique_lock<std::mutex> clients_lock(clients_mtx);
      for (auto &&client: rpc_clients_map) {
          if (client.second) {
              client.second->set_reliable(flag);
          }
      }
  }

  template<typename StateMachine, typename Command>
  int RaftNode<StateMachine, Command>::get_list_state_log_num() {
      /* only applied to ListStateMachine*/
      std::unique_lock<std::mutex> lock(mtx);

      return state->num_append_logs;
  }

  template<typename StateMachine, typename Command>
  int RaftNode<StateMachine, Command>::rpc_count() {
      int sum = 0;
      std::unique_lock<std::mutex> clients_lock(clients_mtx);

      for (auto &&client: rpc_clients_map) {
          if (client.second) {
              sum += client.second->count();
          }
      }

      return sum;
  }

  template<typename StateMachine, typename Command>
  std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
      if (is_stopped()) {
          return std::vector<u8>();
      }

      std::unique_lock<std::mutex> lock(mtx);

      return state->snapshot();
  }

}// namespace chfs