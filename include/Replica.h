//
// Created by Andrey Piterkin on 12/2/22.
//

#ifndef RAFT_STARTER_CODE_REPLICA_H
#define RAFT_STARTER_CODE_REPLICA_H

#include <unordered_map>
#include <string>
#include "json.hpp"
#include "SocketManager.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <utility>

class SocketManager;


enum ReplicaState {
    Follower,
    Candidate,
    Leader,
};

using json = nlohmann::json;
class Replica {
public:
    Replica(int port, std::string ip, std::string id, std::vector<std::string> others, int others_length);
    ~Replica();
    void receiveMessage(const json& message);
    // main routine method
    void run();
private:
    // message type handlers
    void handle_put(const json& message);
    void handle_get(const json& message);
    void handle_append(const json& message);
    void handle_append_reply(const json& message);
    void handle_request_vote(const json& message);
    void handle_receive_vote(const json& message);
    void execute_leader_duties();

    // helper methods
    std::string get(const std::string& key);
    void put(const json& message);
    void redirect(const json& original_message);
    void convert_to_follower(const json& message);
    void issue_append(const json& message);
    void handleLogOperation(const std::function<void(void)>& thunk);

    // Config member fields
    std::string m_id;
    std::unordered_map<std::string, std::string> m_value_store;
    std::string m_leader;
    std::vector<std::string> m_others{};
    int m_others_length{};
    SocketManager* m_sock_manager{};
    std::thread m_sock_worker;

    // Raft state machine fields
    int m_current_term = 0;
    int m_commit_index = 0;
    int m_last_applied = -1;
    std::string m_voted_for;
    std::vector<std::pair<int, json>> m_log;
    std::vector<int> m_next_index;
    std::vector<int> m_match_index;

    // Other fields
    double m_timeout;
    ReplicaState m_state = Follower;
    // votes for if we are asking for votes in an election
    // INVARIANT: if this replica is not a candidate, votes = 0
    int votes = 0;
    bool received_message = false;
    bool append_confirmed = true;
    std::mutex m_mutex;

};

#endif //RAFT_STARTER_CODE_REPLICA_H
