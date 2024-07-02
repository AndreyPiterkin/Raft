//
// Created by Andrey Piterkin on 12/5/22.
//

#include "../include/Replica.h"
#include <algorithm>
#include <string>
#include <iostream>
#include <utility>
#include <mutex>


#define BROADCAST "FFFF"
#define log_int_size ((int)this->m_log.size())
#define RUNTIME_COEFFICIENT 3600

Replica::Replica(int port, std::string ip, std::string id, std::vector<std::string> others, int others_length) {

    // config member fields
    this->m_id = id;
    this->m_others = std::move(others);
    this->m_others_length = others_length;
    this->m_leader = std::string("FFFF");
    this->m_sock_manager = new SocketManager(port, std::move(ip));
    this->m_sock_manager->addOnMessageListener(this);

    // raft state machine fields
    this->m_value_store = std::unordered_map<std::string, std::string>();

    this->m_next_index = std::vector<int>(this->m_others_length);
    this->m_match_index = std::vector<int>(this->m_others_length);

    for (int i = 0; i < this->m_others_length; ++i) {
        this->m_next_index.at(i) = 0;
        this->m_match_index.at(i) = -1;
    }

    this->m_voted_for = "";
    this->m_log = std::vector<std::pair<int, json>>();

    this->m_timeout = 0.15 + (0.3 - 0.15) * drand48();

    std::cout << "Replica " << id << " starting up with timeout " << this->m_timeout << std::endl;
    json hello = {
            {"src", this->m_id},
            {"dst", BROADCAST},
            {"leader", this->m_leader},
            {"type", "hello"}
    };
    this->m_sock_manager->send(hello);
    std::cout << "Sent hello message: " << hello.dump() << std::endl;
    this->m_sock_worker = std::thread([&] {
        this->m_sock_manager->listen();
    });
}

/**
 * Destructor.
 */
Replica::~Replica() {
    delete this->m_sock_manager;
}
/**
 * Run the replica.
 */
void Replica::run() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds((int)(1000 * this->m_timeout)));
        if (this->m_id == this->m_leader) {
            this->execute_leader_duties();
        } else {
            if (!this->received_message) {
                std::cout << "Running for election..." << std::endl;
                this->m_state = Candidate;
                this->m_current_term += 1;
                this->m_voted_for = this->m_id;
                this->votes += 1;
                this->m_sock_manager->send({
                       {"src", this->m_id},
                       {"dst", BROADCAST},
                       {"leader", this->m_leader},
                       {"type", "request_vote"},
                       {"term", this->m_current_term},
                       {"lastLogIndex", log_int_size - 1},
                       {"lastLogTerm", log_int_size > 0 ? this->m_log.at(log_int_size - 1).first : -1}
                });
                this->m_timeout = 0.15 + (0.3 - 0.15) * drand48();
            }
        }
        this->received_message = false;
    }
}

/**
 * Send heartbeats and AppendEntries RPCs.
 * @param dst the destination to send a message to
 * @param resend false if a message is original, true if it is being resent; defaulted to false
 */
void Replica::execute_leader_duties() {
    this->m_sock_manager->send({
           {"src",          this->m_id},
           {"dst",          BROADCAST},
           {"leader",       this->m_leader},
           {"type",         "append"},
           {"term",         this->m_current_term},
           {"leaderCommit", this->m_commit_index},
   });
}

/**
 * Issues an append to the follower replicas based on the given message.
 * @param message the data to issue an append for
 */
void Replica::issue_append(const json &message) {
    for (int replica_id = 0; replica_id < this->m_others.size(); ++replica_id) {
        this->m_sock_manager->send({
            {"id", message["MID"]},
            {"src", this->m_id},
            {"dst", this->m_others[replica_id]},
            {"leader", this->m_leader},
            {"type", "append"},
            {"term", this->m_current_term},
            {"prevLogIndex", this->m_next_index[replica_id] - 1},
            {"prevLogTerm", this->m_next_index[replica_id] - 1 >= 0 ? this->m_log.at(this->m_next_index[replica_id] - 1).first : -1},
            {"entries", std::vector<std::pair<int, json>>(this->m_log.begin() + this->m_next_index[replica_id], this->m_log.begin() + this->m_next_index[replica_id] + std::min((int)std::distance(this->m_log.begin() + this->m_next_index[replica_id], this->m_log.end()), 10))},
            {"leaderCommit", this->m_commit_index}
        });
    }
}

void Replica::handleLogOperation(const std::function<void(void)>& thunk) {
    const std::lock_guard<std::mutex> lock(this->m_mutex);
    thunk();
}

/**
 * Respond to an AppendEntries RPC.
 * @param message an AppendEntries RPC
 */
void Replica::handle_append(const json &message) {
    this->received_message = true;

    if (message["term"] >= this->m_current_term) {
        this->convert_to_follower(message);
    }

    if (!message.contains("entries")) return;

    if (message["term"] < this->m_current_term
        || ((int)message["prevLogIndex"] >= 0 && (int)message["prevLogIndex"] < log_int_size
        && this->m_log.at(message["prevLogIndex"]).first != message["prevLogTerm"])){

        this->m_sock_manager->send({
           {"src", this->m_id},
           {"dst", message["src"]},
           {"leader", this->m_leader},
           {"type", "replyAppend"},
           {"term", this->m_current_term},
           {"success", false}
        });

        return;
    }

    this->handleLogOperation(
            [&]() {
                if (message["prevLogIndex"] == -1) {
                    this->m_log.clear();
                } else if (message["prevLogIndex"] >= 0) {
                    this->m_log.erase(this->m_log.begin() + message["prevLogIndex"] + 1, this->m_log.end());
                }

                std::vector<std::pair<int, json>> entries = message["entries"].get<std::vector<std::pair<int, json>>>();
                for (auto entry : entries) {
                    this->m_log.push_back(entry);
                }
            });

    this->m_sock_manager->send({
       {"src", this->m_id},
       {"dst", message["src"]},
       {"leader", this->m_leader},
       {"type", "replyAppend"},
       {"term", this->m_current_term},
       {"success", true},
       {"lastLogIndex", log_int_size - 1},
    });

    this->m_commit_index = std::min((int) message["leaderCommit"], log_int_size - 1);
    while (this->m_last_applied < this->m_commit_index) {
        this->m_last_applied++;
        this->put(this->m_log.at(this->m_last_applied).second);
    }
}

/**
 * Handle the responses and consensus checking after sending an AppendEntries RPC.
 * @param message a response to an AppendEntries RPC
 */
void Replica::handle_append_reply(const json& message) {
    int replica_index;
    for(int i = 0; i < this->m_others_length; i++) {
        if(message["src"] == this->m_others[i]) {
            replica_index = i;
        }
    }

    if ((bool)message["success"]) {
        this->m_match_index[replica_index] = message["lastLogIndex"];
        this->m_next_index[replica_index] = (int)message["lastLogIndex"] + 1;
//        std::cout << "Replica " << m_others[replica_index] << " next: " << m_next_index[replica_index] << "\n";

        std::vector<int> sorted_progress = std::vector<int>(m_match_index);
        std::sort(sorted_progress.begin(), sorted_progress.end());

        if (sorted_progress.at(sorted_progress.size() / 2) > this->m_commit_index
            && this->m_log.at(sorted_progress.at(sorted_progress.size() / 2)).first == this->m_current_term) {

            this->m_commit_index = sorted_progress.at(sorted_progress.size() / 2);
        }

        while (this->m_last_applied < this->m_commit_index) {
            this->m_last_applied++;
            this->put(this->m_log.at(m_last_applied).second);
        }
    } else {
        this->m_next_index[replica_index]--;
        this->m_sock_manager->send({
           {"src", this->m_id},
           {"dst", this->m_others[replica_index]},
           {"leader", this->m_leader},
           {"type", "append"},
           {"term", this->m_current_term},
           {"prevLogIndex", this->m_next_index[replica_index] - 1},
           {"prevLogTerm", this->m_next_index[replica_index] - 1 >= 0 ? this->m_log.at(this->m_next_index[replica_index] - 1).first : -1},
           {"entries", std::vector<std::pair<int, json>>(this->m_log.begin() + this->m_next_index[replica_index], this->m_log.end())},
           {"leaderCommit", this->m_commit_index}
        });
    }
}

/**
 * Handle a RequestVote RPC.
 * @param message the RequestVote RPC received
 */
void Replica::handle_request_vote(const json &message) {
    this->received_message = true;
    if (this->m_voted_for == "FFFF"
        && message["term"] >= this->m_current_term
        && message["lastLogIndex"] >= (log_int_size - 1)) {
        this->m_current_term = message["term"];
        this->m_sock_manager->send({
           {"src", this->m_id},
           {"dst", message["src"]},
           {"type", "give_vote"},
           {"leader", this->m_leader},
           {"term", this->m_current_term},
           {"lastLogIndex", log_int_size - 1},
           {"lastLogTerm", log_int_size - 1 >= 0 ? this->m_log.at(log_int_size - 1).first : -1},
           {"voteGranted", true}
        });
        this->m_voted_for = message["src"];
    } else {
        this->m_sock_manager->send({
           {"src", this->m_id},
           {"dst", message["src"]},
           {"type", "give_vote"},
           {"leader", this->m_leader},
           {"term", this->m_current_term},
           {"voteGranted", false},
           {"lastLogIndex", log_int_size - 1},
           {"lastLogTerm", log_int_size - 1 >= 0 ? this->m_log.at(log_int_size - 1).first : -1},
        });
    }
}

/**
 * Handle vote counting after initiating an election.
 * @param message the message received from another replica
 */
void Replica::handle_receive_vote(const json &message) {
    this->received_message = true;
    if (message["voteGranted"] && this->m_state == Candidate) {
        this->votes += 1;
    }

    if (this->m_state == Candidate && this->votes > (this->m_others_length / 2)) {
        this->m_state = Leader;
        this->m_voted_for = "FFFF";
        this->m_leader = this->m_id;
        for (auto i = 0; i < this->m_others_length; ++i) {
            this->m_next_index[i] = log_int_size;
            this->m_match_index[i] = -1;
        }
        this->execute_leader_duties();
        this->votes = 0;
    }
}

/**
 * Log and complete a get request from the client.
 * @param message the get request from the client
 */
void Replica::handle_get(const json &message) {
    if (this->m_id != this->m_leader) this->redirect(message);
    else {
        this->m_sock_manager->send({
           {"value", this->get(message["key"])},
           {"src", this->m_id},
           {"dst", message["src"]},
           {"leader", this->m_leader},
           {"type", "ok"},
           {"MID", message["MID"]}
        });
    }
}

/**
 * Log a put request.
 * @param message the put request from the client
 */
void Replica::handle_put(const json &message) {
    if (this->m_id != this->m_leader) this->redirect(message);
    else {
        handleLogOperation([&]{
            this->m_log.push_back(std::pair<int, json>(this->m_current_term, message));
        });
        this->issue_append(message);
    };
}

/**
 * Handle a message from a replica.
 * @param message the message sent by the replica
 */
void Replica::receiveMessage(const json& message) {
    std::cout << message.dump() << std::endl;
    std::string msg_type = message["type"];

    if (message.contains("term") && message["term"] > this->m_current_term) {
        this->convert_to_follower(message);
    }

    if (msg_type == "put") this->handle_put(message);
    if (msg_type == "get") this->handle_get(message);
    if (msg_type == "append") this->handle_append(message);
    if (msg_type == "give_vote") this->handle_receive_vote(message);
    if (msg_type == "request_vote") this->handle_request_vote(message);
    if (msg_type == "replyAppend") this->handle_append_reply(message);
}

/**
 * Convert a replica to a follower.
 * @param message a message containing the current leader and its term
 */
void Replica::convert_to_follower(const json& message) {
    this->m_state = Follower;
    this->m_leader = message["leader"];
    this->m_current_term = message["term"];
    this->votes = 0;
    this->m_voted_for = "FFFF";
}

/**
 * Complete a get request, returning the value for a given key.
 * @param key the key provided by the client
 * @return the value associated with the given key
 */
std::string Replica::get(const std::string& key) {
    auto found = this->m_value_store.find(key);
    if (found == this->m_value_store.end()) {
        return "";
    } else {
        return found->second;
    }
}

/**
 * Complete a put request, applying a key-value pair to the state machine.
 * @param message the write request from the client
 */
void Replica::put(const json& message) {
    this->m_value_store[message["key"]] = message["value"];
    if (this->m_state == Leader) {
        this->m_sock_manager->send({
               {"src", this->m_id},
               {"dst", message["src"]},
               {"leader", this->m_leader},
               {"MID", message["MID"]},
               {"type", "ok"}
       });
    }

//    this->append_votes = 0;
}

/**
 * Redirect a message to the leader when the client sends any
 * message (get() or put()) to a replica that is not the leader.
 * @param original_message the message being redirected
 */
void Replica::redirect(const json &original_message) {
    this->m_sock_manager->send({
       {"src", this->m_id},
       {"dst", original_message["src"]},
       {"leader", this->m_leader},
       {"type", "redirect"},
       {"MID", original_message["MID"]}
   });
}