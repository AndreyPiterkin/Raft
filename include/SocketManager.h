//
// Created by Andrey Piterkin on 12/3/22.
//

#ifndef RAFT_STARTER_CODE_SOCKETMANAGER_H
#define RAFT_STARTER_CODE_SOCKETMANAGER_H

#include <string>
#include "json.hpp"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <unistd.h>
#include <netdb.h>
#include <iostream>
#include "../include/Replica.h"

class Replica;
using json = nlohmann::json;

class SocketManager {
public:
    SocketManager(int port, std::string ip);
    ~SocketManager();
    void send(const json& message);
    void addOnMessageListener(Replica* r);
    void listen();
private:
    int m_port;
    std::string m_ip;
    int m_socket;
    char* in_buffer;
    struct sockaddr_in addr{};
    socklen_t len;
    fd_set master{};
    fd_set temp{};
    int fdmax;
    struct timeval tv{};
    std::vector<Replica*> listeners;
};


#endif //RAFT_STARTER_CODE_SOCKETMANAGER_H
