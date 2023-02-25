//
// Created by Andrey Piterkin on 12/3/22.
//

#include "../include/SocketManager.h"

/**
 * A manager for the replicas' sockets, used to send and receive data
 * @param port the port number
 * @param ip the ip address
 */
SocketManager::SocketManager(int port, std::string ip) {
    this->m_ip = ip;
    this->m_port = port;
    this->m_socket = socket(AF_INET, SOCK_DGRAM, 0);
    this->in_buffer = new char[65536];

    int status;

    struct sockaddr_in local_sim_addr{};
    local_sim_addr.sin_family = AF_INET;
    local_sim_addr.sin_port = htons(0);
    local_sim_addr.sin_addr.s_addr = INADDR_ANY;
    memset(local_sim_addr.sin_zero, '\0', sizeof local_sim_addr.sin_zero);

    if ((status = bind(this->m_socket, (const struct sockaddr*)&local_sim_addr, sizeof local_sim_addr)) != 0) {
        perror("Error in binding to localhost:0");
        exit(1);
    }

    this->addr.sin_family = AF_INET;
    this->addr.sin_port = htons(this->m_port);
    this->addr.sin_addr.s_addr = inet_addr(this->m_ip.c_str());
    memset(this->addr.sin_zero, '\0', sizeof this->addr.sin_zero);
    this->len = sizeof this->addr;

    FD_ZERO(&this->master);
    FD_ZERO(&this->temp);
    FD_SET(this->m_socket, &this->master);
    this->fdmax = this->m_socket;

    this->tv.tv_sec = 0;
    this->tv.tv_usec = 50;
}

/**
 * Destructor.
 */
SocketManager::~SocketManager() {
    delete[] this->in_buffer;
    shutdown(this->m_socket, SHUT_RDWR);
}

/**
 * Send a message.
 * @param message the message to be sent
 */
void SocketManager::send(const json& message) {
    std::string msg = message.dump();

    int sent = sendto(this->m_socket, (void*)msg.c_str(), msg.size(), 0, (struct sockaddr*)&this->addr, this->len);
    if (sent == -1) {
        perror("sendto");
        std::cout << msg.size() << "\n";
        std::cout << msg << "\n";
    }
}

/**
 * Add a replica to the list of listeners.
 * @param r the replica to add
 */
void SocketManager::addOnMessageListener(Replica* r) {
    if (r == nullptr) {
        std::cerr << "Passed in null pointer!" << std::endl;
        exit(1);
    }
    this->listeners.push_back(r);
}

/**
 * Listen for messages being sent.
 */
void SocketManager::listen() {
    while (true) {
        this->temp = this->master;
        if (select(this->fdmax+1, &this->temp, NULL, NULL, &this->tv) == -1) {
            perror("select");
            exit(4);
        } else {
            if (FD_ISSET(this->m_socket, &this->temp)) {
                int received_bytes = recvfrom(this->m_socket, this->in_buffer, 65536, 0,
                    (struct sockaddr*)&this->addr, &this->len);
                if (received_bytes < 0) {
                    perror("recvfrom");
                    exit(1);
                } else if (received_bytes > 0) {
                    this->in_buffer[received_bytes] = '\0';
                    std::string msg(this->in_buffer);
                    json message = json::parse(msg);
                    for (const auto &item: this->listeners) {
                        item->receiveMessage(message);
                    }
                }
            }
        }
    }
}