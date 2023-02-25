#include <iostream>
#include "../include/SocketManager.h"

/**
 * Handle command line input
 * @param argc argument count
 * @param argv argument vector
 */
int main(int argc, char* argv[]) {
    int port = atoi(argv[1]);
    std::string id = argv[2];
    srand48(time(NULL) * atoi(argv[2]));

    std::vector<std::string> others(argc - 3);
    for (int i = 3; i < argc; i++) {
        others[i - 3] = argv[i];
    }

    Replica r(port, "127.0.0.1", id, others, argc - 3);
    r.run();
}