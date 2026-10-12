#include <iostream>
#include <thread>
#include <vector>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"

void socketThreadTask(ConnectionHandler &handler, StompProtocol &protocol) {
    while (true) {
        std::string answer;
        if (!handler.getFrameAscii(answer, '\0')) {
            std::cout << "Disconnected from server" << std::endl;
            break;
        }
        std::string result = protocol.processResponse(answer);
        std::cout << result << std::endl;
        if (result.find("Ready to close") != std::string::npos) {
            handler.close();
            break;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " host port" << std::endl;
        return -1;
    }
    std::string host = argv[1];
    short port = atoi(argv[2]);

    ConnectionHandler handler(host, port);
    if (!handler.connect()) {
        std::cerr << "Cannot connect to " << host << ":" << port << std::endl;
        return -1;
    }
	std::cout << "Connected to the server successfully!" << std::endl;
    StompProtocol protocol;
    std::thread readerThread(socketThreadTask, std::ref(handler), std::ref(protocol));
    while (true) {
        const short bufsize = 1024;
        char buf[bufsize];
        std::cin.getline(buf, bufsize);
        std::string line(buf);
        std::vector<std::string> frames = protocol.processInput(line); 
        if (line == "SERVER_REPORT") {
            std::string reportFrame = "SERVER_REPORT\n\n"; 
            
            if (!handler.sendFrameAscii(reportFrame, '\0')) { 
                std::cout << "Disconnected from server" << std::endl;
                break;
            }
            continue;
        }
            for (const std::string& frame : frames) {
                if (frame != "") {
                    if (!handler.sendFrameAscii(frame, '\0')) {
                        std::cout << "Disconnected from server" << std::endl;
                        handler.close();
                        break;
                    }
                }
            }
        if (line == "logout") {
            if (readerThread.joinable()) {
                readerThread.join();
            }
            break;
        }
    }
    return 0;
}