#include "../include/StompProtocol.h"
#include "../include/event.h" 
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

StompProtocol::StompProtocol() : user_name(""), subscription_id_counter(0), receipt_id_counter(0), game_to_subId(), summary_file_names(), pending_receipts(), mtx() {}

std::vector<std::string> StompProtocol::processInput(std::string input) {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<std::string> frames;
    std::stringstream ss(input);
    std::string command;
    ss >> command;

    if (command == "login") {
        std::string hostPort, user, password;
        ss >> hostPort >> user >> password;
        this->user_name = user;
        
        std::string connectFrame = "CONNECT\n"
                                   "accept-version:1.2\n"
                                   "host:stomp.cs.bgu.ac.il\n"
                                   "login:" + user + "\n"
                                   "passcode:" + password + "\n\n";
        frames.push_back(connectFrame);
        return frames;
    }

    if (command == "join") {
        std::string gameName;
        ss >> gameName;
        
        if (game_to_subId.find(gameName) == game_to_subId.end()) {
            game_to_subId[gameName] = subscription_id_counter++;
        }
        int subId = game_to_subId[gameName];
        std::string receiptId = std::to_string(receipt_id_counter++);
        pending_receipts[receiptId] = "Joined channel " + gameName;

        std::string subFrame = "SUBSCRIBE\n"
                               "destination:/" + gameName + "\n"
                               "id:" + std::to_string(subId) + "\n"
                               "receipt:" + receiptId + "\n\n";
                               
        frames.push_back(subFrame);
        return frames;
    }

    if (command == "exit") {
        std::string gameName;
        ss >> gameName;

        if (game_to_subId.find(gameName) != game_to_subId.end()) {
            int subId = game_to_subId[gameName];
            std::string receiptId = std::to_string(receipt_id_counter++);
            pending_receipts[receiptId] = "Exited channel " + gameName;

            std::string unsubFrame = "UNSUBSCRIBE\n"
                                     "id:" + std::to_string(subId) + "\n"
                                     "receipt:" + receiptId + "\n\n";
            frames.push_back(unsubFrame);
        } else {
            std::cout << "You are not subscribed to " << gameName << std::endl;
        }
        return frames;
    }

    if (command == "logout") {
        std::string receiptId = std::to_string(receipt_id_counter++);
        pending_receipts[receiptId] = "Disconnecting";
        
        std::string logoutFrame = "DISCONNECT\n"
                                  "receipt:" + receiptId + "\n\n";
        frames.push_back(logoutFrame);
        return frames;
    }

    if (command == "report") {
        std::string fileName;
        ss >> fileName;
        
        try {
            names_and_events nne = parseEventsFile(fileName);
            std::string gameName = nne.team_a_name + "_" + nne.team_b_name;

            std::cout << "Parsed file " << fileName << ". Sending " << nne.events.size() << " events." << std::endl;

            for (const Event& e : nne.events) {
                std::string frame = "SEND\n";
                frame += "destination:/" + gameName + "\n\n"; 
                
                frame += "user:" + user_name + "\n";
                frame += "team a:" + nne.team_a_name + "\n";
                frame += "team b:" + nne.team_b_name + "\n";
                frame += "event name:" + e.get_name() + "\n";
                frame += "time:" + std::to_string(e.get_time()) + "\n";
                
                frame += "general game updates:\n";
                for (auto const& update : e.get_game_updates()) {
                    frame += "    " + update.first + ":" + update.second + "\n";
                }
                
                frame += "team a updates:\n";
                for (auto const& update : e.get_team_a_updates()) {
                    frame += "    " + update.first + ":" + update.second + "\n";
                }
                
                frame += "team b updates:\n";
                for (auto const& update : e.get_team_b_updates()) {
                    frame += "    " + update.first + ":" + update.second + "\n";
                }
                
                frame += "description:\n" + e.get_discription() + "\n";
                
                frames.push_back(frame);
            }
        } catch (const std::exception& e) {
             std::cout << "Error parsing events file: " << e.what() << std::endl;
        }
        return frames;
    }

    if (command == "summary") {
        std::string gameName, user, fileName;
        ss >> gameName >> user >> fileName;
        summary_file_names[gameName] = fileName;
        std::string frame = "SEND\n";
        frame += "destination:/summary\n";
        frame += "req_game:" + gameName + "\n";
        frame += "req_user:" + user + "\n";
        frame += "file-name:" + fileName + "\n\n";
        
        frames.push_back(frame);
        std::cout << "Sending summary request for " << user << " in " << gameName << std::endl;
        return frames;
    }

    return frames;
}

std::string StompProtocol::processResponse(std::string response) {
    std::lock_guard<std::mutex> lock(mtx);
    std::stringstream ss(response);
    std::string line;
    std::string command;
    std::getline(ss, command);

    if (!command.empty() && command.back() == '\r') command.pop_back();

    if (command == "CONNECTED") {
        return "Login successful";
    }

    if (command == "ERROR") {
        std::string errMsg;
        while(std::getline(ss, line)) {
            if (line.find("message:") != std::string::npos) errMsg = line.substr(8);
        }
        return "Error: " + errMsg;
    }

    if (command == "RECEIPT") {
        std::string receiptId;
        while(std::getline(ss, line)) {
             if (line.find("receipt-id:") != std::string::npos) {
                receiptId = line.substr(11);
                size_t end = receiptId.find_last_not_of(" \t\r\n");
                if (end != std::string::npos) receiptId = receiptId.substr(0, end + 1);
            }
        }
        if (pending_receipts.count(receiptId)) {
            std::string msg = pending_receipts[receiptId];
            pending_receipts.erase(receiptId);
            if (msg == "Disconnecting") return "Ready to close";
            return msg;
        }
    }

    if (command == "MESSAGE") {
        std::string destination;
        std::string targetFile = "";
        std::string body = "";
            
        while (std::getline(ss, line) && line != "" && line != "\r") {
            if (line.back() == '\r') line.pop_back();
            if (line.find("destination:") == 0) destination = line.substr(12);
            if (line.find("file-name:") == 0) targetFile = line.substr(10); 
        }

        std::string bodyLine;
        while (std::getline(ss, bodyLine)) {
            if (bodyLine.back() == '\r') bodyLine.pop_back(); // optional cleanup
            body += bodyLine + "\n";
        }
        if (!body.empty() && body.back() == '\0') body.pop_back();

        std::string gameName = destination;
        if (gameName.find('/') == 0) gameName = gameName.substr(1);

        if (summary_file_names.count(gameName)) {
                std::string targetFile = summary_file_names[gameName];
                std::ofstream outFile(targetFile, std::ios::trunc);
                if (outFile.is_open()) {
                    outFile << body;
                    outFile.close();
                    summary_file_names.erase(gameName);
                    return "Summary saved to " + targetFile;
                }
        }
        return "Received update for " + destination;
    }
    return response;
}