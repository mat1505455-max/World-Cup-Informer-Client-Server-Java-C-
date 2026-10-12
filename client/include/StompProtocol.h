#pragma once
#include <string>
#include <vector>
#include <map>
#include <mutex>


class StompProtocol {
private:
    std::string user_name;
    int subscription_id_counter;
    int receipt_id_counter;
    
    std::map<std::string, int> game_to_subId;
        
    std::map<std::string, std::string> pending_receipts; 

    std::map<std::string, std::string> summary_file_names;

    std::mutex mtx;

public:
    StompProtocol();
    
    std::vector<std::string> processInput(std::string input);
    
    std::string processResponse(std::string response);

    std::string generateSummaryString(std::string gameName);
};