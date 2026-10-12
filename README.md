# World Cup 2026 STOMP Informer ⚽🌍

A community-led real-time update subscription service implementing the **STOMP 1.2 protocol** (Simple-Text-Oriented-Messaging-Protocol) from scratch. The system consists of a multi-threaded Java server (supporting both **Thread-Per-Client** and **Reactor** concurrency patterns) and a multi-threaded C++ client managing asynchronous socket communication and console interaction.

---

## 🚀 Key Features & Architecture

- **Dual-Model Java Server:** 
  - Implemented both Thread-Per-Client (TPC) and Reactor server patterns.
  - Designed robust connection and protocol management interfaces (`Connections`, `StompMessagingProtocol`, `ConnectionHandler`) to handle active client sessions, subscriptions, and topic broadcasting.
- **Multi-Threaded C++ Client:** 
  - Built a concurrent client utilizing two independent threads: one dedicated to non-blocking standard input (keyboard commands) and another continuously listening to incoming socket frames.
  - Supports full STOMP client lifecycle commands: `login`, `join`, `exit`, `report`, `summary`, and `logout`.
- **Protocol Compliance:** Fully conforms to STOMP 1.2 frame formatting rules (commands, headers, body separation, and null-character termination).

---

## 🛠️ Build & Run Instructions

### 🖥️ Server (Java / Maven)
Compile and run the server supporting either TPC or Reactor modes:
```bash
# Compile using Maven
mvn compile

# Run Thread-Per-Client server
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="<port> tpc"

# Run Reactor server
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="<port> reactor"

💻 Client (C++ / Make)
Build and run the client executable:

Bash
# Build using Makefile
make

# Run the client
./bin/StompWCIClient

📂 Project Structure
├── server/          # Java backend (TPC/Reactor servers, STOMP protocol, Maven config)
├── client/          # C++ frontend client (Multi-threaded I/O, event parsing, Makefile)
└── README.md