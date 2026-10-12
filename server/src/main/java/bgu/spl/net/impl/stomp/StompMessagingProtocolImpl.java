package bgu.spl.net.impl.stomp;

import java.time.LocalDateTime;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.data.User;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;
import bgu.spl.net.srv.DatabaseConnector;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {

    private int connectionId;
    private Connections<String> connections;
    private boolean shouldTerminate = false;
    private DatabaseConnector db = new DatabaseConnector("127.0.0.1", 7778);

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(String message) {
        if (message == null || message.trim().isEmpty()) return;
        String[] lines = message.split("\n");
        if (lines.length == 0) return;
        String command = lines[0].trim();

        try {
            switch (command) {
                case "CONNECT": handleConnect(lines); break;
                case "SUBSCRIBE": if (handleSubscribe(lines)) sendReceiptIfRequested(lines); break;
                case "SEND": if (handleSend(lines, message)) sendReceiptIfRequested(lines); break;
                case "UNSUBSCRIBE": if (handleUnsubscribe(lines)) sendReceiptIfRequested(lines); break;
                case "DISCONNECT": handleDisconnect(lines); break;
                default: sendError("Unknown command: " + command, null); break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            sendError("Server error: " + e.getMessage(), null);
        }
    }

    private boolean handleSend(String[] lines, String fullFrame) {
        String destination = getHeader(lines, "destination");
        if (destination == null) {
            sendError("Malformatted SEND frame: no destination", getHeader(lines, "receipt"));
            return false;
        }

        if (destination.equals("/summary")) {
            String reqGame = getHeader(lines, "req_game");
            String reqUser = getHeader(lines, "req_user");
            String targetFile = getHeader(lines, "file-name");
            
            System.out.println("Processing Summary Request for: " + reqUser + " in " + reqGame);
            
            String query = "SELECT filename, upload_time FROM file_tracking " +
                           "WHERE username = '" + reqUser + "' AND game_channel = '" + reqGame + "'"; 
            

            String result = db.sendSQL(query);
            
            int rowCount = result.split("\n").length - 1; 
            System.out.println("Fetching " + (rowCount > 0 ? rowCount : 0) + " rows from DB");

            StringBuilder response = new StringBuilder();
            response.append("MESSAGE\n");
            response.append("destination:/summary\n");
            response.append("message-id:").append(System.currentTimeMillis()).append("\n");
            if (targetFile != null) response.append("file-name:").append(targetFile).append("\n");
            response.append("\n");
            
            response.append("Summary for ").append(reqUser).append(" in ").append(reqGame).append(":\n");
            response.append("--------------------------------------------------\n");
            response.append(result).append("\n");
            response.append("\0");

            connections.send(connectionId, response.toString());
            return true;
        }

        int bodyStartIndex = fullFrame.indexOf("\n\n");
        if (bodyStartIndex == -1) bodyStartIndex = fullFrame.indexOf("\r\n\r\n");
        String body = "";
        if (bodyStartIndex != -1) {
            int offset = (fullFrame.charAt(bodyStartIndex) == '\r') ? 4 : 2;
            body = fullFrame.substring(bodyStartIndex + offset).trim();
            if (body.endsWith("\0")) body = body.substring(0, body.length() - 1);
        }

        ConnectionsImpl<String> connImpl = (ConnectionsImpl<String>) connections;
        User user = connImpl.getUserByConnId(connectionId);
        
        if (user != null && body.contains("event name:")) {
            try {
                String generatedFilename = "report_" + destination.replace("/", "") + "_" + System.currentTimeMillis();
                String query = "INSERT INTO file_tracking (username, filename, upload_time, game_channel) VALUES ('" 
                                + user.name + "', '" + generatedFilename + "', '" + LocalDateTime.now() + "', '" + destination + "');";
                db.sendSQL(query);
            } catch (Exception e) {
                System.out.println("SQL Error: " + e.getMessage());
            }
        }
        connImpl.send(destination, body);
        return true;
    }
    
    private void handleConnect(String[] lines) {
        String login = getHeader(lines, "login");
        String passcode = getHeader(lines, "passcode");
        ConnectionsImpl<String> connImpl = (ConnectionsImpl<String>) connections;
        User user = connImpl.getUser(login);
        if (user == null) {
            connImpl.registerUser(connectionId, login, passcode);
            user = connImpl.getUser(login);
            try { db.sendSQL("INSERT OR IGNORE INTO users (username, password, registration_date) VALUES ('" + login + "', '" + passcode + "', '" + LocalDateTime.now() + "');"); } catch (Exception e) {}
        } else {
            if (!user.password.equals(passcode)) { sendError("Wrong password", null); return; }
            if (user.isLoggedIn()) { sendError("User already logged in", null); return; }
        }
        user.login();
        user.setConnectionId(connectionId);
        try { db.sendSQL("INSERT INTO login_history (username, login_time) VALUES ('" + login + "', '" + LocalDateTime.now() + "');"); } catch (Exception e) {}
        connections.send(connectionId, "CONNECTED\nversion:1.2\n\n\0");
        sendReceiptIfRequested(lines);
    }
    
    private boolean handleSubscribe(String[] lines) { 
        String destination = getHeader(lines, "destination");
        String subId = getHeader(lines, "id");
        if (destination == null || subId == null) return false;
        ((ConnectionsImpl<String>) connections).subscribe(connectionId, destination, subId);
        return true;
    }
    
    private boolean handleUnsubscribe(String[] lines) {
        String subId = getHeader(lines, "id");
        if (subId == null) return false;
        ((ConnectionsImpl<String>) connections).unsubscribe(connectionId, subId);
        return true;
    }
    
    private void handleDisconnect(String[] lines) {
        ConnectionsImpl<String> connImpl = (ConnectionsImpl<String>) connections;
        User user = connImpl.getUserByConnId(connectionId);
        if (user != null) {
            try { db.sendSQL("UPDATE login_history SET logout_time = '" + LocalDateTime.now() + "' WHERE username = '" + user.name + "' AND logout_time IS NULL;"); } catch (Exception e) {}
            user.logout();
        }
        sendReceiptIfRequested(lines);
        shouldTerminate = true;
    }

    private void sendReceiptIfRequested(String[] lines) {
        String receiptId = getHeader(lines, "receipt");
        if (receiptId != null) connections.send(connectionId, "RECEIPT\nreceipt-id:" + receiptId + "\n\n\0");
    }

    private void sendError(String errorMessage, String receiptId) {
        String errorFrame = "ERROR\nmessage:" + errorMessage + "\n\n\0";
        connections.send(connectionId, errorFrame);
        shouldTerminate = true;
        connections.disconnect(connectionId);
    }

    private String getHeader(String[] lines, String key) {
        for (String line : lines) {
            if (line.startsWith(key + ":")) return line.substring(key.length() + 1).trim();
        }
        return null;
    }
    
    @Override
    public boolean shouldTerminate() { return shouldTerminate; }
}