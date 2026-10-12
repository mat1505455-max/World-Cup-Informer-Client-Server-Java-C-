package bgu.spl.net.srv;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class DatabaseConnector {
    private final String host;
    private final int port;

    public DatabaseConnector(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public synchronized String sendSQL(String query) {
        String message = query + "\0";
        
        try (Socket socket = new Socket(host, port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {
            
            out.write(message.getBytes(StandardCharsets.UTF_8));
            out.flush();

            StringBuilder response = new StringBuilder();
            int b;
            while ((b = in.read()) != -1 && b != 0) {
                response.append((char) b);
            }
            
            return response.toString();
            
        } catch (IOException e) {
            return "ERROR: " + e.getMessage();
        }
    }
}