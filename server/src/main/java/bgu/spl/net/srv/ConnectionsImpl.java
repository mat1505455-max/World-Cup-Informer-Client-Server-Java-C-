package bgu.spl.net.srv;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import bgu.spl.net.impl.data.User;

public class ConnectionsImpl<T> implements Connections<T> {
    private final Map<Integer, ConnectionHandler<T>> activeConnections = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> channelSubscribers = new ConcurrentHashMap<>();
    private final Map<String, User> registeredUsers = new ConcurrentHashMap<>();
    private final Map<Integer, Map<String, String>> clientSubscriptions = new ConcurrentHashMap<>();
    private int messageIdCounter = 1;

    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = activeConnections.get(connectionId);
        if (handler != null) {
            handler.send(msg);
            return true;
        }
        return false;
    }

    @Override
    public void send(String channel, T msg) {
        Set<Integer> subscribers = channelSubscribers.get(channel);
        
        if (subscribers != null) {
            for (Integer connId : subscribers) {
                String subId = null;
                Map<String, String> userSubs = clientSubscriptions.get(connId);
                if (userSubs != null) {
                    for (Map.Entry<String, String> entry : userSubs.entrySet()) {
                        if (entry.getValue().equals(channel)) {
                            subId = entry.getKey();
                            break;
                        }
                    }
                }

                if (subId != null) {
                    String messageFrame = "MESSAGE\n" +
                                        "subscription:" + subId + "\n" +
                                        "message-id:" + (messageIdCounter++) + "\n" +
                                        "destination:" + channel + "\n\n" +
                                        msg + "\0";
                    
                    send(connId, (T) messageFrame); 
                }
            }
        }
    }

    @Override
    public void disconnect(int connectionId) {
        activeConnections.remove(connectionId);
        for (Set<Integer> subscribers : channelSubscribers.values()) {
            subscribers.remove(connectionId);
        }
    }

    public void addConnection(int connectionId, ConnectionHandler<T> handler) {
        activeConnections.put(connectionId, handler);
    }
    public User getUser(String username) {
        return registeredUsers.get(username);
    }

    public void registerUser(int connectionId, String name, String password) {
        User newUser = new User(connectionId, name, password);
        registeredUsers.put(name, newUser);
    }

    public void subscribe(int connectionId, String channel, String subId) {
    channelSubscribers.computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>()).add(connectionId);
    clientSubscriptions.computeIfAbsent(connectionId, k -> new ConcurrentHashMap<>()).put(subId, channel);
    }

    public void unsubscribe(int connectionId, String subId) {
        Map<String, String> userSubs = clientSubscriptions.get(connectionId);
        if (userSubs != null) {
            String channelName = userSubs.remove(subId);
            
            if (channelName != null) {
                Set<Integer> subscribers = channelSubscribers.get(channelName);
                if (subscribers != null) {
                    subscribers.remove(connectionId);
                }
            }
        }
    }

    public User getUserByConnId(int connectionId) {
        for (User user : registeredUsers.values()) {
            if (user.isLoggedIn() && user.getConnectionId() == connectionId) {
                return user;
            }
        }
        return null;
    }
}