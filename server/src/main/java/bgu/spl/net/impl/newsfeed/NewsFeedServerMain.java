package bgu.spl.net.impl.newsfeed;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.rci.ObjectEncoderDecoder;
import bgu.spl.net.impl.rci.RemoteCommandInvocationProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.Server;

import java.io.Serializable;

public class NewsFeedServerMain {

    public static void main(String[] args) {
        NewsFeed feed = new NewsFeed();

        Server.threadPerClient(
                7777, 
                () -> new StompMessagingProtocol<Serializable>() { // Adapt to STOMP
                    private final RemoteCommandInvocationProtocol<NewsFeed> innerProtocol = new RemoteCommandInvocationProtocol<>(feed);
                    private int connectionId;
                    private Connections<Serializable> connections;

                    @Override
                    public void start(int connectionId, Connections<Serializable> connections) {
                        this.connectionId = connectionId;
                        this.connections = connections;
                    }

                    @Override
                    public void process(Serializable message) {
                        Serializable response = innerProtocol.process(message);
                        if (response != null) {
                            connections.send(connectionId, response);
                        }
                    }

                    @Override
                    public boolean shouldTerminate() {
                        return false;
                    }
                },
                ObjectEncoderDecoder::new
        ).serve();
    }
}