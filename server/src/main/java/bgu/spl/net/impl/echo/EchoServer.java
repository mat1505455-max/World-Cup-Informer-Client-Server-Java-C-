package bgu.spl.net.impl.echo;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.Server;

public class EchoServer {

    public static void main(String[] args) {

        Server.threadPerClient(
                7777, 
                () -> new StompMessagingProtocol<String>() { // Create an anonymous STOMP protocol
                    private int connectionId;
                    private Connections<String> connections;

                    @Override
                    public void start(int connectionId, Connections<String> connections) {
                        this.connectionId = connectionId;
                        this.connections = connections;
                    }

                    @Override
                    public void process(String message) {
                        // Echo logic: Send the received message back using the connections object
                        connections.send(connectionId, message);
                    }

                    @Override
                    public boolean shouldTerminate() {
                        return false;
                    }
                },
                LineMessageEncoderDecoder::new 
        ).serve();
    }
}