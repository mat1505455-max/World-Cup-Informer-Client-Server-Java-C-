package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

public class StompServer {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Please provide port and server type (tpc/reactor)");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String serverType = args[1];

        if (serverType.equals("tpc")) {
            Server.<String>threadPerClient(
                    port, 
                    StompMessagingProtocolImpl::new, 
                    StompEncDec::new              
            ).serve();
        } else if (serverType.equals("reactor")) {
            Server.reactor(
                    5,
                    port,
                    StompMessagingProtocolImpl::new,
                    StompEncDec::new
            ).serve();
        }
    }
}