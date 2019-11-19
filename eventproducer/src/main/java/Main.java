
import java.io.IOException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println(new Event());
        String host = args[0];
        Integer port = Integer.valueOf(args[1]);
        int incr = 0;
        System.out.println(host + port);
        InetSocketAddress address = new InetSocketAddress(host, port);

        SocketChannel socket = SocketChannel.open(address);
        while (true) {
            System.out.println(" enter number events");
            Scanner s = new Scanner(System.in);
            if (s.hasNextInt()) {
                int numEvent = s.nextInt();

                for (int i = 1; i < numEvent; i++) {
                    if (incr == 300) {
                        Thread.sleep(2000);
                        incr = 0;
                    }
                    Event event = new Event();
                    System.out.println(event);
                    ByteBuffer buffer = ByteBuffer.wrap(event.toString().getBytes());
                    socket.write(buffer);
                    incr++;
                }
                System.out.println("done");//
            }
        }
    }


}
