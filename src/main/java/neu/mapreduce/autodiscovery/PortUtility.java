package neu.mapreduce.autodiscovery;

/**
 * Created by vishal on 4/13/15.
 */
import neu.mapreduce.io.sockets.IOCommons;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

/**
 * Finds an available port on localhost.
 * Reference: http://fahdshariff.blogspot.com/2012/10/java-find-available-port-number.html
 */
public class PortUtility {
    private static final int MIN_PORT_NUMBER = 7000;
    private static final int MAX_PORT_NUMBER = 10000;
    public static final int ONE = 1;
    private static  int nextPortNumber = MIN_PORT_NUMBER;

    /**
     * Find a free port number between MIN_PORT_NUMBER and MAX_PORT_NUMBER
     * @return Next free port number
     */
    public static int findFreePort() {
        for (int i = nextPortNumber; i <= MAX_PORT_NUMBER; i++) {
            if (available(i)) {
                nextPortNumber = i + ONE;
                return i;
            }
        }
        throw new RuntimeException("Could not find an available port between " +
                MIN_PORT_NUMBER + " and " + MAX_PORT_NUMBER);
    }

    /**
     * Returns true if the specified port is available on this host.
     *
     * @param port the port to check
     * @return true if the port is available, false otherwise
     */
    private static boolean available(final int port) {
        ServerSocket serverSocket = null;
        DatagramSocket dataSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            dataSocket = new DatagramSocket(port);
            dataSocket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            IOCommons.shutDownDatagramSocket(dataSocket);
            IOCommons.shutDownServerSocket(serverSocket);
        }
    }
}
