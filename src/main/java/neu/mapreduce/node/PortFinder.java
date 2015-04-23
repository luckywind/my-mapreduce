package neu.mapreduce.node;

/**
 * Created by vishal on 4/13/15.
 */
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

/**
 * Finds an available port on localhost.
 */
public class PortFinder {
    private static final int MIN_PORT_NUMBER = 7000;
    private static final int MAX_PORT_NUMBER = 10000;

    /**
     * Finds a free port between
     * {@link #MIN_PORT_NUMBER} and {@link #MAX_PORT_NUMBER}.
     *
     * @return a free port
     * @throw RuntimeException if a port could not be found
     */
    public static int findFreePort() {
        for (int i = MIN_PORT_NUMBER; i <= MAX_PORT_NUMBER; i++) {
            if (available(i)) {
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
        } catch (final IOException e) {
            return false;
        } finally {
            if (dataSocket != null) {
                dataSocket.close();
            }
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (final IOException e) {
                    // can never happen
                }
            }
        }
    }
}
