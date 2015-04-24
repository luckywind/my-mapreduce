package neu.mapreduce.autodiscovery;

/**
 * Created by Vishal on 4/22/15.
 */

import org.apache.commons.io.IOUtils;

import java.io.InputStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;

public class NodeRegistration {

    public static final String TRUNCATE_OPERATION_PARAM = "trunc";
    public static final String SEND_REQUEST_DATA = "";
    public static final String TRUE = "true";
    public static final String REGISTER_ME_CMD = "regme";
    public static final String REGISTRY_URL = "http://www.jquerypluginscripts.com/mr.php";
    public static final String REGISTRY_DATA_FILE = "http://jquerypluginscripts.com/nodes.txt";
    public static final String LINE_SPLITTER = "\n";
    public static final String DATA_FILE_IP_PORT_SPLITTER = "\\|";
    public static final int ZERO = 0;
    public static final String UBUNTU_NETWORK_NAME = "wlan0";
    public static final String MAC_NETWORK_NAME = "en0";

    /**
     * Delete registry file http://jquerypluginscripts.com/nodes.txt
     *
     * @return true if the file successfully deleted
     * @throws IOException
     */
    public static boolean truncateRegistry() throws IOException {
        String result = sendRequest(TRUNCATE_OPERATION_PARAM, SEND_REQUEST_DATA);
        if (result.equals(TRUE))
            return true;
        return false;
    }

    /**
     * Register called to server with metadata sent in 'data'
     *
     * @param metadata Metadata about this autodiscovery
     * @return Returns the output received from web service
     * @throws IOException
     */
    public static String register(String metadata) throws IOException {
        return sendRequest(REGISTER_ME_CMD, metadata);
    }

    /**
     * Send POST request to Webservice.
     *
     * @param operationType Type of operation to be performed as below
     *                      "trunc": Delete file http://jquerypluginscripts.com/nodes.txt
     *                      Returns boolean to indicate success of failure
     *                      "regme": Register self in nodes.txt. Public IP of current caller will
     *                      automatically be detected and written in the file
     * @param data          Actual data written in the file
     * @return Actual string written into the nodes.txt or NODATAWRITTEN,
     * true/false or OPNotDefined depending upon operation success (Refer PHP file for details)
     * @throws IOException
     */
    public static String sendRequest(String operationType, String data) throws IOException {
        URLConnection conn = getConnection();
        writeToWebService(conn, operationType, data);
        return readFromWebService(conn);
    }

    /**
     * Get connection object for the registry URL
     *
     * @return connection object for the registry URL
     * @throws IOException
     */
    public static URLConnection getConnection() throws IOException {
        URL url = new URL(REGISTRY_URL);
        URLConnection conn = url.openConnection();
        conn.setDoOutput(true);
        return conn;
    }

    /**
     * * Invoke the webservice with the type of operation and the data
     *
     * @param operationType Type of operation. It can be either add new entry in registry or clean the registry
     * @param data          The data parameter for the webservice
     * @throws IOException
     */
    public static void writeToWebService(URLConnection conn, String operationType, String data) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
        writer.write("op=" + operationType + "&data=" + data);
        writer.flush();
        writer.close();
    }

    /**
     * * Reads the output from web service
     *
     * @param conn Connection details for the web service
     * @return All lines received from the web service URL
     * @throws IOException
     */
    private static String readFromWebService(URLConnection conn) throws IOException {
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder sb = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return new String(sb);
    }

    /**
     * Get all slave nodes in the registry
     *
     * @return Array list having all the slave nodes.
     * @throws IOException
     */
    public static ArrayList<NodeDAO> getAllNodes() throws IOException {
        InputStream input = new URL(REGISTRY_DATA_FILE).openStream();
        ArrayList<NodeDAO> slaveConnectionDetails = new ArrayList<>();
        try {
            String[] allNodesContent = IOUtils.toString(input).split(LINE_SPLITTER);
            for (String eachNode : allNodesContent) {
                String[] ipList = eachNode.split(DATA_FILE_IP_PORT_SPLITTER);
                slaveConnectionDetails.add(new NodeDAO(ipList[ZERO]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(input);
        }
        return slaveConnectionDetails;
    }

    /**
     * Get various IPv4 address addresses of the current slave autodiscovery. These IPs are taken Operating System
     *
     * @return IPv4 address in string
     * @throws SocketException
     */
    public static String getIPsInString() throws SocketException {
        Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface ni = e.nextElement();
            if (ni.getName().equals(UBUNTU_NETWORK_NAME) || ni.getName().equals(MAC_NETWORK_NAME)) {
               return getIPFromNetworkInterface(ni);
            }
        }
        return null;
    }

    /**
     * Gets IPv4 address from Network interface instance
     * @param ni Network interface instance
     * @return IPv4 address in string
     */
    private static String getIPFromNetworkInterface(NetworkInterface ni) {
        Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
        while (inetAddresses.hasMoreElements()) {
            InetAddress address = inetAddresses.nextElement();
            if (address instanceof Inet4Address) {
                return address.getHostAddress();
            }
        }
        return null;
    }
    /**
     * To truncate registry data file*
     *
     * @param args Input arguments
     */
    public static void main(String[] args) {
        try {
            System.out.println(getIPsInString());
        } catch (IOException e) {
        }
    }
}