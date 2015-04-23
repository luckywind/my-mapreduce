package neu.mapreduce.autodiscovery;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by vishal on 4/13/15.
 * Data Access Object class for autodiscovery information. It will have
 * IP and port numbers for the autodiscovery
 */

public class NodeDAO {
    private static final Logger LOGGER = Logger.getLogger(NodeDAO.class.getName());
    public static final String RAW_REGISTRY_SPLITTER = "\\t";
    public static final String IP_PORT_SPLITTER = ":";
    public static final int ZERO = 0;
    public static final int LENGTH_ONLY_IP = 1;
    public static final int LENGTH_IP_PORTS = 3;
    public static final int LOCAL_IP_POSITION = 1;
    public static final String MULTIPLE_IP_SPLITTER = " ";

    private String ip;
    private int fileTransferPort;
    private int messagingServicePort;

    /**
     * Public default constructor
     */
    public NodeDAO() {
    }

    /**
     * Constructor with IP address initialization
     *
     * @param ipAndPorts IP address which has IP address with the file transfer and messaging port number
     */
    public NodeDAO(String ipAndPorts) {
        String[] line = ipAndPorts.split(RAW_REGISTRY_SPLITTER);
        String[] ipPorts = line[ZERO].split(IP_PORT_SPLITTER);
        if (ipPorts.length == LENGTH_ONLY_IP) {
            this.ip = ipAndPorts;
        } else if (ipPorts.length == LENGTH_IP_PORTS) {
            registerIPPorts(ipPorts);
        } else {
            LOGGER.log(Level.SEVERE, "Incorrect input to the slave registry");
        }
    }

    /**
     * Constructor with IP address and ports initialization
     *
     * @param messagingServicePort The port number on which the autodiscovery will receive status signals
     * @param fileTransferPort     The port used for transfer of file between the nodes
     * @param ip                   IP address on which current autodiscovery is listening
     */
    public NodeDAO(int messagingServicePort, int fileTransferPort, String ip) {
        this.messagingServicePort = messagingServicePort;
        this.fileTransferPort = fileTransferPort;
        this.ip = ip;
    }

    /**
     * Helps the constructor to initialize IP and ports*
     * @param ipPorts String array with IP and Ports
     */
    private void registerIPPorts(String[] ipPorts) {
        this.ip = ipPorts[ZERO];
        try {
            this.messagingServicePort = Integer.parseInt(ipPorts[1]);
            this.fileTransferPort = Integer.parseInt(ipPorts[2]);
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Invalid port number in the slave registry");
        }
    }

    /**
     * Registers the autodiscovery to registry at http://www.jquerypluginscripts.com/nodes.txt
     * Next available ports are automatically found and then assigned to this autodiscovery
     * @throws IOException
     */
    public void registerThisNode() throws IOException {
        String allIPs = NodeRegistration.getIPsInString();
        String[] splitedIPs = allIPs.split(MULTIPLE_IP_SPLITTER);
        String ip = splitedIPs[LOCAL_IP_POSITION];
        setIp(ip);
        setFileTransferPort(PortUtility.findFreePort());
        setMessagingServicePort(PortUtility.findFreePort());
        NodeRegistration.register(ip + ":" + getFileTransferPort() + ":" + getMessagingServicePort());
    }

    /**
     * Get IP where this autodiscovery is listening
     *
     * @return Returns IP address where this autodiscovery is listening
     */
    public String getIp() {
        return ip;
    }

    /**
     * Set IP where this autodiscovery will listen
     *
     * @param ip IP address where this autodiscovery listen
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * Get file transfer port of this autodiscovery
     *
     * @return File transfer port of this autodiscovery
     */
    public int getFileTransferPort() {
        return fileTransferPort;
    }

    /**
     * Set file transfer port of this autodiscovery
     *
     * @param fileTransferPort A integer which represents port where this autodiscovery will listen
     */
    public void setFileTransferPort(int fileTransferPort) {
        this.fileTransferPort = fileTransferPort;
    }

    /**
     * Get message service port
     *
     * @return Port number where this autodiscovery is listening for message communication
     */
    public int getMessagingServicePort() {
        return messagingServicePort;
    }

    /**
     * Set message service port
     *
     * @param messagingServicePort Port number where this autodiscovery will listen for message communication
     */
    public void setMessagingServicePort(int messagingServicePort) {
        this.messagingServicePort = messagingServicePort;
    }
}
