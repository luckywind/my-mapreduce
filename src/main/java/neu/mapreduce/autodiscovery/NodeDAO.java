package neu.mapreduce.autodiscovery;

import java.io.IOException;

/**
 * Created by vishal on 4/13/15.
 * Data Access Object class for autodiscovery information. It will have
 * IP and port numbers for the autodiscovery
 */

public class NodeDAO {
    String ip;
    int fileTransferPort;
    int messagingServicePort;

    public static int LOCAL_IP_POSITION = 1;

    /**
     * Public constructor
     * @param ip IP address which has IP address with the file transfer and messaging port number
     */
    public NodeDAO(String ip) {
        String[] line = ip.split("\\t");
        String [] ipPorts = line[0].split(":");
        this.ip = ip;

        if(ipPorts.length == 3){
            this.ip = ipPorts[0];
            try {
                this.messagingServicePort = Integer.parseInt(ipPorts[1]);
                this.fileTransferPort = Integer.parseInt(ipPorts[2]);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     *  Public default constructor
     */
    public  NodeDAO(){

    }

    /**
     *
     * @param messagingServicePort  The port number on which the autodiscovery will receive status signals
     * @param fileTransferPort The port used for transfer of file between the nodes
     * @param ip IP address on which current autodiscovery is listening
     */
    public NodeDAO(int messagingServicePort, int fileTransferPort, String ip) {
        this.messagingServicePort = messagingServicePort;
        this.fileTransferPort = fileTransferPort;
        this.ip = ip;
    }

    /**
     * Registers the autodiscovery to registry at http://www.jquerypluginscripts.com/nodes.txt
     * Next available ports are automatically found and then assigned to this autodiscovery
     * @throws IOException
     */
    public void registerThisNode() throws IOException {
        String allIPs = NodeRegistration.getIPsInString();
        String[] splitedIPs = allIPs.split(" ");
        String ip = splitedIPs [LOCAL_IP_POSITION];
        setIp(ip);
        setFileTransferPort(PortUtility.findFreePort());
        setMessagingServicePort(PortUtility.findFreePort());
        NodeRegistration.register(ip+":"+getFileTransferPort()+":"+getMessagingServicePort());

    }

    /**
     * Get IP where this autodiscovery is listening
     * @return Returns IP address where this autodiscovery is listening
     */
    public String getIp() {
        return ip;
    }

    /**
     * Set IP where this autodiscovery will listen
     * @param ip IP address where this autodiscovery listen
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * Get file transfer port of this autodiscovery
     * @return File transfer port of this autodiscovery
     */
    public int getFileTransferPort() {
        return fileTransferPort;
    }

    /**
     * Set file transfer port of this autodiscovery
     * @param fileTransferPort A integer which represents port where this autodiscovery will listen
     */
    public void setFileTransferPort(int fileTransferPort) {
        this.fileTransferPort = fileTransferPort;
    }

    /**
     * Get message service port
     * @return Port number where this autodiscovery is listening for message communication
     */
    public int getMessagingServicePort() {
        return messagingServicePort;
    }

    /**
     * Set message service port
     * @param messagingServicePort Port number where this autodiscovery will listen for message communication
     */
    public void setMessagingServicePort(int messagingServicePort) {
        this.messagingServicePort = messagingServicePort;
    }
}
