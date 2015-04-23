package neu.mapreduce.node;

import java.io.IOException;

/**
 * Created by vishal on 4/13/15.
 * Data Access Object class for node information. It will have
 * IP and port numbers for the node
 */

public class NodeDAO {
    String ip;
    int fileTransferPort;
    int messagingServicePort;

    public static int LOCAL_IP_POSITION = 1;

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

    public  NodeDAO(){

    }

    public NodeDAO(int messagingServicePort, int fileTransferPort, String ip) {
        this.messagingServicePort = messagingServicePort;
        this.fileTransferPort = fileTransferPort;
        this.ip = ip;
    }

    public void registerThisNode() throws IOException {
        String allIPs = NodeRegistration.getIPsInString();
        String[] splitedIPs = allIPs.split(" ");
        String ip = splitedIPs [LOCAL_IP_POSITION];
        setIp(ip);
        setFileTransferPort(PortFinder.findFreePort());
        setMessagingServicePort(PortFinder.findFreePort()+1);
        NodeRegistration.register(ip+":"+getFileTransferPort()+":"+getMessagingServicePort());

    }
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getFileTransferPort() {
        return fileTransferPort;
    }

    public void setFileTransferPort(int fileTransferPort) {
        this.fileTransferPort = fileTransferPort;
    }

    public int getMessagingServicePort() {
        return messagingServicePort;
    }

    public void setMessagingServicePort(int messagingServicePort) {
        this.messagingServicePort = messagingServicePort;
    }
}
