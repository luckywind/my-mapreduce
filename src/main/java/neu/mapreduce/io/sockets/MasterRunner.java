package neu.mapreduce.io.sockets;

import neu.mapreduce.io.fileSplitter.SplitFile;
import neu.mapreduce.node.NodeDAO;
import neu.mapreduce.node.NodeRegistration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/20/15. Modified by Srikar, Vishal, Mit
 */
public class MasterRunner
{
    private final static Logger LOGGER = Logger.getLogger(MasterRunner.class.getName());


    private final String jobConfClassName;
    int splitSizeInMB;
    ArrayList<String> fileSplits;
    HashMap<String, Socket> slaves;
    HashMap<String, Integer> slaveToSlavePorts;
    String inputJar;

    public MasterRunner(String inputFile, String inputJar, String jobConfClassName, int splitSizeInMB) throws IOException {
        this.splitSizeInMB = splitSizeInMB;
        this.inputJar = inputJar;
        SplitFile splitFile = new SplitFile(splitSizeInMB);
        this.fileSplits = splitFile.splitFile(inputFile);
        this.jobConfClassName = jobConfClassName;
        this.slaveToSlavePorts = new HashMap<>();
        this.slaves = new HashMap<>();
        this.addSlaves();
    }

    public void addSlaves() throws IOException {
        ArrayList<NodeDAO> x = NodeRegistration.getAllNodes();
        Iterator<NodeDAO> it = x.iterator();
        while (it.hasNext()) {
            NodeDAO n = it.next();
            //System.out.println(NodeDAO.getURL(n));
            String ipPort = n.getIp() + ":" + n.getMessagingServicePort();
            slaves.put(ipPort, new Socket(n.getIp(), n.getMessagingServicePort()));
            slaveToSlavePorts.put(ipPort, n.getFileTransferPort());
        }
    }

    public void runJob() throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        MasterScheduler masterScheduler = new MasterScheduler(this.fileSplits, this.inputJar, this.slaves, this.jobConfClassName, this.slaveToSlavePorts);
        masterScheduler.scheduleMap();
        System.out.println("Complete");
    }
    
    /*public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String inputFile = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + "/sample.txt";
        String CLIENT_JAR_WITH_DEPENDENCIES_JAR = "/client-1.4-SNAPSHOT-jar-with-dependencies.jar";
        String inputJar =  Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + CLIENT_JAR_WITH_DEPENDENCIES_JAR;
        String jobConfClassName = "mapperImpl.AirlineJobConf";
        int splitSizeInMB = 64;
        MasterRunner masterDaemon = new MasterRunner(inputFile, inputJar, jobConfClassName, splitSizeInMB);
        masterDaemon.runJob();
    }*/
    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long start = System.currentTimeMillis();
        String inputFile = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + "/inputData.txt";
        String CLIENT_JAR_WITH_DEPENDENCIES_JAR = "/client-1.4-SNAPSHOT-jar-with-dependencies.jar";
        String inputJar = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + CLIENT_JAR_WITH_DEPENDENCIES_JAR;
        String jobConfClassName = "mapperImpl.AirlineJobConf";
        int splitSizeInMB = 64;
        MasterRunner masterDaemon = new MasterRunner(inputFile, inputJar, jobConfClassName, splitSizeInMB);
        masterDaemon.runJob();
        long end = System.currentTimeMillis();
        NodeRegistration.truncateRegistry();
        LOGGER.log(Level.INFO, "It ran for " + (end - start) + " milliseconds");
    }
}