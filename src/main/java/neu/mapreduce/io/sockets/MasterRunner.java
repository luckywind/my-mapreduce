package neu.mapreduce.io.sockets;

import neu.mapreduce.io.fileSplitter.SplitFile;
import neu.mapreduce.autodiscovery.NodeDAO;
import neu.mapreduce.autodiscovery.NodeRegistration;

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

    /**
     * Public constructor which receives input data
     * @param inputFile File path of client's input data
     * @param inputJar File path of client's input JAR
     * @param jobConfClassName Name of input job configuration class
     * @param splitSizeInMB File size for each input chunk to mapper
     * @throws IOException
     */
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

    /**
     * Detect and add all running slave nodes.
     * @throws IOException
     */
    public void addSlaves() throws IOException {
        ArrayList<NodeDAO> x = NodeRegistration.getAllNodes();
        Iterator<NodeDAO> it = x.iterator();
        while (it.hasNext()) {
            NodeDAO n = it.next();
            String ipPort = n.getIp() + ":" + n.getMessagingServicePort();
            slaves.put(ipPort, new Socket(n.getIp(), n.getMessagingServicePort()));
            slaveToSlavePorts.put(ipPort, n.getFileTransferPort());
        }
    }

    /**
     * Starts running the MapReduce job.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public void runJob() throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        MasterScheduler masterScheduler = new MasterScheduler(this.fileSplits, this.inputJar, this.slaves, this.jobConfClassName, this.slaveToSlavePorts);
        masterScheduler.scheduleMap();
        System.out.println("Complete");
    }

    /**
     * Get the ball rolling. Start point of MasterRunner
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void runMaster() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long start = System.currentTimeMillis();
        this.runJob();
        long end = System.currentTimeMillis();
        NodeRegistration.truncateRegistry();
        LOGGER.log(Level.INFO, "It ran for " + (end - start) + " milliseconds");
    }
}