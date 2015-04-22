package neu.mapreduce.io.sockets;

import neu.mapreduce.io.fileSplitter.SplitFile;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Amitash on 4/20/15.
 */
public class MasterRunner
{
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
        //TODO: Should access slave file given online. Remove constant below.
        slaves.put("localhost:8087", new Socket("localhost", 8087));
        slaves.put("localhost:8083", new Socket("localhost", 8083));
        slaves.put("localhost:8090", new Socket("localhost", 8090));
        slaveToSlavePorts.put("localhost:8087", 7062);
        slaveToSlavePorts.put("localhost:8083", 7067);
        slaveToSlavePorts.put("localhost:8090", 7090);
    }

    public void runJob() throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        MasterScheduler masterScheduler = new MasterScheduler(this.fileSplits, this.inputJar, this.slaves, this.jobConfClassName, this.slaveToSlavePorts);
        masterScheduler.scheduleMap();
        System.out.println("Complete");
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String inputFile = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + "/sample.txt";
        String CLIENT_JAR_WITH_DEPENDENCIES_JAR = "/client-1.4-SNAPSHOT-jar-with-dependencies.jar";
        String inputJar =  Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + CLIENT_JAR_WITH_DEPENDENCIES_JAR;
        String jobConfClassName = "mapperImpl.AirlineJobConf";
        int splitSizeInMB = 64;
        MasterRunner masterDaemon = new MasterRunner(inputFile, inputJar, jobConfClassName, splitSizeInMB);
        masterDaemon.runJob();
    }
}