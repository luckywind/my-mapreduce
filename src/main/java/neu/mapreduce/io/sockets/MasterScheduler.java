package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.autodiscovery.NodeRegistration;
import neu.mapreduce.core.factory.JobConfFactory;
import neu.mapreduce.core.shuffle.ShuffleRun;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/18/15.
 */
public class MasterScheduler {

    public static final int MASTER_FT_PORT_MAPPER = 6060;
    public static final int MASTER_FT_PORT_REDUCER = 6061;
    
    private static final Logger LOGGER = Logger.getLogger(MasterScheduler.class.getName());
    public static int keyMappingFileCounter = 0;
    public static final String KEY_MAPPING_FILE = Constants.HOME+Constants.USER+Constants.MR_RUN_FOLDER+Constants.MASTER_FOLER +"/keyMapping";
    private  HashMap<String, Integer> slaveToSlavePorts;


    public String masterIP;
    private ArrayList<String> fileSplits;
    private String inputJar;
    private HashMap<String, Socket> slaves;
    private Job job;
    private JobConf jobConf;
    private String jobConfClassName;
    //freeSlaveID is a string in the format: ip:port. eg: 192.168.1.1:8087
    private String freeSlaveID;
    private String curSplit;
    private HashMap<String, ArrayList<String>> keyFileMapping;


    /**
     * Public constructor
     * @param fileSplits List of split size
     * @param inputJar File path of client's input JAR
     * @param slaves List of all running slaves
     * @param jobConfClassName Class name of Job configuration
     * @param slaveToSlavePorts List of all slave to slave ports
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     */
    public MasterScheduler(ArrayList<String> fileSplits, String inputJar, HashMap<String, Socket> slaves, String jobConfClassName, HashMap<String, Integer> slaveToSlavePorts) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException, SocketException {
        this.fileSplits = fileSplits;
        this.inputJar = inputJar;
        this.slaves = slaves;
        this.job = new Job();
        this.keyFileMapping = new HashMap<>();
        this.jobConfClassName = jobConfClassName;
        JobConfFactory jobConfFactory = new JobConfFactory(this.inputJar, jobConfClassName);
        this.jobConf = jobConfFactory.getSingletonObject();
        this.slaveToSlavePorts = slaveToSlavePorts;
        this.masterIP = NodeRegistration.getIPsInString();
    }

    /**
     * Schedules and runs the Map job on free slaves.
     * @throws IOException
     */
    public void scheduleMap() throws IOException {
        LOGGER.log(Level.INFO, "Number of slaves: " + this.slaves.size());
        boolean isCompleted = false;

        while (!isCompleted) {
            //Do the mapper phase
            if (!(fileSplits.isEmpty())) {
                //Allocate task
                allocateMapTask();
            } else {
                //Check for mapper completion
                isCompleted = checkForCompletion(job.getMapperSlaveID());
            }
        }

        LOGGER.log(Level.INFO, "Mapper complete.. Requesting for key-mapping files");
        //Start reducer here
        ArrayList<HashMap<String, ArrayList<String>>> listSmallerHashMaps = receiveKeyMappingFiles();
        LOGGER.log(Level.INFO, listSmallerHashMaps.size()+" equals "+this.jobConf.getNumReducers()+":: # of hashmaps vs # of reducers");
        LOGGER.log(Level.INFO, "Starting reducer/s: # of reducers:"+this.jobConf.getNumReducers());
        scheduleReducer(listSmallerHashMaps);
        System.out.println("Success!");
    }

    /**
     *Allocates the map job to a free slave  * 
     * @throws IOException
     */
    private void allocateMapTask() throws IOException {
        //Find a free slave
        if (findFreeSlave()) {
            LOGGER.log(Level.INFO, "Allocation map task");

            this.job.getMapperSlaveID().add(this.freeSlaveID);
            //Allocate job to slave
            this.curSplit = fileSplits.get(0);
            fileSplits.remove(0);
            initiateMapSlaveJob();
        }
    }


    /**
     * Checks if all allocated jobs are complete.
     * * @param listSlaveID
     * @return true if all jobs are complete, false otherwise. 
     * @throws IOException
     */
    private boolean checkForCompletion(ArrayList<String> listSlaveID) throws IOException {
        String status;
        boolean isCompleted = true;
        for (String slaveID : new HashSet<>(listSlaveID)) {
            Socket slaveSocket = slaves.get(slaveID);
            PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(slaveSocket.getInputStream()));
            out.println(Message.STATUS);
            status = in.readLine();
            if(status.equals("Complete")){
                LOGGER.log(Level.INFO, "Frome checkForCompletion(): Master commands slave to change status from complete to idle");
                out.println(Message.CHANGE_STATUS);
            } else if (status.equals("Busy")) {
                isCompleted = false;
            }
        }
        return isCompleted;
    }

    /**
     * Finds a free slave* 
     * @return set freeSlaveId if free slave found and returns true, else return false.
     * @throws IOException
     */
    private boolean findFreeSlave() throws IOException {
        boolean slaveFound = false;
        for (String slaveID : slaves.keySet()) {
            Socket slaveSocket = slaves.get(slaveID);
            PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(slaveSocket.getInputStream()));
            out.println(Message.STATUS);
            String slaveStatus = in.readLine();
            if (slaveStatus.equals("Idle")) {
                slaveFound = true;
            }else if(slaveStatus.equals("Complete")){
                LOGGER.log(Level.INFO, "From findFreeSlave(): Master commands slave to change status from complete to idle");
                out.println(Message.CHANGE_STATUS);
            }
            if(slaveFound){
                this.freeSlaveID = slaveID;
                return true;
            }
        }
        return false;
    }

    /**
     * Sends data to mapper slave and sends a message to start the job * 
     * @throws IOException
     */
    private void initiateMapSlaveJob() throws IOException {
        Socket slaveSocket = slaves.get(this.freeSlaveID);
        PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(slaveSocket.getInputStream()));
        out.println(Message.RUN_JOB +":"+this.jobConfClassName + ":" + this.masterIP);
        in.readLine();
        //Send split
        sendFile(this.curSplit, getIp(this.freeSlaveID), MASTER_FT_PORT_MAPPER);
        //Send jar;
        sendFile(this.inputJar, getIp(this.freeSlaveID), MASTER_FT_PORT_MAPPER);
    }

    /**
     * Gets the key mapping files from the shuffler.* 
     * @throws IOException
     */
    private  ArrayList<HashMap<String, ArrayList<String>>> receiveKeyMappingFiles() throws IOException {

        for (String slaveID : this.job.getMapperSlaveID()) {
            ServerSocket listener = new ServerSocket(MASTER_FT_PORT_MAPPER);
            requestKeyMappingFile(slaveID);
            IOCommons.receiveFile(listener, KEY_MAPPING_FILE+(keyMappingFileCounter++));
            listener.close();
            updateKeyMappingHashMap(slaveID);
        }
        ArrayList<HashMap<String, ArrayList<String>>> listSmallerHashmaps = splitKeyMapping(this.keyFileMapping, this.jobConf.getNumReducers());
        return listSmallerHashmaps;
    }

    /**
     * Requests slave to send the keyMapping file* 
     * @param slaveID
     * @throws IOException
     */
    private void requestKeyMappingFile(String slaveID) throws IOException {
        Socket slaveSocket = slaves.get(slaveID);
        PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
        out.println(Message.SEND_KEY_MAPPING_FILE_MESSAGE);
    }

    /**
     * Updates the local keyMap with values from the shuffle key map files sent by mappers* 
     * @param slaveID
     * @throws IOException
     */
    private void updateKeyMappingHashMap(String slaveID) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(KEY_MAPPING_FILE+(keyMappingFileCounter-1)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            // key \t filelocation
            String[] splitLine = line.split(ShuffleRun.OUTPUT_SHUFFLE_FILE_VALUE_SEPARATOR);
            if (!keyFileMapping.containsKey(splitLine[0])) {
                keyFileMapping.put(splitLine[0], new ArrayList<String>());
            }
            // updates the array list
            keyFileMapping.get(splitLine[0]).add(slaveID + ":" + splitLine[1]);
        }


    }

    /**
     * Splits the keys in keyMap to be sent to different reducers. Will return the same hashmap if numReducers = 1* 
     * @param keyFileMapping
     * @param numReducers
     * @return list of smaller hash maps with size = numReducers
     */
    private ArrayList<HashMap<String, ArrayList<String>>> splitKeyMapping(HashMap<String, ArrayList<String>> keyFileMapping, int numReducers) {

        ArrayList<HashMap<String, ArrayList<String>>> listKeyFileMapping = new ArrayList<HashMap<String, ArrayList<String>>>();
        for (int i = 0; i < numReducers; i++) {
            listKeyFileMapping.add(new HashMap<String, ArrayList<String>>());
        }
        int counter = 0;
        for (String key : keyFileMapping.keySet()) {
            listKeyFileMapping.get(counter % numReducers).put(key, keyFileMapping.get(key));
            counter++;
        }
        LOGGER.log(Level.INFO, "Created smaller key-file mapping for each reducer");

        return listKeyFileMapping;
    }

    /**
     * *
     * @param listSmallerHashmaps
     * @throws IOException
     */
    private void scheduleReducer(ArrayList<HashMap<String, ArrayList<String>>> listSmallerHashmaps) throws IOException {
        boolean isCompleted = false;

        while (!isCompleted) {
            //Do the reduce phase
            if (!(listSmallerHashmaps.isEmpty())) {
                //Allocate task
                allocateReduceTask(listSmallerHashmaps);
            } else {
                //Check for reduce completion
                isCompleted = checkForCompletion(job.getReducerSlaveID());
            }
        }
    }

    /**
     * Allocates tasks to reducers. * 
     * @param keyShuffleFileInfoMapping
     * @throws IOException
     */
    private void allocateReduceTask(ArrayList<HashMap<String, ArrayList<String>>> keyShuffleFileInfoMapping) throws IOException {
        
        //update the job.reducers
        
        //find free slave
        if (findFreeSlave()) {
            LOGGER.log(Level.INFO, "Allocating one reduce task");

            this.job.getReducerSlaveID().add(this.freeSlaveID);
            //Allocate job to slave
            HashMap<String, ArrayList<String>> smallHM = keyShuffleFileInfoMapping.get(0);
            keyShuffleFileInfoMapping.remove(0);

            initiateReducerSlaveJob(smallHM);
        }
    }

    /**
     * Initiates reducer job
     * @param keyShuffleFileInfoMapping
     * @throws IOException
     */
    private void initiateReducerSlaveJob(HashMap<String, ArrayList<String>> keyShuffleFileInfoMapping) throws IOException {
        //send the client jar
        Socket messageSocket = slaves.get(this.freeSlaveID);
        LOGGER.log(Level.INFO, "Allocating slave "+this.freeSlaveID+" as reducer. # of keys to this reducer: " + keyShuffleFileInfoMapping.size());

        PrintWriter reducerOut = new PrintWriter(messageSocket.getOutputStream(), true);
        BufferedReader reducerIn = new BufferedReader(
                new InputStreamReader(messageSocket.getInputStream()));
        reducerOut.println(Message.INITIAL_REDUCE);
        while(!reducerIn.readLine().equals(Message.READY_TO_RECEIVE_JAR)){

        }
        LOGGER.log(Level.INFO, "Reducer is ready to receive jar");

        sendFile(this.inputJar, getIp(this.freeSlaveID), MASTER_FT_PORT_REDUCER);
        while(!reducerIn.readLine().equals(Message.JAR_RECEIVED)){

        }
        LOGGER.log(Level.INFO, "Jar sent to reducer");
        for (String key : keyShuffleFileInfoMapping.keySet()) {
            //Slave should open a socket and wait for files. It should create a dir for each key
            reducerOut.println(Message.INITIAL_GET_KEY_SHUFFLE);
            while(!reducerIn.readLine().equals(Message.JAR_RECEIVED)){

            }
            for (String fileLoc : keyShuffleFileInfoMapping.get(key)) {
                String destId = getDestId(fileLoc);
                Socket shuffleSocket = slaves.get(destId);
                PrintWriter shuffleOut = new PrintWriter(shuffleSocket.getOutputStream(), true);
                BufferedReader shuffleIn = new BufferedReader(
                        new InputStreamReader(shuffleSocket.getInputStream()));
                //SENDSHUFFLEFILE:rmt_ip:rmt_port_msg:local_file_loc:rmt_port_FT
                shuffleOut.println(Message.SEND_SHUFFLE_FILE + ":" + this.freeSlaveID + ":" + fileLoc.split(":")[2]+":"+slaveToSlavePorts.get(this.freeSlaveID));
                while(!shuffleIn.readLine().equals(Message.FILE_SENT)){

                }
            }
        }
        reducerOut.println(Message.RUN_REDUCE + ":" + jobConfClassName);
    }

    /**
     * Gets destination id
     * @param fileLoc Location of the file
     * @return
     */
    private String getDestId(String fileLoc) {
        String[] fileSplit = fileLoc.split(":");
        return (fileSplit[0] + ":" + fileSplit[1]);
    }

    /**
     * SEnds file to given IP address and port number
     * @param fileName Location of file to be send
     * @param ip IP address of destination
     * @param port Port number of destination
     * @throws IOException
     */
    private void sendFile(String fileName, String ip, int port) throws IOException {
        Socket fileSender = new Socket(ip, port);
        OutputStream os = fileSender.getOutputStream();
        InputStream is = new FileInputStream(fileName);
        IOUtils.copy(is, os);
        os.close();
        is.close();
        fileSender.close();
    }

    /**
     * Extract IP address from slave id
     * @param freeSlaveID Slave id
     * @return
     */
    private String getIp(String freeSlaveID) {
        String[] slaveIdSplit = freeSlaveID.split(":");
        return slaveIdSplit[0];
    }

}

