package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.core.factory.JobConfFactory;
import neu.mapreduce.core.shuffle.Shuffle;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/18/15.
 */
public class MasterScheduler {

    private static final Logger LOGGER = Logger.getLogger(MasterScheduler.class.getName());
   // private static final Integer NUM_REDUCERS = 1;
   public static final String masterIP = "localhost";
    private final HashMap<String, Integer> slaveToSlavePorts;


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
    public static int keyMappingFileCounter = 0;
    public static final String KEY_MAPPING_FILE = Constants.HOME+Constants.USER+Constants.MR_RUN_FOLDER+Constants.MASTER_FOLER +"/keyMapping";

    public MasterScheduler(ArrayList<String> fileSplits, String inputJar, HashMap<String, Socket> slaves, String jobConfClassName, HashMap<String, Integer> slaveToSlavePorts) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException {
        this.fileSplits = fileSplits;
        this.inputJar = inputJar;
        this.slaves = slaves;
        this.job = new Job();
        this.keyFileMapping = new HashMap<>();
        this.jobConfClassName = jobConfClassName;
        JobConfFactory jobConfFactory = new JobConfFactory(this.inputJar, jobConfClassName);
        this.jobConf = jobConfFactory.getSingletonObject();
        this.slaveToSlavePorts = slaveToSlavePorts;
    }

    public void schedule() throws IOException {
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
        receiveKeyMappingFiles();
        System.out.println("Success!");
    }

    private void receiveKeyMappingFiles() throws IOException {

        for (String slaveID : this.job.getMapperSlaveID()) {
            Socket slaveSocket = slaves.get(slaveID);
            PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
            out.println(Message.SEND_KEY_MAPPING_FILE_MESSAGE);
            ServerSocket listener = new ServerSocket(SlaveListener.LISTENER_PORT);
            Socket listenerSocket = listener.accept();

            InputStream fileInputStream = listenerSocket.getInputStream();
            OutputStream outputStream = new FileOutputStream(KEY_MAPPING_FILE+(keyMappingFileCounter++));
            IOUtils.copy(fileInputStream, outputStream);

            // method to update hash map for key, list of files
            updateKeyMappingHashMap(slaveID);
            listenerSocket.close();
            listener.close();

        }
        ArrayList<HashMap<String, ArrayList<String>>> listSmallerHashmaps = splitKeyMapping(this.keyFileMapping, this.jobConf.getNumReducers());
        LOGGER.log(Level.INFO, listSmallerHashmaps.size()+" equals "+this.jobConf.getNumReducers()+":: # of hashmaps vs # of reducers");
        LOGGER.log(Level.INFO, "Starting reducer/s: # of reducers:"+this.jobConf.getNumReducers());
        scheduleReducer(listSmallerHashmaps);
    }

    private void updateKeyMappingHashMap(String slaveID) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(KEY_MAPPING_FILE+(keyMappingFileCounter-1)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            // key \t filelocation
            String[] splitLine = line.split(Shuffle.OUTPUT_SHUFFLE_FILE_VALUE_SEPARATOR);
            if (!keyFileMapping.containsKey(splitLine[0])) {
                keyFileMapping.put(splitLine[0], new ArrayList<String>());
            }
            // updates the array list
            keyFileMapping.get(splitLine[0]).add(slaveID + ":" + splitLine[1]);
        }

        
    }

    private void scheduleReducer(ArrayList<HashMap<String, ArrayList<String>>> listSmallerHashmaps) throws IOException {
        boolean isCompleted = false;

        while (!isCompleted) {
            //Do the mapper phase
            if (!(listSmallerHashmaps.isEmpty())) {
                //Allocate task
                allocateReduceTask(listSmallerHashmaps);
            } else {
                //Check for mapper completion
                isCompleted = checkForCompletion(job.getReducerSlaveID());
            }
        }
    }

    private void allocateReduceTask(ArrayList<HashMap<String, ArrayList<String>>> keyShuffleFileInfoMapping) throws IOException {
        //find free slave
        //update the job.reducers
        //send the client jar
        if (findFreeSlave()) {
            LOGGER.log(Level.INFO, "Allocating one reduce task");

            this.job.getReducerSlaveID().add(this.freeSlaveID);
            //Allocate job to slave
            HashMap<String, ArrayList<String>> smallHM = keyShuffleFileInfoMapping.get(0);
            keyShuffleFileInfoMapping.remove(0);

            initiateReducerSlaveJob(smallHM);
        }
    }

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

        sendFile(this.inputJar, getIp(this.freeSlaveID), SlaveListener.REDUCER_LISTENER_PORT);
        while(!reducerIn.readLine().equals(Message.JAR_RECEIVED)){

        }
        LOGGER.log(Level.INFO, "Jar sent to reducer");
        for (String key : keyShuffleFileInfoMapping.keySet()) {
            //Slave should open a socket and wait for files. It should create a dir for each key
            reducerOut.println(Message.INITIAL_GET_KEY_SHUFFLE);
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
                LOGGER.log(Level.INFO, "File sent. File details: " + fileLoc);
            }
        }
        reducerOut.println(Message.RUN_REDUCE+":"+jobConfClassName);
    }

    private String getDestId(String fileLoc) {
        String[] fileSplit = fileLoc.split(":");
        return (fileSplit[0] + ":" + fileSplit[1]);
    }


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
                //Handle what to do with successful mappers and the data file locations.
                LOGGER.log(Level.INFO, "Frome checkForCompletion(): Master commands slave to change status from complete to idle");
                out.println(Message.CHANGE_STATUS);
            } else if (status.equals("Busy")) {
                isCompleted = false;
            } 
        }
        return isCompleted;
    }


    private void allocateMapTask() throws IOException {
        //Find a free slave
        if (findFreeSlave()) {
            LOGGER.log(Level.INFO, "Allocation map task");

            this.job.getMapperSlaveID().add(this.freeSlaveID);
            //Allocate job to slave
            this.curSplit = fileSplits.get(0);
            fileSplits.remove(0);
            initiateSlaveJob();
        }
    }

    private void initiateSlaveJob() throws IOException {
        Socket slaveSocket = slaves.get(this.freeSlaveID);
        PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(slaveSocket.getInputStream()));
        out.println(Message.RUN_JOB +":"+this.jobConfClassName);
        in.readLine();
        //while(!(in.readLine().equals("readyForJob"))) {
        //}
        //Send split
        sendFile(this.curSplit, getIp(this.freeSlaveID), 6060);
        //Send jar;
        sendFile(this.inputJar, getIp(this.freeSlaveID), 6060);
    }

    private void sendFile(String fileName, String ip, int port) throws IOException {
        Socket fileSender = new Socket(ip, port);
        OutputStream os = fileSender.getOutputStream();
        InputStream is = new FileInputStream(fileName);
        IOUtils.copy(is, os);
        os.close();
        is.close();
        fileSender.close();
    }

    private String getIp(String freeSlaveID) {
        String[] slaveIdSplit = freeSlaveID.split(":");
        return slaveIdSplit[0];
    }

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
               // slaveFound = true;
            }
            if(slaveFound){
                this.freeSlaveID = slaveID;
                return true;
            }
        }
        return false;
    }

}

