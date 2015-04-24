package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.core.factory.JobConfFactory;
import neu.mapreduce.core.shuffle.ShuffleRun;
import neu.mapreduce.autodiscovery.NodeDAO;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/13/15.
 */
public class SlaveListener {
    private static final Logger LOGGER = Logger.getLogger(SlaveListener.class.getName());

    public static String REDUCER_FOLDER_PATH;

    public static String REDUCER_CLIENT_JAR_PATH;
    public static String MAPPER_FOLDER_PATH = Constants.HOME + Constants.USER + Constants.MR_RUN_FOLDER + Constants.MAP_FOLDER;
    public static int shuffleDirCounter;
    public static ConnectionTypes status;

    private static int numMapTasks=0;

    public static String INPUT_CHUNK;
    public static String MAPPER_CLIENT_JAR_PATH;
    public static String MAP_OUTPUT_FILE_PATH;
    public static String SHUFFLE_OUTPUT_FOLDER;
    
    //This port is used for message transfer with master
    public int port;
    //This port is used to communicate with other slaves
    private int slaveToSlavePort;

    public static final int TWO = 2;
    public static final int ONE = 1;
    public static final int THREE = 3;
    public static final int FOUR = 4;
    public static final String MSG_SPLITTER = ":";
    private String masterIp;

    /**
     * Public constructor
     * @param port
     * @param slaveToSlavePort
     */
    public SlaveListener(int port, int slaveToSlavePort) {
        //Setup for communication
        this.port = port;
        this.slaveToSlavePort = slaveToSlavePort;
        SlaveListener.status = ConnectionTypes.IDLE;

        // Initializes the names of folder and files for map-reduce tasks
        REDUCER_FOLDER_PATH = Constants.HOME + Constants.USER + Constants.MR_RUN_FOLDER + Constants.REDUCE_FOLDER+this.port;
        REDUCER_CLIENT_JAR_PATH = REDUCER_FOLDER_PATH + "/red-client-jar-with-dependencies.jar";
        SlaveListener.MAPPER_FOLDER_PATH = SlaveListener.MAPPER_FOLDER_PATH + this.port;
        INPUT_CHUNK = MAPPER_FOLDER_PATH + "/input_chunk.txt";
        MAPPER_CLIENT_JAR_PATH = MAPPER_FOLDER_PATH + "/map-client-jar-with-dependencies.jar";
        MAP_OUTPUT_FILE_PATH = MAPPER_FOLDER_PATH + "/map_op_shuffle_ip.txt";
        SHUFFLE_OUTPUT_FOLDER = MAPPER_FOLDER_PATH + "/shuffle";

        //Starting a thread for slave to slave communication
        new Thread(new SlaveToSlaveFileTransferThread(this.slaveToSlavePort)).start();
    }

    /**
     * This is startup method for the slave. It starts the connection 
     * and waits for communication from the master.
     * 
     * * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void startListening() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        System.out.println("Listening for messages on port: "+this.port);
        System.out.println("Listening for File transfer on port: "+this.slaveToSlavePort);
        ServerSocket listener = new ServerSocket(port);
        while (true) {
            Socket socket = listener.accept();
            String inputMessage;
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            while (true) {
                while ((inputMessage = in.readLine()) == null) {
                    //idle slave
                }
                //Send idle/busy response. Switch case to handle all possible requests.
                handleRequest(inputMessage, socket);
            }
        }
    }


    /**
     * Handles messages sent by the master 
     * (Switch cases is not available on String class in Java 1.6)
     *  
     * @param inputMessage
     * @param socket
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void handleRequest(String inputMessage, Socket socket) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        if (inputMessage.equals(Message.STATUS)) {
            statusRequestHandler(socket);
        } else if (inputMessage.equals(Message.CHANGE_STATUS)) {
            changeToIdle();
        } else if (inputMessage.startsWith(Message.RUN_JOB)) {
            this.masterIp = inputMessage.split(":")[2];
            preProcessAndRunMap(socket, getJobConfigClassname(inputMessage));
        } else if (inputMessage.equals(Message.SEND_KEY_MAPPING_FILE_MESSAGE)) {
            sendKeyMappingFile();
        } else if (inputMessage.equals(Message.INITIAL_REDUCE)) {
            preProcessReduce(socket);
        } else if (inputMessage.equals(Message.INITIAL_GET_KEY_SHUFFLE)) {
            createShuffleDir(socket);
        } else if (inputMessage.startsWith(Message.SEND_SHUFFLE_FILE)) {
            sendShuffleFiles(inputMessage, socket);
        } else if (inputMessage.startsWith(Message.RUN_REDUCE)) {
            runReduce(getJobConfigClassname(inputMessage));
        }
    }

    /**
     * Responds with the status of the machine as either Idle, Busy or Job Complete  
     * @param socket Socket information
     * @throws IOException
     */
    private void statusRequestHandler(Socket socket) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        if (SlaveListener.status == ConnectionTypes.IDLE) {
            out.println(Message.IDLE);
        } else if (SlaveListener.status == ConnectionTypes.JOB_COMPLETE) {
            //handle what happens after job is done.
            out.println(Message.COMPLETE);
        } else {
            out.println(Message.BUSY);
        }
    }

    /**
     * Changes the status to Idle 
     */
    private void changeToIdle() {
        LOGGER.log(Level.INFO, "Master tells slave to be idle");
        SlaveListener.status = ConnectionTypes.IDLE;
    }

    /**
     * Receives the files from mapper and runs the mapper 
     * * @param socket Spcket information
     * @param jobConfClassName Job config class name
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void preProcessAndRunMap(Socket socket, String jobConfClassName) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        new File(MAPPER_FOLDER_PATH).mkdirs();
        ServerSocket listener = new ServerSocket(MasterScheduler.MASTER_FT_PORT_MAPPER);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println(Message.READY_FOR_JOB);
        //Receives input chunk and jar from the master
        IOCommons.receiveFile(listener, INPUT_CHUNK);
        IOCommons.receiveFile(listener, MAPPER_CLIENT_JAR_PATH);
        listener.close();
        runMap(jobConfClassName);
    }

    /**
     * Initiates a thread to run the map job
     * @param jobConfClassName Job configuration class name
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    private void runMap(String jobConfClassName) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        SlaveListener.status = ConnectionTypes.BUSY;
        new Thread(new SlaveMapRunThread(
                INPUT_CHUNK,
                MAP_OUTPUT_FILE_PATH,
                SHUFFLE_OUTPUT_FOLDER + numMapTasks++,
                MAPPER_CLIENT_JAR_PATH,
                getJobConf(MAPPER_CLIENT_JAR_PATH, jobConfClassName)))
                .start();
    }

    /**
     * Send the key mapping file from the output of the map phase to master
     * * 
     * @throws IOException
     */
    private void sendKeyMappingFile() throws IOException {
        IOCommons.sendFile(
                SHUFFLE_OUTPUT_FOLDER + (--numMapTasks) + "/" + ShuffleRun.KEY_FILENAME_MAPPING,
                this.masterIp,
                MasterScheduler.MASTER_FT_PORT_MAPPER);
    }

    /**
     * Send shuffle files to reduce slave
     * * 
     * @param inputMessage
     * @param masterSocket
     * @throws IOException
     */
    private void sendShuffleFiles(String inputMessage, Socket masterSocket) throws IOException {
        LOGGER.log(Level.INFO, inputMessage);
        //0:SEND_SHUFFLE_FILE 1: dest_ip, 2: dest_port; 3:local_file_loc, 4:rmt_slave_to_slave_port
        String[] inputMsgSplit = inputMessage.split(MSG_SPLITTER);
        IOCommons.sendFile(inputMsgSplit[THREE], inputMsgSplit[ONE], Integer.parseInt(inputMsgSplit[FOUR]));
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.FILE_SENT);
    }

    /**
     * When slave is a reducer, it creates a directory for each key to receive the shuffle files.
     * All the shuffle file for one key are saved in one directory 
     * * createShuffleDir
     */
    private void createShuffleDir(Socket masterSocket) throws IOException {
        new File(REDUCER_FOLDER_PATH + "/" + SlaveListener.shuffleDirCounter).mkdir();
        SlaveListener.shuffleDirCounter++;
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.JAR_RECEIVED);
    }

    /**
     * Once selected as a reducer, slave needs receive the client jar 
     * @param masterSocket
     * @throws IOException
     */
    private void preProcessReduce(Socket masterSocket) throws IOException {
        LOGGER.log(Level.INFO, "Selected as reducer..");
        ServerSocket listener = new ServerSocket(MasterScheduler.MASTER_FT_PORT_REDUCER);
        //Before the slave receives the jar, it prepares the directory for the entire reduce task 
        //and initializes a socket for file transfer
        new File(REDUCER_FOLDER_PATH).mkdirs();
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.READY_TO_RECEIVE_JAR);
        IOCommons.receiveFile(listener, REDUCER_CLIENT_JAR_PATH);
        out.println(Message.JAR_RECEIVED);
        listener.close();
    }

    /**
     * Initiates a thread to run the reduce job 
     * * 
     * @param jobConfigClassname
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     */
    private void runReduce(String jobConfigClassname) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException {
        SlaveListener.status = ConnectionTypes.BUSY;
        new Thread(new SlaveReduceRunThread(REDUCER_CLIENT_JAR_PATH, getJobConf(REDUCER_CLIENT_JAR_PATH, jobConfigClassname))).start();
    }

    /**
     * Gets the Job config class name from an input message from the master 
     * @param inputMessage
     * @return
     */
    private String getJobConfigClassname(String inputMessage) {
        String[] ipMsgSplit = inputMessage.split(":");
        if(ipMsgSplit.length == TWO){
            return ipMsgSplit[ONE];
        }
        LOGGER.log(Level.SEVERE, "Set jobconfig file name in the msg from Master to Mapper Slave");
        return null;
    }

    /**
     * Given the class name of JobConf class and the jar file, returns an object of JobConf
     * * 
     * @param clientJarPath
     * @param jobConfClassName Name of Job config class
     * @return
     * @throws java.net.MalformedURLException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    private JobConf getJobConf(String clientJarPath, String jobConfClassName) throws java.net.MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        JobConfFactory jobConfFactory = new JobConfFactory(clientJarPath, jobConfClassName);
        return jobConfFactory.getSingletonObject();
    }

    /**
     * Start slave
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //0:8087
        //SlaveListener listener = new SlaveListener(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        NodeDAO nodeDAO = new NodeDAO();
        nodeDAO.registerThisNode();

        SlaveListener listener = new SlaveListener(nodeDAO.getFileTransferPort(),nodeDAO.getMessagingServicePort());
        listener.startListening();
    }
}
