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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/13/15.
 */
public class SlaveListener {
    private static final Logger LOGGER = Logger.getLogger(SlaveListener.class.getName());
    private static int numMapTasks=0;
    public static final int LISTENER_PORT = 6060;
    public static String REDUCER_FOLDER_PATH;// = Constants.HOME + Constants.USER + Constants.MR_RUN_FOLDER + Constants.REDUCE_FOLDER;
    public static String REDUCER_CLIENT_JAR_PATH; // = REDUCER_FOLDER_PATH + "/red-client-jar-with-dependencies.jar";
    public static String MAPPER_FOLDER_PATH = Constants.HOME + Constants.USER + Constants.MR_RUN_FOLDER + Constants.MAP_FOLDER;
    public static final int REDUCER_LISTENER_PORT = 9061;
    public static int shuffleDirCounter;
    private int slaveToSlavePort;
    public int port;
    public static ConnectionTypes status;
    public static String INPUT_CHUNK; // = MAPPER_FOLDER_PATH + "/input_chunk.txt";
    public static String MAPPER_CLIENT_JAR_PATH; // = MAPPER_FOLDER_PATH + "/map-client-jar-with-dependencies.jar";
    public static String MAP_OUTPUT_FILE_PATH; // = MAPPER_FOLDER_PATH + "/map_op_shuffle_ip.txt";
    public static String SHUFFLE_OUTPUT_FOLDER; // = MAPPER_FOLDER_PATH + "/shuffle";


    public SlaveListener(int port, int slaveToSlavePort) {
        this.port = port;
        this.slaveToSlavePort = slaveToSlavePort;
        SlaveListener.status = ConnectionTypes.IDLE;

        REDUCER_FOLDER_PATH = Constants.HOME + Constants.USER + Constants.MR_RUN_FOLDER + Constants.REDUCE_FOLDER+this.port;
        REDUCER_CLIENT_JAR_PATH = REDUCER_FOLDER_PATH + "/red-client-jar-with-dependencies.jar";
        
        new File(REDUCER_FOLDER_PATH).mkdirs();
        

        SlaveListener.MAPPER_FOLDER_PATH = SlaveListener.MAPPER_FOLDER_PATH + this.port;
        new File(MAPPER_FOLDER_PATH).mkdirs();
        INPUT_CHUNK = MAPPER_FOLDER_PATH + "/input_chunk.txt";
        MAPPER_CLIENT_JAR_PATH = MAPPER_FOLDER_PATH + "/map-client-jar-with-dependencies.jar";
        MAP_OUTPUT_FILE_PATH = MAPPER_FOLDER_PATH + "/map_op_shuffle_ip.txt";
        SHUFFLE_OUTPUT_FOLDER = MAPPER_FOLDER_PATH + "/shuffle";

        new Thread(new SlaveToSlaveFileTransferThread(this.slaveToSlavePort)).start();

     
    }

    public void startListening() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        System.out.println("Listening...On port: "+this.port+" & "+this.slaveToSlavePort);
        ServerSocket listener = new ServerSocket(port);
        while (true) {
            Socket socket = listener.accept();
            String inputMessage;
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            while (true) {
                while ((inputMessage = in.readLine()) == null) {
                    //idle the machine
                }
                //Send idle/busy response. Switch case to handle all possible requests.
                handleRequest(inputMessage, socket);
            }
        }
    }


    private void handleRequest(String inputMessage, Socket socket) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        if (inputMessage.equals(Message.STATUS)) {
            statusRequestHandler(socket);
        } else if (inputMessage.equals(Message.CHANGE_STATUS)) {
            LOGGER.log(Level.INFO, "Master tells slave to be idle");
            SlaveListener.status = ConnectionTypes.IDLE;
        } else if (inputMessage.startsWith(Message.RUN_JOB)) {
            runMapJobRequestHandler(socket, getJobConfigClassname(inputMessage));
        } else if (inputMessage.equals(Message.SEND_KEY_MAPPING_FILE_MESSAGE)) {
            sendKeyMappingFile();
        } else if (inputMessage.equals(Message.INITIAL_REDUCE)) {
            initialReduce(socket);
        } else if (inputMessage.equals(Message.INITIAL_GET_KEY_SHUFFLE)) {
            createShuffleDir();
        } else if (inputMessage.startsWith(Message.SEND_SHUFFLE_FILE)) {
            sendShuffleFiles(inputMessage, socket);
        } else if (inputMessage.startsWith(Message.RUN_REDUCE)) {
            runReduce(getJobConfigClassname(inputMessage));
        }
    }

    private String getJobConfigClassname(String inputMessage) {

        String[] ipMsgSplit = inputMessage.split(":");
        if(ipMsgSplit.length == 2){
            return ipMsgSplit[1];
        }
        LOGGER.log(Level.SEVERE, "Set jobconfig file name in the msg from Master to Mapper Slave");
        return null;
    }

    private void runReduce(String jobConfigClassname) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException {
        SlaveListener.status = ConnectionTypes.BUSY;
        new Thread(new SlaveReduceRunThread(REDUCER_CLIENT_JAR_PATH, getJobConf(REDUCER_CLIENT_JAR_PATH, jobConfigClassname))).start();
    }

    private void sendShuffleFiles(String inputMessage, Socket masterSocket) throws IOException {
        LOGGER.log(Level.INFO, inputMessage);
        //0:SEND_SHUFFLE_FILE 1: dest_ip, 2: dest_port; 3:local_file_loc, 4:rmt_slave_to_slave_port
        String[] inputMsgSplit = inputMessage.split(":");
        IOCommons.sendFile(inputMsgSplit[3], inputMsgSplit[1], Integer.parseInt(inputMsgSplit[4]));
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.FILE_SENT);
    }

    private void createShuffleDir() {
        new File(REDUCER_FOLDER_PATH + "/" + SlaveListener.shuffleDirCounter).mkdir();
        SlaveListener.shuffleDirCounter++;
    }

    private void initialReduce(Socket masterSocket) throws IOException {
        LOGGER.log(Level.INFO, "Selected as reducer..");
        ServerSocket listener = new ServerSocket(REDUCER_LISTENER_PORT);

        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.READY_TO_RECEIVE_JAR);

        receiveFile(listener, REDUCER_CLIENT_JAR_PATH);
        // PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.JAR_RECEIVED);
        // CLOSE AFTER ALL JAR AND SHUFFLE FILES ARE RECEIVED

        listener.close();
    }

    public void receiveFile(ServerSocket listener, String outputFileName) throws IOException {

        Socket sender = listener.accept();
        InputStream in = sender.getInputStream();
        FileOutputStream fos = new FileOutputStream(outputFileName);
        IOUtils.copy(in, fos);
        fos.close();
        in.close();
        sender.close();
    }

    private void sendKeyMappingFile() throws IOException {
        Socket sender = new Socket(MasterScheduler.masterIP, SlaveListener.LISTENER_PORT);

        FileInputStream inputStream = new FileInputStream(SHUFFLE_OUTPUT_FOLDER + (--numMapTasks) + "/" + Shuffle.KEY_FILENAME_MAPPING);
        OutputStream outputStream = sender.getOutputStream();
        IOUtils.copy(inputStream, outputStream);
        inputStream.close();
        outputStream.close();
        sender.close();

    }

    private void runMapJobRequestHandler(Socket socket, String jobConfClassName) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        FileOutputStream fos = null, fos2 = null;
        ServerSocket listener = new ServerSocket(LISTENER_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println(Message.READY_FOR_JOB);

        Socket sender = listener.accept();

        try {
            InputStream in = sender.getInputStream();
            fos = new FileOutputStream(INPUT_CHUNK);
            IOUtils.copy(in, fos);
            fos.close();
            in.close();
            sender.close();
            Socket sender2 = listener.accept();
            in = sender2.getInputStream();
            fos = new FileOutputStream(MAPPER_CLIENT_JAR_PATH);
            IOUtils.copy(in, fos);
            fos.close();
            in.close();
            sender2.close();
            listener.close();
          //  String jobConfClassName = "mapperImpl.AirlineJobConf";
            JobConf jobConf = getJobConf(MAPPER_CLIENT_JAR_PATH, jobConfClassName);

            SlaveListener.status = ConnectionTypes.BUSY;
            //Run the job in new thread here

            //String inputFilePath = "/home/srikar/Desktop/input/purchases.txt";
            //String mapperClassname = "mapperImpl.AirlineMapper";

            //TODO: NEED TO GENERATE ON FLY
            //String keyClassName = "impl.StringWritable";
            //String valueClassname = "impl.FloatWritable";
//            boolean isCombinerSet = false;

            new Thread(new SlaveMapRunThread(
                    INPUT_CHUNK,
                    MAP_OUTPUT_FILE_PATH,
                    SHUFFLE_OUTPUT_FOLDER + numMapTasks++,
                    MAPPER_CLIENT_JAR_PATH,
                    jobConf))
                    .start();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private JobConf getJobConf(String clientJarPath, String jobConfClassName) throws java.net.MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        JobConfFactory jobConfFactory = new JobConfFactory(clientJarPath, jobConfClassName);
        return jobConfFactory.getSingletonObject();
    }


    private void statusRequestHandler(Socket socket) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        if (SlaveListener.status == ConnectionTypes.IDLE) {
            out.println("Idle");
        } else if (SlaveListener.status == ConnectionTypes.JOB_COMPLETE) {
            //handle what happens after job is done.
            out.println("Complete");
        } else {
            out.println("Busy");
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //0:8087
        //SlaveListener listener = new SlaveListener(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        SlaveListener listener = new SlaveListener(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        listener.startListening();
    }
}
