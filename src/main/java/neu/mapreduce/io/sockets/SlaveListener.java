package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.core.factory.JobConfFactory;
import neu.mapreduce.core.shuffle.Shuffle;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/13/15.
 */
public class SlaveListener {
    private static final Logger LOGGER = Logger.getLogger(SlaveListener.class.getName());

    public static final int LISTENER_PORT = 6060;
    public final static String REDUCER_FOLDER_PATH = "/home/" + Constants.USER + Constants.MR_RUN_FOLDER + Constants.REDUCE_FOLDER;
    public static final String MAPPER_FOLDER_PATH = "/home/" + Constants.USER + Constants.MR_RUN_FOLDER + Constants.MAP_FOLDER;
    public static final int REDUCER_LISTENER_PORT = 6061;
    public static int shuffleDirCounter;
    public int port;
    public static ConnectionTypes status;
    public static final String INPUT_CHUNK = MAPPER_FOLDER_PATH + "/input_chunk.txt";
    public static final String CLIENT_JAR_PATH = MAPPER_FOLDER_PATH + "/client-jar-with-dependencies.jar";
    public static final String MAP_OUTPUT_FILE_PATH = MAPPER_FOLDER_PATH + "/map_op_shuffle_ip.txt";
    public static final String SHUFFLE_OUTPUT_FOLDER = MAPPER_FOLDER_PATH + "/shuffle";


    public SlaveListener(int port) {
        this.port = port;
        SlaveListener.status = ConnectionTypes.IDLE;
        new File(REDUCER_FOLDER_PATH).mkdirs();
        new File(MAPPER_FOLDER_PATH).mkdirs();
        new Thread(new SlaveToSlaveFileTransferThread()).start();
    }

    public void startListening() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        System.out.println("Listening...");
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
        } else if (inputMessage.startsWith(Message.RUN_JOB)) {
            runJobRequestHandler(socket, getJobConfigClassname(inputMessage));
        } else if (inputMessage.equals(Message.CHANGE_STATUS)) {
            SlaveListener.status = ConnectionTypes.IDLE;
        } else if (inputMessage.equals(Message.SEND_KEY_MAPPING_FILE_MESSAGE)) {
            sendKeyMappingFile();
        } else if (inputMessage.equals(Message.INITIAL_REDUCE)) {
            initialReduce(socket);
        } else if (inputMessage.equals(Message.INITIAL_GET_KEY_SHUFFLE)) {
            createShuffleDir();
        } else if (inputMessage.startsWith(Message.SEND_SHUFFLE_FILE)) {
            sendShuffleFiles(inputMessage, socket);
        } else if (inputMessage.equals(Message.RUN_REDUCE)) {
            runReduce();
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

    private void runReduce() {
        new Thread(new SlaveReduceRunThread()).start();

    }

    private void sendShuffleFiles(String inputMessage, Socket masterSocket) throws IOException {
        LOGGER.log(Level.INFO, inputMessage);
        //0:SEND_SHUFFLE_FILE 1: dest_ip, 2: dest_port; 3:local_file_loc
        String[] inputMsgSplit = inputMessage.split(":");
        IOCommons.sendFile(inputMsgSplit[3], inputMsgSplit[1], SlaveToSlaveFileTransferThread.SLAVE_TO_SLAVE_PORT);
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.FILE_SENT);
    }

    private void createShuffleDir() {
        new File(REDUCER_FOLDER_PATH + "/" + SlaveListener.shuffleDirCounter).mkdir();
        SlaveListener.shuffleDirCounter++;
    }

    private void initialReduce(Socket masterSocket) throws IOException {

        ServerSocket listener = new ServerSocket(REDUCER_LISTENER_PORT);
        receiveFile(listener, REDUCER_FOLDER_PATH + "/client.jar");
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(Message.JAR_RECEIVED);
        //CLOSE AFTER ALL JAR AND SHUFFLE FILES ARE RECEIVED

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

        FileInputStream inputStream = new FileInputStream(SHUFFLE_OUTPUT_FOLDER + "/" + Shuffle.KEY_FILENAME_MAPPING);
        OutputStream outputStream = sender.getOutputStream();
        IOUtils.copy(inputStream, outputStream);
        inputStream.close();
        outputStream.close();
        sender.close();

    }

    private void runJobRequestHandler(Socket socket, String jobConfClassName) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

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
            fos = new FileOutputStream(CLIENT_JAR_PATH);
            IOUtils.copy(in, fos);
            fos.close();
            in.close();
            sender2.close();
            listener.close();
          //  String jobConfClassName = "mapperImpl.AirlineJobConf";
            JobConfFactory jobConfFactory = new JobConfFactory(CLIENT_JAR_PATH, jobConfClassName);
            JobConf jobConf = jobConfFactory.getSingletonObject();

            SlaveListener.status = ConnectionTypes.BUSY;
            //Run the job in new thread here

//            String inputFilePath = "/home/srikar/Desktop/input/purchases.txt";
//            String mapperClassname = "mapperImpl.AirlineMapper";

            //TODO: NEED TO GENERATE ON FLY
//            String keyClassName = "impl.StringWritable";
//            String valueClassname = "impl.FloatWritable";
//            boolean isCombinerSet = false;

            new Thread(new SlaveMapRunThread(
                    INPUT_CHUNK,
                    MAP_OUTPUT_FILE_PATH,
                    SHUFFLE_OUTPUT_FOLDER,
                    CLIENT_JAR_PATH,
                    jobConf))
                    .start();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
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

        SlaveListener listener = new SlaveListener(8087);
        listener.startListening();
    }
}
