package neu.mapreduce.io.sockets;

import neu.mapreduce.core.mapper.MapRun;
import neu.mapreduce.core.shuffle.Shuffle;
import org.apache.commons.io.IOUtils;

import java.io.*;
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
    public final static String REDUCER_FOLER_PATH = "/home/"+MasterScheduler.USER+"/Desktop/reduce";
    public static final int REDUCER_LISTENER_PORT = 6061;
    public static int shuffleDirCounter;
    int port;
    public static ConnectionTypes status;

    public SlaveListener(int port) {
        this.port = port;
        SlaveListener.status = ConnectionTypes.IDLE;
        new File(REDUCER_FOLER_PATH).mkdir();
        new Thread(new SlaveToSlaveFileTransferThread()).start();
    }

    public void startListening() throws IOException {
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


    private void handleRequest(String inputMessage, Socket socket) throws IOException {
        if (inputMessage.equals("status")) {
            statusRequestHandler(socket);
        } else if (inputMessage.equals("runJob")) {
            runJobRequestHandler(socket);
        } else if (inputMessage.equals("changeStatus")) {
            SlaveListener.status = ConnectionTypes.IDLE;
        } else if(inputMessage.equals(MasterScheduler.SEND_KEY_MAPPING_FILE_MESSAGE)) {
            sendKeyMappingFile();
        } else if(inputMessage.equals(MasterScheduler.INITIAL_REDUCE)) {
            initialReduce(socket);
        } else if(inputMessage.equals(MasterScheduler.INITIAL_GET_KEY_SHUFFLE)) {
            createShuffleDir();
        } else if(inputMessage.startsWith(MasterScheduler.SEND_SHUFFLE_FILE)){
            sendShuffleFiles(inputMessage, socket);
        } else if(inputMessage.equals(MasterScheduler.RUN_REDUCE)){
            runReduce();
        }
    }

    private void runReduce() {
        SlaveListener.status = ConnectionTypes.JOB_COMPLETE;

    }

    private void sendShuffleFiles(String inputMessage, Socket masterSocket) throws IOException {
        LOGGER.log(Level.INFO, inputMessage);
        //0:SEND_SHUFFLE_FILE 1: dest_ip, 2: dest_port; 3:local_file_loc
        String[] inputMsgSplit = inputMessage.split(":");
        IOCommons.sendFile(inputMsgSplit[3], inputMsgSplit[1], SlaveToSlaveFileTransferThread.SLAVE_TO_SLAVE_PORT);
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(MasterScheduler.FILE_SENT);
    }

    private void createShuffleDir() {
        new File(REDUCER_FOLER_PATH + "/" + SlaveListener.shuffleDirCounter).mkdir();
        SlaveListener.shuffleDirCounter++;
    }

    private void initialReduce(Socket masterSocket) throws IOException {
        
        ServerSocket listener = new ServerSocket(REDUCER_LISTENER_PORT);
        receiveFile(listener, REDUCER_FOLER_PATH+"/client.jar");
        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
        out.println(MasterScheduler.JAR_RECEIVED);
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

        FileInputStream inputStream = new FileInputStream(MapRun.SHUFFLE_OUTPUT_PATH + "/" + Shuffle.KEY_FILENAME_MAPPING);
        OutputStream outputStream = sender.getOutputStream();
        IOUtils.copy(inputStream, outputStream);
        inputStream.close();
        outputStream.close();
        sender.close();


    }

    private void runJobRequestHandler(Socket socket) throws IOException {

        FileOutputStream fos = null, fos2 = null;
        ServerSocket listener = new ServerSocket(LISTENER_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println("readyForJob");
        Socket sender = listener.accept();
        String inputFileName = "/home/srikar/Documents/MyHadoop/my-mapreduce/src/main/java/neu/mapreduce/io/sockets/test.txt";
        String clientJarPath = "/home/srikar/Documents/MyHadoop/my-mapreduce/src/main/java/neu/mapreduce/io/sockets/client-1.3-SNAPSHOT-jar-with-dependencies.jar";

        try {
            InputStream in = sender.getInputStream();
            fos = new FileOutputStream(inputFileName);
            IOUtils.copy(in, fos);
            fos.close();
            in.close();
            sender.close();
            Socket sender2 = listener.accept();
            in = sender2.getInputStream();
            fos = new FileOutputStream(clientJarPath);
            IOUtils.copy(in, fos);
            fos.close();
            in.close();
            sender2.close();
            listener.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        SlaveListener.status = ConnectionTypes.BUSY;
        //Run the job in new thread here

        String inputFilePath = "/home/srikar/Desktop/input/purchases.txt";
        String mapperClassname = "mapperImpl.AirlineMapper";

        //TODO: NEED TO GENERATE ON FLY
        String outputFilePath = "/home/srikar/Desktop/map-op-4.txt";
        String keyClassName = "impl.StringWritable";
        String valueClassname = "impl.FloatWritable";
        boolean isCombinerSet = false;

        new Thread(new SlaveMapRunThread(
                inputFilePath,
                mapperClassname,
                outputFilePath,
                clientJarPath,
                keyClassName,
                valueClassname,
                isCombinerSet))
                .start();
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


    public static void main(String[] args) throws IOException {
        SlaveListener listener = new SlaveListener(8087);
        listener.startListening();
    }
}
