package neu.mapreduce.io.sockets;

import org.apache.commons.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by srikar on 4/19/15.
 */

/**
 * Thread for slave to slave file transfer which listen on a
 * standard port and saves the files locally
 */
public class SlaveToSlaveFileTransferThread implements Runnable {
    private final int slaveToSlavePort;

    public SlaveToSlaveFileTransferThread(int slaveToSlavePort){
        this.slaveToSlavePort = slaveToSlavePort;
    }
    
    public static int fileCounter = 0;

    /**
     * listens to other slave for receiving files during reduce phase
     */
    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(slaveToSlavePort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true){
            try {
                int shuffleDirCounter = SlaveListener.shuffleDirCounter-1;
                String outputFileName = SlaveListener.REDUCER_FOLDER_PATH + "/" + shuffleDirCounter +"/"+fileCounter++;
                IOCommons.receiveFile(serverSocket, outputFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
