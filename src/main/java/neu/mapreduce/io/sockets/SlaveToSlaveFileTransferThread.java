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
public class SlaveToSlaveFileTransferThread implements Runnable {
    public static final int SLAVE_TO_SLAVE_PORT = 6062;
    public static int fileCounter = 0;
    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SLAVE_TO_SLAVE_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true){
            try {

                Socket sender = serverSocket.accept();
                InputStream in = sender.getInputStream();
                int shuffleDirCounter = SlaveListener.shuffleDirCounter-1;
                FileOutputStream fos = new FileOutputStream(SlaveListener.REDUCER_FOLER_PATH + "/" + shuffleDirCounter +"/"+fileCounter);
                IOUtils.copy(in, fos);
                fos.close();
                in.close();
                sender.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
