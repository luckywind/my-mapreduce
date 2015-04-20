package neu.mapreduce.io.sockets;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by srikar on 4/19/15.
 */
public class IOCommons {
    public static void sendFile(String fileName, String destIP, int destPort) throws IOException {
        Socket fileSender = new Socket(destIP, destPort);
        OutputStream os = fileSender.getOutputStream();
        InputStream is = new FileInputStream(fileName);
        IOUtils.copy(is, os);
        os.close();
        is.close();
        fileSender.close();
    }
}
