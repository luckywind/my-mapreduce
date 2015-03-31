package neu.mapreduce.commons.sockets;

import org.apache.commons.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * Created by Amitash on 3/31/15.
 */
public class ReceiveFile {
    Socket socket;
    String outputPath;

    public ReceiveFile(String outputPath, String address, int port) throws IOException {
        this.socket = new Socket(address, port);
        this.outputPath = outputPath;
    }

    public void receiveFile() throws IOException {
        FileOutputStream fos = new FileOutputStream(outputPath);
        InputStream in = socket.getInputStream();
        IOUtils.copy(in, fos);
        fos.close();
        socket.close();
    }
}
