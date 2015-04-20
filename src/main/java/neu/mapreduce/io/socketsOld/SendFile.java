package neu.mapreduce.io.socketsOld;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Amitash on 3/31/15.
 */
public class SendFile {
    ServerSocket server_socket;
    File myFile;
    public SendFile(int port, String myPath) throws IOException {
        this.server_socket = new ServerSocket(port);
        this.myFile = new File(myPath);

    }

    public void sendFile() throws IOException {
        Socket socket = server_socket.accept();
        OutputStream out = socket.getOutputStream();
        InputStream in = new FileInputStream(myFile);
        IOUtils.copy(in, out);
        socket.close();
    }




}
