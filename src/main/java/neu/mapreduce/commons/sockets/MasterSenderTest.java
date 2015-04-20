package neu.mapreduce.commons.sockets;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.Socket;

/**
 * Created by Amitash on 4/13/15.
 */
public class MasterSenderTest {
    public static void main(String[] args) throws IOException {
        Socket sender = new Socket("localhost", 8087);
        PrintWriter out = new PrintWriter(sender.getOutputStream(), true);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(sender.getInputStream()));
        while(true){
            out.println("status");
            if(in.readLine().equals("Idle")){
                System.out.println("sending..");
                out.println("runJob");
                in.readLine();
                //while(!(in.readLine().equals("readyForJob"))) {
                //}
                Socket fileSender = new Socket("localhost", 6060);
                OutputStream os = fileSender.getOutputStream();
                InputStream is = new FileInputStream("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/sockets/test.txt");
                IOUtils.copy(is, os);
                fileSender.close();
                System.out.println("yay");
            }
            break;
        }

        sender.close();
    }

}

