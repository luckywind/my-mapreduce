package neu.mapreduce.commons.sockets;

import java.io.IOException;

/**
 * Created by Amitash on 3/31/15.
 */
public class SendFileTest {
    public static void main(String[] args) throws IOException {
        //SendFile sender = new SendFile(6066, "/usr/local/apache-maven/apache-maven-3.2.5/hadoop-course/src/main/java/sockets/SendFileTest.java");
        ReceiveFile receiver = new ReceiveFile("/usr/local/apache-maven/apache-maven-3.2.5/hadoop-course/src/main/java/sockets/testRemote.jar", "192.168.1.24", 6060);
        //sender.sendFile();
        receiver.receiveFile();
    }
}
