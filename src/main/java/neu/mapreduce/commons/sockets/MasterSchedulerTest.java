package neu.mapreduce.commons.sockets;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Amitash on 4/18/15.
 */
public class MasterSchedulerTest {
    public static void main(String[] args) throws IOException {
        ArrayList<String> fileSplits = new ArrayList<String>();
        fileSplits.add("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/sockets/test.txt");
        HashMap<String, Socket> testMap = new HashMap<String, Socket>();
        testMap.put("localhost:8087", new Socket("localhost", 8087));
        MasterScheduler masterScheduler = new MasterScheduler(fileSplits, "/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/sockets/testR.jar", testMap);
        masterScheduler.schedule();
        System.out.println("Complete");
    }
}
