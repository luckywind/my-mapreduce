package neu.mapreduce.io.sockets;

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
        String inputFile = "/home/srikar/Desktop/project-jar/purchases.txt";
        fileSplits.add(inputFile);
        HashMap<String, Socket> testMap = new HashMap<String, Socket>();
        testMap.put("localhost:8087", new Socket("localhost", 8087));
        String inputJar = "/home/srikar/Desktop/project-jar/client-1.3-SNAPSHOT-jar-with-dependencies.jar";
        MasterScheduler masterScheduler = new MasterScheduler(fileSplits, inputJar, testMap);
        masterScheduler.schedule();
        System.out.println("Complete");
    }
}
