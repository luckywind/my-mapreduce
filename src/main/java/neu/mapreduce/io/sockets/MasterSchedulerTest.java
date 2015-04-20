package neu.mapreduce.io.sockets;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Amitash on 4/18/15.
 */
public class MasterSchedulerTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<String> fileSplits = new ArrayList<String>();
        String inputFile = "/home/srikar/Desktop/project-jar/purchases.txt";
        fileSplits.add(inputFile);
        HashMap<String, Socket> testMap = new HashMap<String, Socket>();
        testMap.put("localhost:8087", new Socket("localhost", 8087));
        String inputJar = "/home/srikar/Documents/MyHadoop/my-mapreduce-client/target/client-1.4-SNAPSHOT-jar-with-dependencies.jar";
        String jobConfClassName = "mapperImpl.AirlineJobConf";
        MasterScheduler masterScheduler = new MasterScheduler(fileSplits, inputJar, testMap, jobConfClassName);
        masterScheduler.schedule();
        System.out.println("Complete");
        }
        }
