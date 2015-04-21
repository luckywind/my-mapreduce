package neu.mapreduce.io.sockets;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Amitash on 4/18/15.
 */
@Deprecated
public class MasterSchedulerTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {

        String inputFile = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + "/inputData.txt";
        String CLIENT_JAR_WITH_DEPENDENCIES_JAR = "/client-1.4-SNAPSHOT-jar-with-dependencies.jar";
        String inputJar =  Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + CLIENT_JAR_WITH_DEPENDENCIES_JAR;

        String jobConfClassName = "mapperImpl.AirlineJobConf";

        ArrayList<String> fileSplits = new ArrayList<String>();
        fileSplits.add(inputFile);
        HashMap<String, Socket> testMap = new HashMap<String, Socket>();
        testMap.put("localhost:8087", new Socket("localhost", 8087));
       // MasterScheduler masterScheduler = new MasterScheduler(fileSplits, inputJar, testMap, jobConfClassName);
        //masterScheduler.schedule();
        System.out.println("DEPRECATED... Complete");
    }
}
