package neu.mapreduce.io.sockets;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Mit, Srikar on 4/24/15.
 */
public class ClientRunner {

    public static void main(String [] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        String dataFile = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + "/sample.txt";
        String clientJarName = "/client-1.4-SNAPSHOT-jar-with-dependencies.jar";
        String clientJarPath = Constants.HOME + Constants.USER + Constants.CLIENT_FOLDER + clientJarName;
        String jobConfClassName = "mapperImpl.AirlineJobConf";

        MasterRunner masterRunner = new MasterRunner(dataFile, clientJarPath, jobConfClassName, Constants.SPLIT_SIZE_IN_MB);
        masterRunner.runMaster();
    }
}
