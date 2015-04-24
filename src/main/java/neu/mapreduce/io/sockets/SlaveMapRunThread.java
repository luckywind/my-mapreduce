package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.core.mapper.MapRun;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/19/15.
 */

/**
 * Thread which runs the mapper task on slave machine
 */
public class SlaveMapRunThread implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(SlaveMapRunThread.class.getName());

    private String inputFilePath;
    private String mapOutputFilePath;
    private String shuffleOutputFolder;
    private String clientJarPath;
    private JobConf jobConf;

    /**
     * Public constructor
     * @param inputFilePath File path of input data
     * @param mapOutputFilePath Output file path
     * @param shuffleOutputFolder Folder path where shuffle files are kept
     * @param clientJarPath Path of client JAR
     * @param jobConf JobConf object
     */
    public SlaveMapRunThread(String inputFilePath, String mapOutputFilePath, String shuffleOutputFolder, String clientJarPath, JobConf jobConf) {
        this.inputFilePath = inputFilePath;
        this.mapOutputFilePath = mapOutputFilePath;
        this.clientJarPath = clientJarPath;
        this.shuffleOutputFolder = shuffleOutputFolder;
        this.jobConf = jobConf;
    }

    /**
     * Run the mapRun phase
     */
    @Override
    public void run() {
        new MapRun().mapRun(inputFilePath, mapOutputFilePath, shuffleOutputFolder, clientJarPath, jobConf);
        SlaveListener.status = ConnectionTypes.JOB_COMPLETE;
    }
}
