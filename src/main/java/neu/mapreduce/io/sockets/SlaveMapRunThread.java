package neu.mapreduce.io.sockets;

import neu.mapreduce.core.mapper.MapRun;

/**
 * Created by srikar on 4/19/15.
 */
public class SlaveMapRunThread implements Runnable{

    private String inputFilePath;
    private String mapperClassname;
    private String outputFilePath;
    private String clientJarPath;
    private String keyClassName;
    private String valueClassname;
    private boolean isCombinerSet;

    public SlaveMapRunThread(String inputFilePath, String mapperClassname, String outputFilePath, String clientJarPath, String keyClassName, String valueClassname, boolean isCombinerSet) {
        this.inputFilePath = inputFilePath;
        this.mapperClassname = mapperClassname;
        this.outputFilePath = outputFilePath;
        this.clientJarPath = clientJarPath;
        this.keyClassName = keyClassName;
        this.valueClassname = valueClassname;
        this.isCombinerSet = isCombinerSet;
    }


    @Override
    public void run() {
        // run maprun
        new MapRun().mapRun(inputFilePath, mapperClassname, outputFilePath, clientJarPath, keyClassName, valueClassname, isCombinerSet);
        SlaveListener.status = ConnectionTypes.JOB_COMPLETE;
    }
}
