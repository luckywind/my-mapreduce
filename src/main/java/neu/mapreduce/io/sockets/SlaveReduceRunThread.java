package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.core.reducer.ReduceRun;
import neu.mapreduce.core.sort.ExternalSort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.ArrayList;

/**
 * Created by srikar on 4/18/15.
 */

/**
 * Thread which runs the reduce task on slave machine
 */
public class SlaveReduceRunThread implements Runnable {

    public static final String OUTPUT_FILE_PATH = SlaveListener.REDUCER_FOLDER_PATH + "/op-reducer";

    private JobConf jobConf;
    private String reducerClientJarPath;

    public SlaveReduceRunThread(String reducerClientJarPath, JobConf jobConf) {
        this.reducerClientJarPath = reducerClientJarPath;
        this.jobConf = jobConf;
    }

    /**
     * Merge all the files from shuffle phase based on key and performs reduce task
     */
    @Override
    public void run() {

        ArrayList<String> listOfMergedFilePath = new ArrayList<>();
        String[] subDirectories = getAllSubDirectories(SlaveListener.REDUCER_FOLDER_PATH);
        ExternalSort externalSort = new ExternalSort();
        for (String eachDirectory : subDirectories) {
            try {
                String currentDirPath = SlaveListener.REDUCER_FOLDER_PATH + "/" + eachDirectory;
                // merge all the files after shuffle phase
                externalSort.mergeAllFileInDir(currentDirPath, jobConf.getMapKeyOutputClassName(), jobConf.getMapValueOutputClassName());
                listOfMergedFilePath.add(currentDirPath + "/" + ExternalSort.OUTPUT_FILE_NAME);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        new ReduceRun().reduceRun(OUTPUT_FILE_PATH, listOfMergedFilePath, jobConf.getMapKeyOutputClassName(), jobConf.getMapValueOutputClassName(), jobConf.getReducerClassName(), reducerClientJarPath);
        SlaveListener.status = ConnectionTypes.JOB_COMPLETE;
    }


    /**
     * Gets all the sub directories list of a given directory
     *
     * @param inputDir name of the input directory
     * @return all the subdirectories names present inside a given input directory
     */
    public String[] getAllSubDirectories(String inputDir) {
        File file = new File(inputDir);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });

        //returns just names and not complete path
        return directories;
    }
}
