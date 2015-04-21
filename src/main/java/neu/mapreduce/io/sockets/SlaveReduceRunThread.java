package neu.mapreduce.io.sockets;

import api.JobConf;
import neu.mapreduce.core.reducer.ReduceRun;
import neu.mapreduce.core.sort.MasterShuffleMerge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.ArrayList;

/**
 * Created by srikar on 4/19/15.
 */
public class SlaveReduceRunThread implements Runnable {

    public static final String OUTPUT_FILE_PATH = SlaveListener.REDUCER_FOLDER_PATH + "/op-reducer";
    /*public static final String KEY_CLASS_TYPE = "impl.StringWritable";
    public static final String VALUE_CLASS_TYPE = "impl.FloatWritable";
    public static final String CLIENT_REDUCER_CLASS = "mapperImpl.AirlineReducer";*/
    
    private JobConf jobConf;
    private String reducerClientJarPath;

    public SlaveReduceRunThread(String reducerClientJarPath, JobConf jobConf) {
        this.reducerClientJarPath = reducerClientJarPath;
        this.jobConf = jobConf;
    }

    @Override
    public void run() {

       // new File(SlaveListener.REDUCER_FOLDER_PATH+"/"+ MasterShuffleMerge.OUTPUT_FILE_NAME).mkdir();

        ArrayList<String> listOfMergedFilePath = new ArrayList<>();
        String[] subDirectories = getAllSubDirectories(SlaveListener.REDUCER_FOLDER_PATH);
        MasterShuffleMerge masterShuffleMerge = new MasterShuffleMerge();
        for(String eachDirectory: subDirectories){
            try {
                //masterShuffleMerge.mergeAllFileInDir(SlaveListener.REDUCER_FOLDER_PATH+"/"+eachDirectory, "impl.StringWritable", "impl.FloatWritable");
                masterShuffleMerge.mergeAllFileInDir(SlaveListener.REDUCER_FOLDER_PATH+"/"+eachDirectory, jobConf.getMapKeyOutputClassName(), jobConf.getMapValueOutputClassName());
                listOfMergedFilePath.add(SlaveListener.REDUCER_FOLDER_PATH + "/" + eachDirectory + "/" + MasterShuffleMerge.OUTPUT_FILE_NAME);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }


        new ReduceRun().reduceRun(OUTPUT_FILE_PATH, listOfMergedFilePath, jobConf.getMapKeyOutputClassName(),jobConf.getMapValueOutputClassName(), jobConf.getReducerClassName(), reducerClientJarPath);

        SlaveListener.status = ConnectionTypes.JOB_COMPLETE;


    }
    //returns just names and not complete path
    public String[] getAllSubDirectories(String inputDir) {
        File file = new File(inputDir);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        return directories;
    }
}
