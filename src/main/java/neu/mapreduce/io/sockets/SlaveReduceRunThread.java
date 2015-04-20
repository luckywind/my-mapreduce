package neu.mapreduce.io.sockets;

import neu.mapreduce.core.reducer.Reducer;
import neu.mapreduce.core.sort.MasterShuffleMerge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.ArrayList;

/**
 * Created by srikar on 4/19/15.
 */
public class SlaveReduceRunThread implements Runnable {
    @Override
    public void run() {

       // new File(SlaveListener.REDUCER_FOLDER_PATH+"/"+ MasterShuffleMerge.OUTPUT_FILE_NAME).mkdir();

        ArrayList<String> listOfMergedFilePath = new ArrayList<>();
        String[] subDirectories = getAllSubDirectories(SlaveListener.REDUCER_FOLDER_PATH);
        MasterShuffleMerge masterShuffleMerge = new MasterShuffleMerge();
        for(String eachDirectory: subDirectories){
            try {
                masterShuffleMerge.mergeAllFileInDir(SlaveListener.REDUCER_FOLDER_PATH+"/"+eachDirectory, "impl.StringWritable", "impl.FloatWritable");
                listOfMergedFilePath.add(SlaveListener.REDUCER_FOLDER_PATH+"/"+eachDirectory+"/"+MasterShuffleMerge.OUTPUT_FILE_NAME);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }


        String outputFilePath= SlaveListener.REDUCER_FOLDER_PATH+"/op-reducer";
        String keyClassType = "impl.StringWritable";
        String valueClassType = "impl.FloatWritable";
        String clientReducerClass = "mapperImpl.AirlineReducer";

        new Reducer().reduceRun(outputFilePath, listOfMergedFilePath, keyClassType, valueClassType, clientReducerClass);

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
