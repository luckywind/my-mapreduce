package neu.mapreduce.io.fileSplitter;

import neu.mapreduce.io.sockets.Constants;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by Amitash on 3/31/15.
 */
public class SplitFile {
    public static final String PATH_MASTER_FOLDER = Constants.HOME+Constants.USER+Constants.MR_RUN_FOLDER+Constants.MASTER_FOLER;
    
    int splitSizeInMB; 

    public SplitFile(int splitSizeInMB){
        this.splitSizeInMB = splitSizeInMB;
    }

    public ArrayList<String> splitFile(String filePath) throws IOException {
        ArrayList<String> fileSplits = new ArrayList<String>();
        //TODO: Make this a constant location
        new File(PATH_MASTER_FOLDER).mkdirs();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        //Remember to put a limitation for split size in documentation. int may not be able to represent so much.
        int maxSize = splitSizeInMB * 1024 * 1024;
        int curSize = 0;
        byte[] curPartition = new byte[maxSize];
        int partCount = 0;
        int index = 0;
        while ((line = br.readLine()) != null) {
            line += "\n";
            byte[] b = line.getBytes();
            curSize += b.length;
            if(curSize > maxSize){
                partCount++;
                System.out.println("Partition created");
                index = 0;
                curSize = 0;
                //Write the curPartition to file
                writeByteArrayToFile(curPartition, PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
                fileSplits.add(PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
                ////
                for(int i = 0; i<b.length; i++){
                    curPartition[index] = b[i];
                    index++;
                    curSize++;
                }
            } else {
                for(int i = 0; i<b.length; i++){
                    curPartition[index] = b[i];
                    index++;
                }
            }

        }
        //Finally write the final byte array to file. First create a new byte array of size = index. Then populate and write
        byte[] finPartition = new byte[index+1];
        partCount++;
        for(int i = 0; i<=index; i++){
            finPartition[i] = curPartition[i];
        }
        writeByteArrayToFile(finPartition, PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
        fileSplits.add(PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
        return fileSplits;
    }

    public void writeByteArrayToFile(byte[] ba, String fileName) throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName);
        fos.write(ba);
        fos.close();
    }
}