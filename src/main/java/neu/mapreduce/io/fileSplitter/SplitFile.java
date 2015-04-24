package neu.mapreduce.io.fileSplitter;

import neu.mapreduce.io.sockets.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 3/31/15.
 */
public class SplitFile {
    public static final String PATH_MASTER_FOLDER = Constants.HOME + Constants.USER + Constants.MR_RUN_FOLDER + Constants.MASTER_FOLER;
    public static final int BYTE_CONVERSION = 1024;
    private final static Logger LOGGER = Logger.getLogger(SplitFile.class.getName());

    private int splitSizeInMB;

    public SplitFile(int splitSizeInMB) {
        this.splitSizeInMB = splitSizeInMB;
    }

    /**
     * Split the input file into chunks
     *
     * @param filePath Path of input file
     * @return list of split file names
     * @throws IOException
     */
    public ArrayList<String> splitFile(String filePath) throws IOException {
        ArrayList<String> fileSplits = new ArrayList<>();
        new File(PATH_MASTER_FOLDER).mkdirs();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        //Remember to put a limitation for split size in documentation. int may not be able to represent so much.
        int maxSize = splitSizeInMB * BYTE_CONVERSION * BYTE_CONVERSION;
        int curSize = 0;
        byte[] curPartition = new byte[maxSize];
        int partCount = 0;
        int index = 0;
        while ((line = br.readLine()) != null) {
            line += "\n";
            byte[] b = line.getBytes();
            curSize += b.length;
            if (curSize > maxSize) {
                partCount++;
                LOGGER.log(Level.INFO, "Partition " + String.valueOf(partCount) + " created ");
                index = 0;
                curSize = 0;

                //Write the curPartition to file
                writeByteArrayToFile(curPartition, PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
                fileSplits.add(PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));

                // preserve the extra contents over specified size
                for (int i = 0; i < b.length; i++) {
                    curPartition[index] = b[i];
                    index++;
                    curSize++;
                }
            } else {
                for (int i = 0; i < b.length; i++) {
                    curPartition[index] = b[i];
                    index++;
                }
            }
        }
        //Finally write the final byte array to file. First create a new byte array of size = index.
        // Then populate and write
        byte[] finPartition = new byte[index + 1];
        partCount++;
        for (int i = 0; i <= index; i++) {
            finPartition[i] = curPartition[i];
        }
        writeByteArrayToFile(finPartition, PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
        fileSplits.add(PATH_MASTER_FOLDER + "/part-" + String.valueOf(partCount));
        return fileSplits;
    }

    /**
     * Write byte to file
     *
     * @param ba       Input byte array
     * @param fileName File name where bytes are written
     * @throws IOException
     */
    public void writeByteArrayToFile(byte[] ba, String fileName) throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName);
        fos.write(ba);
        fos.close();
    }
}