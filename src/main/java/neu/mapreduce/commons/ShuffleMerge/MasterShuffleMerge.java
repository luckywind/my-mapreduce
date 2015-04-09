package neu.mapreduce.commons.ShuffleMerge;

/**
 * Created by Amitash, Mit on 4/8/15.
 */

import api.MyWriteComparable;
import neu.mapreduce.api.factory.WriteComparableFactory;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Amitash on 4/8/15.
 * Works only with files whose values can be read as doubles
 */
public class MasterShuffleMerge {
    private final static Logger LOGGER = Logger.getLogger(MasterShuffleMerge.class.getName());

    String OUTPUT_FILE_NAME = "shuffleMerge";
    int fileId;

    public MasterShuffleMerge() {
        this.fileId = 0;
    }

    public void mergeAllFiles(ArrayList<String> files, String keyClassname, String valueClassname) throws FileNotFoundException {

        int fileSize = files.size();
        if (fileSize == 1) {
            //Code to read the only file and write it's full contents to the output file as is.
            try {
                FileUtils.copyFile(new File(files.get(0)), new File(OUTPUT_FILE_NAME));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        if (fileSize == 2) {
            //Code to merge the two files and return the output.
            mergeFiles(files.get(0), files.get(1), OUTPUT_FILE_NAME, keyClassname, valueClassname);
            return;
        }

        //Merge the first two files into output file fileId.
        fileId++;
        mergeFiles(files.get(0), files.get(1), Integer.toString(fileId), keyClassname, valueClassname);

        //Pop the two files from the files list.
        files.remove(0);
        files.remove(0);

        //Append outputfile to the filePointers
        files.add(Integer.toString(fileId));

        //Recursively call mergeAllFiles with the new list.
        mergeAllFiles(files, keyClassname, valueClassname);
    }

    public void mergeFiles(String file1, String file2, String outputFile, String keyClassname, String valueClassname) throws FileNotFoundException {
        BufferedReader br1 = new BufferedReader(new FileReader(file1));
        BufferedReader br2 = new BufferedReader(new FileReader(file2));
        PrintWriter writer = new PrintWriter(outputFile);


        //WriteComparableFactory keyFactory = generateWriteComparableFactory(keyClassname);
        WriteComparableFactory valueFactory = generateWriteComparableFactory(valueClassname);

        try {
            writer.println(br1.readLine());
            br2.readLine();
            String line1 = br1.readLine();
            String line2 = br2.readLine();
            while (true) {
                if (line1 == null) {
                    if (line2 == null) {
                        writer.close();
                        return;
                    }
                    writer.println(line2);
                    //write remaining lines from br2 to file.
                    writeRemainingLines(br2, writer);
                    writer.close();
                    return;
                } else if (line2 == null) {
                    //write reamaining lines from br1 to file.
                    writer.println(line1);
                    writeRemainingLines(br1, writer);
                    writer.close();
                    return;
                } else if (line1.isEmpty()) {
                    line1 = br1.readLine();
                } else if (line2.isEmpty()) {
                    line2 = br2.readLine();
                } else {
                    try {
                        int compare = valueFactory.getNewInstance().deserialize(line1).compareTo(valueFactory.getNewInstance().deserialize(line2));

                        if (compare < 0) {
                            writer.println(line1);
                            line1 = br1.readLine();
                        } else {
                            writer.println(line2);
                            line2 = br2.readLine();
                        }
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br1.close();
                br2.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public void writeRemainingLines(BufferedReader br, PrintWriter writer) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            writer.println(line);
        }
    }

    private static WriteComparableFactory generateWriteComparableFactory(String classname) {
        Class keyClass = null;
        try {
            keyClass = Class.forName(classname);
            return new WriteComparableFactory(keyClass);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Class not found:" + classname);
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

}