package neu.mapreduce.commons.ShuffleMerge;

/**
 * Created by Amitash on 4/8/15.
 */
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by Amitash on 4/8/15.
 * Works only with files whose values can be read as doubles
 */
public class MasterShuffleMerge {
    String OUTPUT_FILE_NAME = "shuffleMerge";
    int fileId;

    public MasterShuffleMerge(){
        this.fileId = 0;
    }

    public void mergeAllFiles(ArrayList<String> files) throws FileNotFoundException {

        int fileSize = files.size();
        if (fileSize == 1){
            //Code to read the only file and write it's full contents to the output file as is.
            try {
                FileUtils.copyFile(new File(files.get(0)), new File(OUTPUT_FILE_NAME));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        if (fileSize == 2){
            //Code to merge the two files and return the output.
            mergeFiles(files.get(0), files.get(1), OUTPUT_FILE_NAME);
            return;
        }

        //Merge the first two files into output file fileId.
        fileId++;
        mergeFiles(files.get(0), files.get(1), Integer.toString(fileId));

        //Pop the two files from the files list.
        files.remove(0);
        files.remove(0);

        //Append outputfile to the filePointers
        files.add(Integer.toString(fileId));

        //Recursively call mergeAllFiles with the new list.
        mergeAllFiles(files);
    }

    public void mergeFiles(String file1, String file2, String outputFile) throws FileNotFoundException {
        BufferedReader br1 = new BufferedReader(new FileReader(file1));
        BufferedReader br2 = new BufferedReader(new FileReader(file2));
        PrintWriter writer = new PrintWriter(outputFile);
        try {
            writer.println(br1.readLine());
            br2.readLine();
            String line1 = br1.readLine();
            String line2 = br2.readLine();
            while(true){
                if(line1 == null){
                    if(line2 == null){
                        writer.close();
                        return;
                    }
                    writer.println(line2);
                    //write remaining lines from br2 to file.
                    writeRemainingLines(br2, writer);
                    writer.close();
                    return;
                }
                if(line2 == null){
                    //write reamaining lines from br1 to file.
                    writer.println(line1);
                    writeRemainingLines(br1, writer);
                    writer.close();
                    return;
                }

                Double val1 = Double.parseDouble(line1.replace("\n", ""));
                Double val2 = Double.parseDouble(line2.replace("\n", ""));
                if(val1 < val2){
                    writer.println(line1);
                    line1 = br1.readLine();
                } else {
                    writer.println(line2);
                    line2 = br2.readLine();
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeRemainingLines(BufferedReader br, PrintWriter writer) throws IOException {
        String line;
        while((line = br.readLine()) != null){
            writer.println(line);
        }
    }


}

