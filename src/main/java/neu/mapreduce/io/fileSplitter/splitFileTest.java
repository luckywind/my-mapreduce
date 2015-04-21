package neu.mapreduce.io.fileSplitter;

import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Amitash on 3/31/15.
 */
public class splitFileTest {
    public static void main(String[] args) throws IOException {
        //SplitFile splitFile = new SplitFile(64);
        //splitFile.splitFile("/Users/Amitash/Desktop/A3/data.csv");
        FileReader fr = new FileReader("/home/mit/Desktop/MR_Client/inputData.txt");
        SplitFile splitFile = new SplitFile(64);
//        splitFile
    }
}
