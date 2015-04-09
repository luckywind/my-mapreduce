package neu.mapreduce.commons.ShuffleMerge;

/**
 * Created by Amitash on 4/8/15.
 */
import java.io.FileNotFoundException;
import java.util.ArrayList;

/**
 * Created by Amitash on 4/8/15.
 */
public class MasterShuffleMergeTest {
    public static void main(String[] args) throws FileNotFoundException {
        MasterShuffleMerge merger = new MasterShuffleMerge();
        ArrayList<String> files = new ArrayList<String>();
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/commons/8a");
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/commons/8b");
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/commons/8c");
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/commons/8d");
        merger.mergeAllFiles(files, "impl.StringWritable", "impl.FloatWritable");
    }

}
