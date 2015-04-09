package neu.mapreduce.core.sort;

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
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/core/sort/8");
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/core/sort/8a");
        files.add("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/core/sort/8a");
        merger.mergeAllFiles(files, "impl.StringWritable", "impl.FloatWritable");
    }

}
