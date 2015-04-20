package neu.mapreduce.core.sort;

/**
 * Created by Amitash on 4/8/15.
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;

/**
 * Created by Amitash on 4/8/15.
 */
public class MasterShuffleMergeTest {
    public static void main(String[] args) throws FileNotFoundException {
        MasterShuffleMerge merger = new MasterShuffleMerge();
//        ArrayList<File> files = new ArrayList<File>();
//        files.add(new File("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/core/sort/8"));
//        files.add(new File("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/core/sort/8a"));
//        files.add(new File("/home/mit/Desktop/input/my-mapreduce/src/main/java/neu/mapreduce/core/sort/8a"));
        merger.mergeAllFileInDir("/home/srikar/Desktop/mergetest", "impl.StringWritable", "impl.FloatWritable");
//        merger.mergeAllFiles(files, "impl.StringWritable", "impl.FloatWritable");
    }

}
