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
        files.add("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/a");
        files.add("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/b");
        files.add("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/c");
        files.add("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/d");
        //files.add("d");
        //files.add("e");
        //files.add("f");
        merger.mergeAllFiles(files);
    }

}
