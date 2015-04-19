package neu.mapreduce.core.mapper;

import api.MyContext;
import neu.mapreduce.core.factory.MapFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;
import neu.mapreduce.core.shuffle.Shuffle;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by mit on 4/2/15.
 */
public class MapRun{

    private final static Logger LOGGER = Logger.getLogger(MapRun.class.getName());
    private final static String SHUFFLE_OUTPUT_PATH = "/home/srikar/Desktop/shuffle";

    public static void main(String [] args){

        String inputFilePath = "/home/srikar/Desktop/input/purchases.txt";
        String mapperClassname = "mapperImpl.AirlineMapper";

        //TODO: NEED TO GENERATE ON FLY
        String outputFilePath = "/home/srikar/Desktop/map-op-4.txt";
        String clientJarPath = "/home/srikar/Desktop/project-jar/client-1.3-SNAPSHOT-jar-with-dependencies.jar";
        String keyClassName = "impl.StringWritable";
        String valueClassname = "impl.FloatWritable";

        new MapRun().mapRun(inputFilePath, mapperClassname, outputFilePath, clientJarPath, keyClassName, valueClassname);
    }

    public void mapRun(String inputFilePath, String mapperClassname, String outputFilePath, String clientJarPath, String keyOutputClassName, String valueOutputClassname)  {


//        //TODO:THESE SHOULD BE INPUT PARAMETER
//        //TODO:FROM CLIENT
//        String inputFilePath = "/home/srikar/Desktop/input/purchases.txt";
//        String mapperClassname = "mapperImpl.AirlineMapper";
//
//        //TODO: NEED TO GENERATE ON FLY
//        String outputFilePath = "/home/srikar/Desktop/map-op-4.txt";
//        String clientJarPath = "/home/srikar/Desktop/project-jar/client-1.3-SNAPSHOT-jar-with-dependencies.jar";
//

        try {
            File aFile = new File(clientJarPath);
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});

//            Class clientClass = Class.forName(mapperClassname);
            MapFactory mapFactory = new MapFactory(clientJarPath, mapperClassname);

            String keyClassName = "impl.LongWritable";
            Class keyClassType = Class.forName(keyClassName);
            WriteComparableFactory keyFactory  = new WriteComparableFactory(keyClassType);

            String valueClassName = "impl.StringWritable";
            Class valueClassType = Class.forName(valueClassName);
            WriteComparableFactory valueFactory  = new WriteComparableFactory(valueClassType);

            BufferedReader br = null;
            BufferedWriter bw = null;
            try {
                br = new BufferedReader(new FileReader(new File(inputFilePath)));
                bw = new BufferedWriter(new FileWriter(new File(outputFilePath)));
                MyContext myContext = new MyContext(bw);

                String line;
                int counter=0;
                while ((line = br.readLine()) != null) {

                    (mapFactory.getSingletonObject()).map(
                            keyFactory.getNewInstance().deserialize(counter+"") ,valueFactory.getNewInstance().deserialize(line), myContext);
                    counter++;
                }
            }finally {
                if(bw!=null){
                    try {
                        bw.flush();
                        bw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if(br != null){
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        LOGGER.log(Level.INFO,"Completed map phase. Starting shuffle.");

        /*SHUFFLE*/
        new Shuffle().shuffle(outputFilePath, SHUFFLE_OUTPUT_PATH, keyOutputClassName, valueOutputClassname, clientJarPath);

    }

//    private void shuffleRun(String outputFilePath, String keyOutputClassName, String valueOutputClassname) {
////        String locShuffleFiles = "/home/srikar/Desktop/shuffle";
//        /*SHUFFLE*/
////        Shuffle shuffle = new Shuffle();
////        String keyClassName = "impl.StringWritable";
////        String valueClassname = "impl.FloatWritable";
//        new Shuffle().shuffle(outputFilePath, SHUFFLE_OUTPUT_PATH, keyOutputClassName, valueOutputClassname);
//    }
}