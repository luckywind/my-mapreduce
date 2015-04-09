package neu.mapreduce.core.mapper;

import api.MyContext;
import neu.mapreduce.core.factory.MapFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;
import neu.mapreduce.core.shuffle.Shuffle;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by mit on 4/2/15.
 */
public class MapRun{

    private final static Logger LOGGER = Logger.getLogger(MapRun.class.getName());

    public static void main(String[] args)  {
        
        
        //TODO:THESE SHOULD BE INPUT PARAMETER
        //TODO:FROM CLIENT
        String inputFilePath = "/home/mit/Desktop/purchase100.txt";
        String mapperClassname = "mapperImpl.AirlineMapper";

        //TODO: NEED TO GENERATE ON FLY
        String outputFilePath = "/home/mit/Desktop/map-op-4.txt";

        try {
            Class clientClass = Class.forName(mapperClassname);
            MapFactory mapFactory = new MapFactory(clientClass);

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
        }


        LOGGER.log(Level.INFO,"Completed map phase. Starting shuffle.");
        String locShuffleFiles = "/home/mit/Desktop/shuffle";
        /*SHUFFLE*/
        Shuffle shuffle = new Shuffle();
        String keyClassName = "impl.StringWritable";
        String valueClassname = "impl.FloatWritable";
        shuffle.shuffle(outputFilePath, locShuffleFiles, keyClassName, valueClassname);

    }
}