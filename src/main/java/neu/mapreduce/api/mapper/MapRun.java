package neu.mapreduce.api.mapper;

import api.MyContext;
import api.MyWriteComparable;
import neu.mapreduce.api.factory.MapFactory;
import neu.mapreduce.api.shuffle.Shuffle;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by mit on 4/2/15.
 */
public class MapRun{

    private final static Logger LOGGER = Logger.getLogger(MapRun.class.getName());

    public static void main(String[] args) throws Exception {
        
        
        //TODO:THESE SHOULD BE INPUT PARAMETER
        //TODO:FROM CLIENT
        String inputFilePath = "/home/mit/Desktop/purchase100.txt";
        String mapperClassname = "mapperImpl.AirlineMapper";

        //TODO: NEED TO GENERATE ON FLY
        String outputFilePath = "/home/mit/Desktop/map-op-2.txt";

        
        Class clientClass = Class.forName(mapperClassname);
        MapFactory mapFactory = new MapFactory(clientClass);


        BufferedReader br = new BufferedReader(new FileReader(new File(inputFilePath)));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputFilePath)));
        MyContext myContext = new MyContext(bw);

        String line;
        while((line = br.readLine())!=null){

                (mapFactory.getSingletonObject()).map(line, myContext);

        }


        bw.flush();
        br.close();
        bw.close();
        
        LOGGER.log(Level.INFO,"Completed map phase. Starting shuffle.");
        String locShuffleFiles = "/home/mit/Desktop/shuffle";
        /*SHUFFLE*/
        Shuffle shuffle = new Shuffle();
        String keyClassName = "impl.StringWritable";
        String valueClassname = "impl.FloatWritable";
        shuffle.shuffle(outputFilePath, locShuffleFiles, keyClassName, valueClassname);

    }
}