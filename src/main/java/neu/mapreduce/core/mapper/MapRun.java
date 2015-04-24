package neu.mapreduce.core.mapper;

import api.JobConf;
import api.MyContext;
import neu.mapreduce.core.factory.MapFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;
import neu.mapreduce.core.shuffle.ShuffleRun;
import neu.mapreduce.io.sockets.IOCommons;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mit and srikar on 4/9/15.
 */
/**
 * Class which is responsible for performing map and shuffle phase
 */

public class MapRun {

    private final static Logger LOGGER = Logger.getLogger(MapRun.class.getName());

    /**
     * Runs the map function for each line from the input file
     * and initiates the shuffle phase
     * *
     *
     * @param inputFilePath        input chunk file path of the mapper
     * @param outputFilePath       file path of the mapper output
     * @param shuffleOutputDir     directory path of shuffle output
     * @param clientJarPath        client jar path
     * @param jobConf   Instance of JobConf from the client
     */
    public void mapRun(String inputFilePath, String outputFilePath, String shuffleOutputDir, String clientJarPath, JobConf jobConf){

        try {
            //Creates a factory to get the object of map given the filename 
            // and location of jar which holds the class file
            MapFactory mapFactory = new MapFactory(clientJarPath, jobConf.getMapperClassName());
            // Creates the factory for mapper input key and value types
            WriteComparableFactory keyFactory = WriteComparableFactory.generateWriteComparableFactory(jobConf.getMapKeyInputClassName());
            WriteComparableFactory valueFactory =WriteComparableFactory.generateWriteComparableFactory(jobConf.getMapValueInputClassName());
            BufferedReader brInputChunk = null;
            BufferedWriter bwOutputOfMapper = null;

            try {
                brInputChunk = new BufferedReader(new FileReader(new File(inputFilePath)));
                bwOutputOfMapper = new BufferedWriter(new FileWriter(new File(outputFilePath)));
                MyContext myContext = new MyContext(bwOutputOfMapper);

                String line;
                int lineCounter = 0;
                while ((line = brInputChunk.readLine()) != null) {
                    //Call map function on each line in the input data
                    (mapFactory.getSingletonObject()).map(
                            keyFactory.getNewInstance().deserialize(Integer.toString(lineCounter++)),
                            valueFactory.getNewInstance().deserialize(line),
                            myContext);
                }
            } catch (FileNotFoundException e) {
                LOGGER.log(Level.SEVERE, "Input data file not found");
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "IOException in reading input data file");
            } finally {
                IOCommons.shutDownBufferedWriter(bwOutputOfMapper);
                IOCommons.shutDownBufferedReader(brInputChunk);
            }
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException |
                NoSuchMethodException | InvocationTargetException e) {
            LOGGER.log(Level.SEVERE, "Error in creating factory for either mapper or mapper-input key or mapper-input value class");
            e.printStackTrace();
        } catch (MalformedURLException e) {
            LOGGER.log(Level.SEVERE, "Error in creating factory for mapper class");
            e.printStackTrace();
        }

        LOGGER.log(Level.INFO, "Completed map phase. Starting shuffle.");

        // Kick of shuffle phase
        new ShuffleRun().shuffle(outputFilePath, shuffleOutputDir, clientJarPath, jobConf);
        LOGGER.log(Level.INFO, "Completed shuffle phase.");
    }
}