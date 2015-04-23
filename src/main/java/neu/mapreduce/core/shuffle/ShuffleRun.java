package neu.mapreduce.core.shuffle;

import api.JobConf;
import api.MyWriteComparable;
import neu.mapreduce.core.combiner.Combiner;
import neu.mapreduce.core.factory.WriteComparableFactory;
import neu.mapreduce.io.sockets.IOCommons;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by mit, srikar, vishal on 4/8/15.
 */
public class ShuffleRun {
    private final static Logger LOGGER = Logger.getLogger(ShuffleRun.class.getName());
    public final static String INPUT_FILE_KEYVALUE_SEPARATOR = "\t";
    public final static String OUTPUT_SHUFFLE_FILE_VALUE_SEPARATOR = "\t";
    private final static String OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR = "\n";
    public final static String KEY_FILENAME_MAPPING = "keyfilemapping";
    private final static int KEY_INDEX = 0;
    private final static int VALUE_INDEX = 1;
    public static final int ZERO = 0;
    public static final int TWO = 2;

    /**
     * This function takes a input the output file from the map phase and creates one file for each key.
     * This file contains the key on the first line and the its values in sorted order.
     * In-memory sorting is performed*
     * @param mapperOutputFilePath   Path to mapper's output file
     * @param locationOfShuffleFiles Path of the output shuffle files
     * @param clientJarPath          Path to the client JAR
     * @param jobConf                Instance of JobConf from the client
     */
    public void shuffle(String mapperOutputFilePath, String locationOfShuffleFiles, String clientJarPath, JobConf jobConf) {
        WriteComparableFactory mapperOutputKeyFactory = WriteComparableFactory.generateWriteComparableFactory(jobConf.getMapKeyOutputClassName());
        WriteComparableFactory mapperOutputValueFactory = WriteComparableFactory.generateWriteComparableFactory(jobConf.getMapValueOutputClassName());
        new File(locationOfShuffleFiles).mkdir();
        String mappingFilename = locationOfShuffleFiles + "/" + KEY_FILENAME_MAPPING;
        BufferedWriter filemappingBW = null;
        BufferedReader inputBufferedReader = null;
        try {
            filemappingBW = new BufferedWriter(new FileWriter(new File(mappingFilename)));
            Hashtable<String, ArrayList> keyListOfValue = readMapperOutput(mapperOutputFilePath, mapperOutputValueFactory);
            performShuffle(keyListOfValue, locationOfShuffleFiles, clientJarPath, jobConf, mapperOutputKeyFactory, filemappingBW);
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed as output file of mapper couldn't be found");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed as IOException while reading output file from mapper couldn't be found");
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            LOGGER.log(Level.SEVERE, "Error while creating an instance of mapper output key or value class");
        } finally {
            IOCommons.shutDownBufferedWriter(filemappingBW);
            IOCommons.shutDownBufferedReader(inputBufferedReader);
        }
    }

    /**
     * Creates a Hashtable that maps key with all the values present in the output of mapper
     * @param mapperOutputFilePath     Location of output of mapper
     * @param mapperOutputValueFactory Instance of a factory of Mapper output value
     * @return Hashtable that maps key with all the values present in the output of mapper
     * @throws IOException
     */
    private Hashtable<String, ArrayList> readMapperOutput(String mapperOutputFilePath, WriteComparableFactory mapperOutputValueFactory) throws IOException {
        Hashtable<String, ArrayList> keyListOfValue = new Hashtable<>();
        BufferedReader inputBufferedReader = new BufferedReader(new FileReader(new File(mapperOutputFilePath)));
        String line;
        while ((line = inputBufferedReader.readLine()) != null) {
            String[] keyvalue = line.split(INPUT_FILE_KEYVALUE_SEPARATOR, TWO);
            if (keyvalue.length < TWO) {
                LOGGER.log(Level.WARNING, "Ignoring one line as it does not have a key and value. Line:" + line);
                continue;
            }
            if (!keyListOfValue.containsKey(keyvalue[KEY_INDEX])) {
                keyListOfValue.put(keyvalue[KEY_INDEX], new ArrayList());
            }
            try {
                keyListOfValue.get(keyvalue[KEY_INDEX]).add(mapperOutputValueFactory.getNewInstance().deserialize(keyvalue[VALUE_INDEX]));
            } catch (IllegalAccessException | InstantiationException e) {
                LOGGER.log(Level.WARNING, "Couldn't create an instance for a line in output of mapper: Line:" + line);
            }
        }
        return keyListOfValue;
    }

    /**
     * Takes as input key and corresponding list of values.
     * Either calls the combiner or sorts the list of values and write the output to appropriate file
     * It also updates the file that holds the mapping between key and shuffle file output.
     * @param keyListOfValue         Hashtable that maps key with all the values present in the output of mapper
     * @param locationOfShuffleFiles Location where shuffle files needs to stored
     * @param clientJarPath          Location of client JAr
     * @param jobConf                Instance of JobConf from the client
     * @param mapperOutputKeyFactory Instance of a factory of Mapper output key
     * @param filemappingBW          BufferedWriter for fileMapping
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    private void performShuffle(Hashtable<String, ArrayList> keyListOfValue, String locationOfShuffleFiles, String clientJarPath, JobConf jobConf, WriteComparableFactory mapperOutputKeyFactory, BufferedWriter filemappingBW) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        int shuffleCounter = ZERO;
        for (String key : keyListOfValue.keySet()) {
            String locationShuffleFile = locationOfShuffleFiles + "/" + shuffleCounter;
            BufferedWriter shuffleFileBW = new BufferedWriter(new FileWriter(new File(locationShuffleFile)));
            Collections.sort(keyListOfValue.get(key));
            if (jobConf.isIsCombinerSet()) {
                new Combiner().combinerRun(key, keyListOfValue.get(key).iterator(), mapperOutputKeyFactory, clientJarPath, jobConf.getCombinerClassName(), shuffleFileBW);
            } else {
                writeToFile(shuffleFileBW, key, keyListOfValue.get(key));
            }
            writeToKeyFileMapping(filemappingBW, key, locationShuffleFile);
            shuffleFileBW.flush();
            shuffleFileBW.close();
            shuffleCounter++;
        }
    }

    /**
     * Writes to the file that holds the mapping between the key and output shuffle file*
     * @param filemappingBW       BufferedWriter for the file that holds the mapping between the key and output shuffle file*
     * @param key                 the output key of the mapper
     * @param locationShuffleFile Location of the shuffle file for the given key
     * @throws IOException
     */
    private static void writeToKeyFileMapping(BufferedWriter filemappingBW, String key, String locationShuffleFile) throws IOException {
        filemappingBW.write(key + OUTPUT_SHUFFLE_FILE_VALUE_SEPARATOR + locationShuffleFile + OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
    }

    /**
     * Given an key and list of values writes it to file based on the BufferedWriter*
     * @param fileBW BufferedWriter of the output file
     * @param key    Key is written as the first line of the output file
     * @param values Values is a list that is written to the file after the key
     * @throws IOException
     */
    private static void writeToFile(BufferedWriter fileBW, String key, ArrayList values) throws IOException {
        fileBW.write(key + OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
        Iterator iterator = values.iterator();
        while (iterator.hasNext()) {
            fileBW.write(((MyWriteComparable) iterator.next()).getString() + OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
        }
    }
}
