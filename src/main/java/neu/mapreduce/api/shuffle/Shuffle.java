package neu.mapreduce.api.shuffle;

import api.MyWriteComparable;
import neu.mapreduce.api.factory.WriteComparableFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by mit, srikar, visahl on 4/8/15.
 */
public class Shuffle{
    private final static Logger LOGGER = Logger.getLogger(Shuffle.class.getName());
    private final static String INPUT_FILE_KEYVALUE_SEPARATOR = "\t";
    private final static String OUTPUT_SHUFFLE_FILE_VALUE_SEPARATOR = "\t";
    private final static String OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR = "\n";
    private final static String KEY_FILENAME_MAPPING = "keyfilemapping";
    private final static int KEY_INDEX = 0;
    private final static int VALUE_INDEX = 1;


    public void shuffle(String mapperOutputFilePath, String locationOfShuffleFiles, String keyClassname, String valueClassname) {
        int shuffleCounter = 0;
        Hashtable<String, ArrayList> keyListOfValue = new Hashtable<String, ArrayList>();
        
        WriteComparableFactory keyFactory = generateWriteComparableFactory(keyClassname);
        WriteComparableFactory valueFactory = generateWriteComparableFactory(valueClassname);
        
        new File(locationOfShuffleFiles).mkdir();
        String mappingFilename = locationOfShuffleFiles + "/" + KEY_FILENAME_MAPPING;
        BufferedWriter filemappingBW = null;
        BufferedReader inputBufferedReader = null;
        try {
            
            filemappingBW = new BufferedWriter(new FileWriter(new File(mappingFilename)));
            inputBufferedReader = new BufferedReader(new FileReader(new File(mapperOutputFilePath)));
            
            String line;
            
            while ((line = inputBufferedReader.readLine()) != null) {

                String[] keyvalue = line.split(INPUT_FILE_KEYVALUE_SEPARATOR);

                if (keyvalue.length < 2) {
                    LOGGER.log(Level.WARNING, "SHUFFLE: Ignoring one line as it does not have a key and value. Line:" + keyvalue);
                    continue;
                }
                
                if (!keyListOfValue.containsKey(keyvalue[KEY_INDEX])) {
                    keyListOfValue.put(keyvalue[KEY_INDEX], new ArrayList());
                }


                try {
                    keyListOfValue.get(keyvalue[KEY_INDEX]).add(valueFactory.getNewInstance().deserialize(keyvalue[VALUE_INDEX]));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                }

            }//END OF WHILE
            
            //POST PROCESSING
            for(String key:keyListOfValue.keySet()){

                Collections.sort(keyListOfValue.get(key));
                
                String locationShuffleFile = locationOfShuffleFiles + "/" + shuffleCounter;
                BufferedWriter newBW = new BufferedWriter(new FileWriter(new File(locationShuffleFile)));

                writeToFile(newBW, key, keyListOfValue.get(key));
                writeToKeyFileMapping(filemappingBW, key, locationShuffleFile);

                newBW.flush();
                newBW.close();
                
                shuffleCounter++;
            }
            
            //TODO: Send keyTOBw.keySet() to master

        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "SHUFFLE: Failed as output file of mapper couldn't be found");
            e.printStackTrace();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "SHUFFLE: Failed as IOException while reading output file from mapper couldn't be found");
            e.printStackTrace();
        } finally {
            if(filemappingBW!=null) {
                try {
                    filemappingBW.flush();
                    filemappingBW.close();
                    inputBufferedReader.close();
                }catch (IOException e){
                    LOGGER.log(Level.WARNING, "SHUFFLE: Failed to close filemapping bufferedwriter due to IOException");
                }
            }
        }
    }



    private static void writeToKeyFileMapping(BufferedWriter filemappingBW, String key, String locationShuffleFile) throws IOException {
        filemappingBW.write(key + OUTPUT_SHUFFLE_FILE_VALUE_SEPARATOR + locationShuffleFile + OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
    }

    private static void writeToFile(BufferedWriter newBW, String key, ArrayList values) throws IOException {
        newBW.write(key+OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
        Iterator iterator = values.iterator();
        while(iterator.hasNext()){
            newBW.write(((MyWriteComparable)iterator.next()).getString()+OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
        }
        /*for(V value:values){
            newBW.write(value.getString()+OUTPUT_SHUFFLE_FILE_LINE_SEPARATOR);
        }*/
    }

    private static WriteComparableFactory generateWriteComparableFactory(String classname){
        Class keyClass = null;
        try {
            keyClass = Class.forName(classname);
            return new WriteComparableFactory(keyClass);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Class not found:"+classname);
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
    /*JUST FOR TESTING*/
    public static void main(String[] args) {
        Shuffle shuffle = new Shuffle();
        String locShuffleFiles = "/home/mit/Desktop/shuffle_sort_gen2";
        shuffle.shuffle("/home/mit/Desktop/map-op-2.txt", locShuffleFiles, "impl.StringWritable", "impl.FloatWritable");
    }
}
