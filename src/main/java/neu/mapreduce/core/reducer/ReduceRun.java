package neu.mapreduce.core.reducer;

import api.MyContext;
import neu.mapreduce.core.factory.ReducerFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;
import neu.mapreduce.io.sockets.IOCommons;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mit on 4/9/15.
 */
public class ReduceRun {

    private final static Logger LOGGER = Logger.getLogger(ReduceRun.class.getName());

    /**
     * Converts list of shuffle files, one per key and values in sorted order.
     * Creates an iterator of the value and runs the reduce function from the client's jar
     * *
     *
     * @param outputFilePath                  File path to write the output of the reducer
     * @param listOfMergedShuffleFileLocation List of input files path, on per key, with key and values in sorted order
     * @param mapperOutputKeyClassType        Mapper output key class type
     * @param mapperOutputpValueClassType     Mapper output value class type
     * @param clientReducerClass              Class name of reducer inside client's JAR
     * @param clientJarPath                   Location of client JAR
     */
    public void reduceRun(String outputFilePath, List<String> listOfMergedShuffleFileLocation, String mapperOutputKeyClassType, String mapperOutputpValueClassType, String clientReducerClass, String clientJarPath) {
        WriteComparableFactory keyFactory = WriteComparableFactory.generateWriteComparableFactory(mapperOutputKeyClassType);
        WriteComparableFactory valueFactory = WriteComparableFactory.generateWriteComparableFactory(mapperOutputpValueClassType);
        BufferedWriter reducerOutputBW = null;
        try {
            reducerOutputBW = new BufferedWriter(new FileWriter(new File(outputFilePath)));
            for (String mergedShuffleFileLocation : listOfMergedShuffleFileLocation) {
                performReduceForShuffleFile(clientReducerClass, clientJarPath, keyFactory, valueFactory, reducerOutputBW, mergedShuffleFileLocation);
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Error in creating factory");
            e.printStackTrace();
        } catch (NullPointerException e) {
            LOGGER.log(Level.SEVERE, "Null pointer exception while creating the iterator or WriteComparable Factory");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IO exception while reading the merged-shuffle output or writing reduce output");
            e.printStackTrace();
        } finally {
            IOCommons.shutDownBufferedWriter(reducerOutputBW);
        }
    }

    /**
     * Perform the reduce method on each key and its sorted value stored in a file *
     *
     * @param clientReducerClass        Class name of reducer inside client's JAR
     * @param clientJarPath             Location of client JAR
     * @param keyFactory                Factory of class that is input key to the reducer(or the output key of the mapper)
     * @param valueFactory              Factory of class that is input value to the reducer(or the output value of the mapper)
     * @param reducerOutputBW           BufferedWriter for the reducer's output file
     * @param mergedShuffleFileLocation Location of the sorted merged shuffle file for the given key
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    private void performReduceForShuffleFile(String clientReducerClass, String clientJarPath, WriteComparableFactory keyFactory, WriteComparableFactory valueFactory, BufferedWriter reducerOutputBW, String mergedShuffleFileLocation) throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        BufferedReader mergedShuffleFileBR = null;
        try {
            mergedShuffleFileBR = new BufferedReader(new FileReader(mergedShuffleFileLocation));
            MyContext myContext = new MyContext(reducerOutputBW);
            //Key is first line in shuffle file
            String key = mergedShuffleFileBR.readLine();
            ReducerFactory reducerFactory = new ReducerFactory(clientJarPath, clientReducerClass);
            Iterator iterator = getIterator(mergedShuffleFileBR, valueFactory);
            if (iterator != null) {
                (reducerFactory.getSingletonObject()).reduce(
                        keyFactory.getNewInstance().deserialize(key),
                        iterator, myContext);
            } else {
                LOGGER.log(Level.SEVERE, "Unable to create an iterator");
            }
        } finally {
            IOCommons.shutDownBufferedReader(mergedShuffleFileBR);
        }
    }

    /**
     * Creates an iterator of the valus from the merged shuffle file*
     *
     * @param mergedShuffleFileBR         BufferedReader for merged shuffle file
     * @param writeComparableFactoryValue WriteComparable Factory of the mapper's output value class
     * @return an iterator of the values or null if there is an exception
     */
    private static Iterator getIterator(BufferedReader mergedShuffleFileBR, WriteComparableFactory writeComparableFactoryValue) {
        try {
            ArrayList arrayList = new ArrayList();
            String line;
            while ((line = mergedShuffleFileBR.readLine()) != null) {
                arrayList.add(writeComparableFactoryValue.getNewInstance().deserialize(line));
            }
            return arrayList.iterator();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IOException while reading merged shuffle file");
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.log(Level.SEVERE, "Error in creating an instance of value during creation of iterator");
        }
        return null;
    }
}
