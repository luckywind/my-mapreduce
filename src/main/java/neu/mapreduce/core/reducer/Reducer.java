package neu.mapreduce.core.reducer;

import api.MyContext;
import neu.mapreduce.core.factory.ReducerFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mit on 4/9/15.
 */
public class Reducer {

    //Takes as input a jar file which contains the reducer code
    // Converts the output of the sort to itertor
    // Runs the reducer code only once

    private final static Logger LOGGER = Logger.getLogger(Reducer.class.getName());

    /*public static void main(String args[]) {
        String outputFilePath = "/home/mit/Desktop/input/my-mapreduce/op-reducer";
        
        String filelocationofSort1 = "/home/mit/Desktop/input/my-mapreduce/shuffleMerge1";
        List<String> listOfAllSortedFile =  new ArrayList<String>();
        listOfAllSortedFile.add(filelocationofSort1);
        listOfAllSortedFile.add(filelocationofSort1);
        
        String keyClassType = "impl.StringWritable";
        String valueClassType = "impl.FloatWritable";
        String clientReducerClass = "mapperImpl.AirlineReducer";

        //reduceRun(outputFilePath, listOfAllSortedFile, keyClassType, valueClassType, clientReducerClass);

    }*/

    public void reduceRun(String outputFilePath, List<String> listOfAllSortedInputFile, String keyClassType, String valueClassType, String clientReducerClass,String clientJarPath) {
        WriteComparableFactory keyFactory = generateWriteComparableFactory(keyClassType);
        WriteComparableFactory valueFactory = generateWriteComparableFactory(valueClassType);
        //String clientJarPath = "/home/srikar/Desktop/project-jar/client-1.3-SNAPSHOT-jar-with-dependencies.jar";
        BufferedWriter bw = null;
        BufferedReader br = null;
        try {

            bw = new BufferedWriter(new FileWriter(new File(outputFilePath)));
            for (String filelocationofSort : listOfAllSortedInputFile) {
                try {

                    br = new BufferedReader(new FileReader(filelocationofSort));

                    MyContext myContext = new MyContext(bw);
                    String key = br.readLine();

//                    File aFile = new File(clientJarPath);
//                    URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});

                    ReducerFactory reducerFactory = new ReducerFactory(clientJarPath, clientReducerClass);

                    Iterator iterator = getIterator(br, valueFactory);

                    if (iterator != null) {
                        (reducerFactory.getSingletonObject()).reduce(
                                keyFactory.getNewInstance().deserialize(key),
                                iterator, myContext);
                    } else {
                        LOGGER.log(Level.SEVERE, "Unable to create an iterator");
                    }
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } finally {
                    if(br !=null){
                        try {
                            br.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
         catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(bw !=null) {
                try {
                    bw.flush();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private static Iterator getIterator(BufferedReader br, WriteComparableFactory writeComparableFactory) {
        try {
            
            String line;
            ArrayList arrayList = new ArrayList();
            while ((line = br.readLine()) != null) {
                arrayList.add(writeComparableFactory.getNewInstance().deserialize(line));
            }
            return arrayList.iterator();

        } catch (FileNotFoundException e) {

        } catch (IOException e) {

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;   
    }

    protected static WriteComparableFactory generateWriteComparableFactory(String classname) {
        Class keyClass = null;
        try {
            keyClass = Class.forName(classname);
            return new WriteComparableFactory(keyClass);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Class not found:" + classname);
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

}
