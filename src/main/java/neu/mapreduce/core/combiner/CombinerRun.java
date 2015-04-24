package neu.mapreduce.core.combiner;

import api.MyContext;
import neu.mapreduce.core.factory.ReducerFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;

import java.io.BufferedWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Created by Mit on 4/18/15.
 */
public class CombinerRun {
    private final static Logger LOGGER = Logger.getLogger(CombinerRun.class.getName());
    private final static String KEY_VALUE_SEPARATOR = "\n";

    /**
     * *
     * @param key               Key on which the combiner should be performed
     * @param valueIterator     Iterator of values
     * @param keyFactory        Instance of key
     * @param clientJarPath     Path to the client JAR
     * @param combinerClassName Name of the combiner class inside client JAR
     * @param myContextBW       MyContext of the buffered writer of the shuffle file
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     */
    public void combinerRun(String key,
                            Iterator valueIterator,
                            WriteComparableFactory keyFactory,
                            String clientJarPath,
                            String combinerClassName,
                            BufferedWriter myContextBW) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException, NullPointerException {
        ReducerFactory reducerFactory = new ReducerFactory(clientJarPath, combinerClassName);
        MyContext myContext = new MyContext(myContextBW, KEY_VALUE_SEPARATOR);
        (reducerFactory.getSingletonObject()).reduce(
                keyFactory.getNewInstance().deserialize(key),
                valueIterator, myContext);
    }
}


