package neu.mapreduce.core.combiner;

import api.MyContext;
import neu.mapreduce.core.factory.ReducerFactory;
import neu.mapreduce.core.factory.WriteComparableFactory;

import java.io.BufferedWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by srikar on 4/18/15.
 */
public class Combiner {

    private final static Logger LOGGER = Logger.getLogger(Combiner.class.getName());
    private final static String KEY_VALUE_SEPARATOR = "\n";

    /**
     * *
     * @param key Key on which the combiner should be performed
     * @param valueIterator Iterator of values
     * @param keyFactory    Instance of key
     * @param clientJarPath
     * @naram combinerClassName
     * @param bw
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
                            BufferedWriter bw) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException {

        ReducerFactory reducerFactory = new ReducerFactory(clientJarPath, combinerClassName);
        MyContext myContext = new MyContext(bw, KEY_VALUE_SEPARATOR);

        if (valueIterator != null) {
            (reducerFactory.getSingletonObject()).reduce(
                    keyFactory.getNewInstance().deserialize(key),
                    valueIterator, myContext);
        } else {
            LOGGER.log(Level.SEVERE, "Unable to create an iterator");
        }

    }
}


