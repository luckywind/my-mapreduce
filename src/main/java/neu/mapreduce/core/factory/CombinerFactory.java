package neu.mapreduce.core.factory;

import api.MyCombiner;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by srikar on 4/18/15.
 */

public class CombinerFactory<T extends MyCombiner> {

    private Class<T> typeArgumentClass;
    private T singletonObject;

    public CombinerFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    public CombinerFactory(String clientJarPath, String combinerClassName) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        File aFile = new File(clientJarPath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});
        Class<?> combinerClass = urlClassLoader.loadClass(combinerClassName);
        Class<? extends MyCombiner> clientCombinerClass = combinerClass.asSubclass(MyCombiner.class);

        Constructor<? extends MyCombiner> clientConstructor = clientCombinerClass.getConstructor();
        MyCombiner myCombiner = clientConstructor.newInstance();
        singletonObject = (T) myCombiner;

    }

    public T getSingletonObject() {
        return singletonObject;
    }

    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }
}
