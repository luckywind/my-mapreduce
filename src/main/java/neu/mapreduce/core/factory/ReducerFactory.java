package neu.mapreduce.core.factory;

import api.MyMapper;
import api.MyReducer;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by Mit, Srikar on 4/8/15.
 */

public final class ReducerFactory<T extends MyReducer> {

    private Class<T> typeArgumentClass;
    private T singletonObject;

    /**
     * Constructor which creates instance using class object
     *
     * @param typeArgumentClass Class whose instance need to be created
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public ReducerFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    /**
     * Constructor which creates instance of the given reducerClassname which is loaded from the
     * clientJar at runtime using {@link java.net.URLClassLoader}
     *
     * @param clientJarPath    client jar path which has client code
     * @param reducerClassname name of the class whose instance need to be created
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public ReducerFactory(String clientJarPath, String reducerClassname) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        File aFile = new File(clientJarPath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});
        Class<?> reducerClass = urlClassLoader.loadClass(reducerClassname);
        Class<? extends MyReducer> clientMapperClass = reducerClass.asSubclass(MyReducer.class);

        Constructor<? extends MyReducer> clientConstructor = clientMapperClass.getConstructor();
        MyReducer myReducer = clientConstructor.newInstance();
        singletonObject = (T) myReducer;
    }

    /**
     * @return singleton object
     */
    public T getSingletonObject() {
        return singletonObject;
    }

    /**
     * Creates new instance of the given class
     *
     * @return a new instance of given class
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }
}